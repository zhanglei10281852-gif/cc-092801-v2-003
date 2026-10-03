from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, day_bucket, day_start, to_storage
from app.core.errors import ConflictError, NotFoundError, QuotaExceededError, ValidationError
from app.database import get_connection, transaction

# 额度维度与 compute_quotas 列的对应关系，用于统一核算与解释。
QUOTA_DIMENSIONS = {"queued": "max_queued", "running": "max_running", "daily_submissions": "daily_submissions"}


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._enforce_submit_quota(repository, payload["requested_by"], now_value)
            try:
                task = repository.create_task(
                    template_id=template["id"], project_code=payload["project_code"],
                    requested_by=payload["requested_by"], parameters=parameters,
                    parameter_digest=parameter_digest, priority=payload["priority"],
                    idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
                )
            except sqlite3.IntegrityError:
                # 并发提交触达唯一约束时，同一幂等键仍然只保留一个可追踪的服务单。
                existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
                if existing is None:
                    raise
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._record_quota_event(repository, task, "queued", "acquire", "提交占用排队额度", now_value)
            self._record_quota_event(repository, task, "daily_submissions", "acquire", "提交计入当日提交额度", now_value)
            return task

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidates = repository.queued_candidates(capabilities, now)
            if not candidates:
                return None
            owners = {queued["requested_by"] for queued in candidates}
            running_usage = repository.running_counts_by_users(owners)
            quotas = {owner: repository.quota("user", owner) for owner in owners}
            candidate = None
            for queued in candidates:
                owner = queued["requested_by"]
                quota = quotas[owner]
                if quota is not None and running_usage.get(owner, 0) >= int(quota["max_running"]):
                    # 该家庭运行额度已用尽，跳过其订单，避免绕过额度进入执行队列。
                    continue
                candidate = queued
                break
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            self._record_quota_event(repository, candidate, "queued", "release", "领取释放排队额度", now_value)
            self._record_quota_event(repository, candidate, "running", "acquire", "领取占用运行额度", now_value)
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            self._record_quota_event(repository, task, "running", "release", "完成释放运行额度", now_value)
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            self._record_quota_event(repository, task, "running", "release", "失败释放运行额度", now_value)
            if can_retry:
                self._record_quota_event(repository, task, "queued", "acquire", "失败重试重新排队", now_value)
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str, record: Callable[[str, str, str], None]) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
            record("queued", "acquire", "人工重试重新排队")
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str, record: Callable[[str, str, str], None]) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status IN ('running','cancel_requested') AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if task["status"] == "cancel_requested":
                    # 取消请求在租约到期后生效，订单结转为已取消并释放运行额度。
                    status, finished_at = "cancelled", now
                    cancelled.append(int(task["id"]))
                elif int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                self._record_quota_event(repository, task, "running", "release", "租约过期释放运行额度", now_value)
                if status == "queued":
                    self._record_quota_event(repository, task, "queued", "acquire", "租约过期重新排队", now_value)
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str, Callable[[str, str, str], None]], None]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)

            def record(dimension: str, event_action: str, event_reason: str) -> None:
                self._record_quota_event(repository, task, dimension, event_action, event_reason, now_value)

            mutation(connection, task, now, record)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str, record: Callable[[str, str, str], None]) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))
        if status == "cancelled":
            record("queued", "release", "取消释放排队额度")

    def quota_usage(self, subject_type: str, subject_key: str) -> dict[str, Any]:
        """返回指定主体在统一日边界下的额度用量与剩余额度。"""
        now_value = self.clock.now()
        quota = self.repository.quota(subject_type, subject_key)
        used = self._usage(self.repository, subject_type, subject_key, now_value)
        dimensions: dict[str, Any] = {}
        for dimension, column in QUOTA_DIMENSIONS.items():
            limit = int(quota[column]) if quota is not None else None
            dimensions[dimension] = {
                "limit": limit,
                "used": used[dimension],
                "remaining": None if limit is None else max(0, limit - used[dimension]),
            }
        return {
            "subject_type": subject_type,
            "subject_key": subject_key,
            "day_start": to_storage(day_start(now_value)),
            "dimensions": dimensions,
        }

    def quota_events(self, subject_type: str, subject_key: str, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.quota_events(subject_type, subject_key, limit)

    def quota_report(self) -> dict[str, Any]:
        """供维护命令使用：列出全部已配置额度的主体及其用量。"""
        now_value = self.clock.now()
        subjects = []
        for quota in self.repository.list_quotas():
            used = self._usage(self.repository, quota["subject_type"], quota["subject_key"], now_value)
            dimensions = {
                dimension: {
                    "limit": int(quota[column]),
                    "used": used[dimension],
                    "remaining": max(0, int(quota[column]) - used[dimension]),
                }
                for dimension, column in QUOTA_DIMENSIONS.items()
            }
            subjects.append({"subject_type": quota["subject_type"], "subject_key": quota["subject_key"], "dimensions": dimensions})
        return {"day_start": to_storage(day_start(now_value)), "subjects": subjects}

    def _usage(self, repository: ComputeRepository, subject_type: str, subject_key: str, now_value: datetime) -> dict[str, int]:
        states = repository.count_states_by_subject(subject_type, subject_key)
        return {
            "queued": states.get("queued", 0),
            "running": states.get("running", 0) + states.get("cancel_requested", 0),
            "daily_submissions": repository.count_submissions_since(subject_type, subject_key, to_storage(day_start(now_value))),
        }

    def _record_quota_event(self, repository: ComputeRepository, task: sqlite3.Row | dict[str, Any], dimension: str, action: str, reason: str, now_value: datetime) -> None:
        repository.add_quota_event(
            subject_type="user",
            subject_key=str(task["requested_by"]),
            task_id=int(task["id"]),
            dimension=dimension,
            action=action,
            reason=reason,
            day_bucket=day_bucket(now_value),
            now=to_storage(now_value),
        )

    def _enforce_submit_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        used = self._usage(repository, "user", requested_by, now)
        labels = {"queued": "排队", "daily_submissions": "当日提交"}
        for dimension in ("queued", "daily_submissions"):
            limit = int(quota[QUOTA_DIMENSIONS[dimension]])
            if used[dimension] >= limit:
                raise QuotaExceededError(
                    f"用户{labels[dimension]}额度已用尽：上限 {limit}，已用 {used[dimension]}，剩余 0",
                    context={
                        "subject_type": "user",
                        "subject_key": requested_by,
                        "dimension": dimension,
                        "limit": limit,
                        "used": used[dimension],
                        "remaining": max(0, limit - used[dimension]),
                    },
                )

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
