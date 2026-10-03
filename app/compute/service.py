from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, QuotaExceededError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def business_day_for(now: datetime) -> str:
    """跨日结算统一使用 UTC 业务日（YYYY-MM-DD），各处边界保持一致。"""
    return now.astimezone(UTC).date().isoformat()


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

    QUOTA_BUCKETS = (
        ("queued", "max_queued", "排队"),
        ("running", "max_running", "运行"),
        ("daily", "daily_submissions", "当日提交"),
    )

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        business_day = business_day_for(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            parameter_digest = digest(parameters)
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                # 重复提交直接回放既有服务单，不再次扣减任何额度。
                return dict(repository.task_by_id(existing["id"]))
            subject_type, subject_key = "user", payload["requested_by"]
            # 提交只占用排队与当日提交额度；运行额度在领取（派车）时才校验，排队中的订单不占运行额度。
            self._require_capacity(repository, subject_type, subject_key, now_value, ("queued", "daily"))
            try:
                task = repository.create_task(
                    template_id=template["id"], project_code=payload["project_code"],
                    requested_by=payload["requested_by"], parameters=parameters,
                    parameter_digest=parameter_digest, priority=payload["priority"],
                    idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"],
                    quota_subject_type=subject_type, quota_subject_key=subject_key, now=now,
                )
            except sqlite3.IntegrityError:
                # 并发提交同一幂等键：唯一约束兜底，回放先落库的那一个服务单。
                winner = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
                if winner is None:
                    raise
                if winner["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数") from None
                return dict(repository.task_by_id(winner["id"]))
            for bucket, reason in (("daily", "submit"), ("queued", "submit")):
                repository.add_ledger_entry(
                    subject_type=subject_type, subject_key=subject_key, task_id=task["id"],
                    bucket=bucket, delta=1, business_day=business_day, reason=reason,
                    actor=payload["requested_by"], idempotency_key=payload["idempotency_key"], now=now,
                )
            return dict(repository.task_by_id(task["id"]))

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["quota_ledger"] = self.repository.ledger_entries_for_task(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        business_day = business_day_for(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidates = repository.queued_candidates(capabilities, now, limit=20)
            for candidate in candidates:
                # 运行额度不足的家庭跳过，让后面的可执行服务单先行。
                if not self._has_capacity(repository, candidate["quota_subject_type"], candidate["quota_subject_key"], now_value, ("running",)):
                    continue
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                    (worker_id, lease_until, now, now, candidate["id"]),
                )
                if cursor.rowcount != 1:
                    # 并发领取：候选已被其他工作者抢走，尝试下一个。
                    continue
                self._transfer_hold(
                    repository, task_id=candidate["id"],
                    subject_type=candidate["quota_subject_type"], subject_key=candidate["quota_subject_key"],
                    from_bucket="queued", to_bucket="running", business_day=business_day,
                    reason="claim", actor=worker_id, idempotency_key=candidate["idempotency_key"], now=now,
                )
                return dict(repository.task_by_id(candidate["id"]))
            return None

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
        business_day = business_day_for(now_value)
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
            self._release_holds(repository, task, ("running",), "complete", worker_id, now, business_day)
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        business_day = business_day_for(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            # 运行额度先释放；只有排队额度仍有空位时才允许重新排队，保证额度不被绕过。
            self._release_holds(repository, task, ("running",), "fail", worker_id, now, business_day)
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            if can_retry and not self._has_capacity(repository, task["quota_subject_type"], task["quota_subject_key"], now_value, ("queued",)):
                can_retry = False
                error_code = "retry_quota_exhausted"
                message = "失败可重试，但家庭排队额度已用尽，任务终止"
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            if can_retry:
                self._acquire_hold(repository, task, "queued", business_day, "retry", worker_id, now)
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, now: str, business_day: str, actor: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            # 重试不是新提交，不占当日额度；但排队额度必须重新持有，超额则拒绝并返回剩余额度。
            self._require_capacity(repository, task["quota_subject_type"], task["quota_subject_key"], self.clock.now(), ("queued",))
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
            self._acquire_hold(repository, task, "queued", business_day, "retry", actor, now)
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, now: str, business_day: str, actor: str) -> None:
            del repository, business_day, actor
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
                entry: dict[str, Any] = {"task_id": task_id, "code": exc.code, "message": exc.message}
                if exc.context:
                    entry["context"] = exc.context
                failed.append(entry)
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        business_day = business_day_for(now_value)
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute(
                "SELECT * FROM compute_tasks WHERE lease_expires_at<>'' AND lease_expires_at<? AND status IN ('running','cancel_requested') ORDER BY id",
                (now,),
            ).fetchall()
            for task in rows:
                before = dict(task)
                if task["status"] == "cancel_requested":
                    # 取消请求早已释放运行额度；工作者失联则直接落为已取消。
                    self._release_holds(repository, task, ("running",), "lease_expired_cancel", actor, now, business_day)
                    connection.execute(
                        "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',last_error_code='cancelled',last_error_message='租约过期且取消请求未确认',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, now, task["id"]),
                    )
                    exhausted.append(int(task["id"]))
                    after = dict(repository.task_by_id(task["id"]))
                    repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期，取消请求未确认", before=before, after=after, batch_key="", now=now)
                    continue
                self._release_holds(repository, task, ("running",), "lease_expired", actor, now, business_day)
                # 排队额度仍有空位才回到队列，否则按失败终止，避免恢复动作绕过额度。
                can_requeue = int(task["attempt_count"]) < int(task["max_attempts"]) and self._has_capacity(
                    repository, task["quota_subject_type"], task["quota_subject_key"], now_value, ("queued",)
                )
                if can_requeue:
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                if can_requeue:
                    self._acquire_hold(repository, task, "queued", business_day, "lease_recovery", actor, now)
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[..., None]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        business_day = business_day_for(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, repository, task, now, business_day, actor)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def acknowledge_cancel(self, task_id: int, worker_id: str, note: str) -> dict[str, Any]:
        """运行中的任务收到取消请求后，由持单工作者确认收尾。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        business_day = business_day_for(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "cancel_requested":
                raise ConflictError("当前任务没有待确认的取消请求")
            if task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            before = dict(task)
            # 取消请求时已释放运行额度，这里只做状态收尾并留审计。
            self._release_holds(repository, task, ("running",), "cancel_ack", worker_id, now, business_day)
            connection.execute(
                "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',last_error_code='cancelled',last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (note[:2000], now, now, task_id),
            )
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=worker_id, action="cancel_ack", reason=note, before=before, after=after, batch_key="", now=now)
            return after

    def _cancel_mutation(self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, now: str, business_day: str, actor: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        if task["status"] == "running":
            # 运行额度立即释放，其他家庭可顶补；工作者确认后状态落为已取消。
            status, finished = "cancel_requested", None
            self._release_holds(repository, task, ("running",), "cancel_requested", actor, now, business_day)
        else:
            status, finished = "cancelled", now
            self._release_holds(repository, task, ("queued",), "cancel", actor, now, business_day)
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, finished, now, task["id"]))

    def quota_status(self, requested_by: str) -> dict[str, Any]:
        """返回家庭额度、已用量与剩余量，供接口和超额错误统一解释。"""
        now_value = self.clock.now()
        business_day = business_day_for(now_value)
        quota_row = self.repository.quota("user", requested_by)
        limits = {
            "max_queued": int(quota_row["max_queued"]) if quota_row else None,
            "max_running": int(quota_row["max_running"]) if quota_row else None,
            "daily_submissions": int(quota_row["daily_submissions"]) if quota_row else None,
        }
        usage = self.repository.quota_usage("user", requested_by, business_day)
        used = {"queued": usage["queued"], "running": usage["running"], "daily": usage["daily"]}
        remaining = {
            "queued": None if limits["max_queued"] is None else max(0, limits["max_queued"] - used["queued"]),
            "running": None if limits["max_running"] is None else max(0, limits["max_running"] - used["running"]),
            "daily": None if limits["daily_submissions"] is None else max(0, limits["daily_submissions"] - used["daily"]),
        }
        return {
            "subject_type": "user",
            "subject_key": requested_by,
            "business_day": business_day,
            "limits": limits,
            "used": used,
            "remaining": remaining,
        }

    def _has_capacity(self, repository: ComputeRepository, subject_type: str, subject_key: str, now_value: datetime, buckets: tuple[str, ...]) -> bool:
        quota = repository.quota(subject_type, subject_key)
        if quota is None:
            return True
        usage = repository.quota_usage(subject_type, subject_key, business_day_for(now_value))
        for bucket, limit_field, _label in self.QUOTA_BUCKETS:
            if bucket in buckets and usage[bucket] >= int(quota[limit_field]):
                return False
        return True

    def _require_capacity(self, repository: ComputeRepository, subject_type: str, subject_key: str, now_value: datetime, buckets: tuple[str, ...]) -> None:
        quota = repository.quota(subject_type, subject_key)
        if quota is None:
            return
        business_day = business_day_for(now_value)
        usage = repository.quota_usage(subject_type, subject_key, business_day)
        violations: list[dict[str, Any]] = []
        for bucket, limit_field, label in self.QUOTA_BUCKETS:
            if bucket not in buckets:
                continue
            limit = int(quota[limit_field])
            if usage[bucket] >= limit:
                violations.append({
                    "bucket": bucket,
                    "label": f"{label}额度",
                    "limit": limit,
                    "used": usage[bucket],
                    "remaining": 0,
                })
        if violations:
            raise QuotaExceededError(
                "家庭服务额度已用尽：" + "、".join(f"{item['label']} 剩余 0/{item['limit']}" for item in violations),
                context={
                    "subject_type": subject_type,
                    "subject_key": subject_key,
                    "business_day": business_day,
                    "limits": {
                        "queued": int(quota["max_queued"]),
                        "running": int(quota["max_running"]),
                        "daily": int(quota["daily_submissions"]),
                    },
                    "used": {"queued": usage["queued"], "running": usage["running"], "daily": usage["daily"]},
                    "remaining": {
                        "queued": max(0, int(quota["max_queued"]) - usage["queued"]),
                        "running": max(0, int(quota["max_running"]) - usage["running"]),
                        "daily": max(0, int(quota["daily_submissions"]) - usage["daily"]),
                    },
                    "violations": violations,
                },
            )

    def _acquire_hold(self, repository: ComputeRepository, task: sqlite3.Row, bucket: str, business_day: str, reason: str, actor: str, now: str) -> None:
        holds = repository.ledger_task_holds(int(task["id"]))
        if holds.get(bucket, 0) > 0:
            return
        repository.add_ledger_entry(
            subject_type=task["quota_subject_type"], subject_key=task["quota_subject_key"],
            task_id=int(task["id"]), bucket=bucket, delta=1, business_day=business_day,
            reason=reason, actor=actor, idempotency_key=task["idempotency_key"], now=now,
        )

    def _release_holds(self, repository: ComputeRepository, task: sqlite3.Row, buckets: tuple[str, ...], reason: str, actor: str, now: str, business_day: str) -> None:
        holds = repository.ledger_task_holds(int(task["id"]))
        for bucket in buckets:
            if holds.get(bucket, 0) <= 0:
                continue
            repository.add_ledger_entry(
                subject_type=task["quota_subject_type"], subject_key=task["quota_subject_key"],
                task_id=int(task["id"]), bucket=bucket, delta=-1, business_day=business_day,
                reason=reason, actor=actor, idempotency_key=task["idempotency_key"], now=now,
            )

    def _transfer_hold(self, repository: ComputeRepository, *, task_id: int, subject_type: str, subject_key: str, from_bucket: str, to_bucket: str, business_day: str, reason: str, actor: str, idempotency_key: str, now: str) -> None:
        holds = repository.ledger_task_holds(task_id)
        if holds.get(from_bucket, 0) > 0:
            repository.add_ledger_entry(
                subject_type=subject_type, subject_key=subject_key, task_id=task_id,
                bucket=from_bucket, delta=-1, business_day=business_day, reason=reason, actor=actor,
                idempotency_key=idempotency_key, now=now,
            )
        if holds.get(to_bucket, 0) <= 0:
            repository.add_ledger_entry(
                subject_type=subject_type, subject_key=subject_key, task_id=task_id,
                bucket=to_bucket, delta=1, business_day=business_day, reason=reason, actor=actor,
                idempotency_key=idempotency_key, now=now,
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
