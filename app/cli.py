from __future__ import annotations

import argparse
import json
import threading
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.core.errors import ConflictError
from app.database import database_path, get_connection, init_db
from app.main import app


def utc_stamp() -> str:
    from app.core.clock import utc_now
    return utc_now().strftime("%Y%m%d%H%M%S%f")


QUOTA_CHECK_TEMPLATE = {
    "code": "quota-check",
    "name": "额度校验模板",
    "algorithm": "quota-check",
    "parameter_schema": {
        "samples": {"type": "integer", "required": True, "minimum": 1, "maximum": 1000000},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def command_init() -> int:
    init_db()
    print(json.dumps({"database": str(database_path()), "status": "initialized"}, ensure_ascii=False))
    return 0


def command_check() -> int:
    init_db()
    connection = get_connection()
    result = {
        "database": str(database_path()),
        "integrity": connection.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "tables": connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["integrity"] == "ok" and result["foreign_keys"] == 1 else 1


def command_smoke() -> int:
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    result = {"root": root.json(), "health": health.json(), "status_codes": [root.status_code, health.status_code]}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status_codes"] == [200, 200] else 1


def command_compute_demo() -> int:
    template = {
        "code": "monte-carlo-demo",
        "name": "蒙特卡洛演示",
        "algorithm": "monte-carlo",
        "parameter_schema": {
            "samples": {"type": "integer", "required": True, "minimum": 10, "maximum": 1000000},
            "seed": {"type": "integer", "required": True},
        },
        "default_parameters": {},
        "max_runtime_seconds": 60,
        "max_attempts": 3,
    }
    with TestClient(app) as client:
        created = client.post("/api/compute/templates?actor=cli-demo", json=template)
        if created.status_code not in {201, 409}:
            print(created.text)
            return 1
        task = client.post(
            "/api/compute/tasks",
            json={
                "template_code": "monte-carlo-demo",
                "project_code": "demo",
                "requested_by": "cli-user",
                "parameters": {"samples": 1000, "seed": 42},
                "priority": 80,
                "idempotency_key": "compute-demo-000001",
            },
        )
        claimed = client.post(
            "/api/compute/tasks/claim",
            json={"worker_id": "cli-worker", "capabilities": ["monte-carlo"], "lease_seconds": 60},
        )
    result = {"task": task.status_code, "claimed": claimed.status_code, "task_id": task.json().get("id")}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if task.status_code == 202 and claimed.status_code == 200 and claimed.json().get("task") else 1


def command_compute_quota_check() -> int:
    """重复提交、并发领取、取消重试与跨日额度的一致性维护检查。"""
    from app.compute.service import ComputeOperationsService
    from app.core.clock import FrozenClock
    from app.core.errors import QuotaExceededError

    init_db()
    stamp = utc_stamp()
    family = f"qc-family-{stamp}"
    clock = FrozenClock(datetime(2026, 10, 3, 23, 30, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    try:
        service.create_template(QUOTA_CHECK_TEMPLATE, "cli-quota-check")
    except ConflictError:
        pass
    service.set_quota(
        {"subject_type": "user", "subject_key": family, "max_queued": 1, "max_running": 1, "daily_submissions": 2},
        "cli-quota-check",
    )
    key = f"qc-{stamp}-000001"
    payload = {
        "template_code": "quota-check", "project_code": "quota-check", "requested_by": family,
        "parameters": {"samples": 100}, "priority": 50, "idempotency_key": key,
    }

    findings: list[str] = []
    created: list[int] = []
    created_lock = threading.Lock()

    def duplicate_submit() -> None:
        local = ComputeOperationsService(get_connection(), clock)
        with created_lock:
            created.append(local.submit(dict(payload))["id"])

    threads = [threading.Thread(target=duplicate_submit) for _ in range(6)]
    for item in threads:
        item.start()
    for item in threads:
        item.join()
    if len(set(created)) != 1:
        findings.append(f"重复提交产生了多个服务单：{sorted(set(created))}")
    task_id = created[0]
    day_one = service.quota_status(family)
    if (day_one["used"]["queued"], day_one["used"]["daily"]) != (1, 1):
        findings.append(f"提交后额度占用异常：{day_one['used']}")

    blocked_context: dict | None = None
    try:
        service.submit({**payload, "idempotency_key": f"qc-{stamp}-000002"})
    except QuotaExceededError as exc:
        blocked_context = exc.context
    if blocked_context is None:
        findings.append("超额提交未返回 quota_exceeded")
    elif blocked_context["remaining"]["queued"] != 0 or blocked_context["remaining"]["daily"] != 1:
        findings.append(f"超额错误剩余额度不可解释：{blocked_context['remaining']}")

    winners: list[tuple[str, int]] = []
    winners_lock = threading.Lock()

    def concurrent_claim(worker: str) -> None:
        local = ComputeOperationsService(get_connection(), clock)
        claimed = local.claim(worker, ["quota-check"], 60)
        if claimed is not None:
            with winners_lock:
                winners.append((worker, claimed["id"]))

    claimers = [threading.Thread(target=concurrent_claim, args=(f"w-{i}",)) for i in range(4)]
    for item in claimers:
        item.start()
    for item in claimers:
        item.join()
    if [task for _worker, task in winners] != [task_id]:
        findings.append(f"并发领取结果异常：{winners}")
    winning_worker = winners[0][0] if winners else "w-0"
    if service.quota_status(family)["used"] != {"queued": 0, "running": 1, "daily": 1}:
        findings.append(f"领取后额度占用异常：{service.quota_status(family)['used']}")

    cancelled = service.cancel(task_id, "dispatcher", "车队临时调整")
    if cancelled["status"] != "cancel_requested" or service.quota_status(family)["used"]["running"] != 0:
        findings.append("取消请求未立即释放运行额度")
    acked = service.acknowledge_cancel(task_id, winning_worker, "现场确认取消")
    if acked["status"] != "cancelled":
        findings.append("取消确认状态异常")

    clock.advance(hours=12)
    day_two = service.quota_status(family)
    if day_two["business_day"] != "2026-10-04" or day_two["used"]["daily"] != 0:
        findings.append(f"跨日提交额度未按统一边界重置：{day_two['business_day']} / {day_two['used']}")

    retried = service.retry(task_id, "dispatcher", "档期恢复")
    if retried["status"] != "queued" or service.quota_status(family)["used"]["daily"] != 0:
        findings.append("取消后重试异常地重新占用了当日提交额度")
    claimed = service.claim("w-9", ["quota-check"], 60)
    if claimed is None or claimed["id"] != task_id:
        findings.append("重试后的服务单未能重新领取")
    failed_once = service.fail(task_id, "w-9", "transient", "车辆故障", True)
    if failed_once["status"] != "queued" or service.quota_status(family)["used"] != {"queued": 1, "running": 0, "daily": 0}:
        findings.append(f"失败重试额度流转异常：{failed_once['status']} / {service.quota_status(family)['used']}")
    clock.advance(seconds=2)
    reclaimed = service.claim("w-9", ["quota-check"], 60)
    if reclaimed is None or reclaimed["id"] != task_id:
        findings.append("失败重试后的服务单未能重新领取")
    failed_final = service.fail(task_id, "w-9", "fatal", "再次故障", True)
    if failed_final["status"] != "failed" or service.quota_status(family)["used"] != {"queued": 0, "running": 0, "daily": 0}:
        findings.append(f"失败终止后额度未全部释放：{service.quota_status(family)['used']}")

    ledger = service.repository.ledger_entries_for_task(task_id)
    paired = {"queued": 0, "running": 0, "daily": 0}
    for entry in ledger:
        paired[entry["bucket"]] += int(entry["delta"])
    if any(value < 0 for value in paired.values()):
        findings.append(f"台账出现负占用：{paired}")

    report = {
        "family": family,
        "task_id": task_id,
        "duplicate_submissions": len(created),
        "distinct_orders": len(set(created)),
        "concurrent_claim_winners": [{"worker": worker, "task_id": task} for worker, task in winners],
        "quota_after_submit": day_one["used"],
        "over_quota_remaining": None if blocked_context is None else blocked_context["remaining"],
        "quota_next_business_day": day_two["used"],
        "ledger_entries": len(ledger),
        "findings": findings,
        "status": "ok" if not findings else "failed",
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not findings else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="ceremony-operations", description="红白喜事服务运营平台维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")
    subparsers.add_parser("compute-demo", help="执行计算任务提交与领取演示")
    subparsers.add_parser("compute-quota-check", help="校验幂等提交、并发领取、取消重试与跨日额度")
    args = parser.parse_args()
    commands = {
        "init-db": command_init,
        "check-db": command_check,
        "smoke": command_smoke,
        "compute-demo": command_compute_demo,
        "compute-quota-check": command_compute_quota_check,
    }
    return commands[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
