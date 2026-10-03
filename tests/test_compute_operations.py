from __future__ import annotations

import json
import threading
from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import QuotaExceededError
from app.database import get_connection, transaction


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "tolerance": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {"tolerance": 0.001},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def test_template_submission_idempotency_and_parameter_validation(client):
    create_template(client)
    first = client.post("/api/compute/tasks", json=submit_payload("request-000001"))
    second = client.post("/api/compute/tasks", json=submit_payload("request-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    invalid = submit_payload("request-000002")
    invalid["parameters"]["iterations"] = 20000
    rejected = client.post("/api/compute/tasks", json=invalid)
    assert rejected.status_code == 422


def test_priority_capability_claim_and_result_version(client):
    create_template(client)
    low = client.post("/api/compute/tasks", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/compute/tasks", json=submit_payload("priority-high", priority=90)).json()
    no_match = client.post("/api/compute/tasks/claim", json={"worker_id": "w0", "capabilities": ["other"], "lease_seconds": 60})
    assert no_match.status_code == 200 and no_match.json()["task"] is None
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200
    assert claimed.json()["task"]["id"] == high["id"]
    completed = client.post(
        f"/api/compute/tasks/{high['id']}/complete",
        json={"worker_id": "w1", "result": {"value": 3.14}, "metrics": {"seconds": 2}},
    )
    assert completed.status_code == 200
    details = client.get(f"/api/compute/task-details/{high['id']}").json()
    assert details["status"] == "succeeded"
    assert details["current_result_version"] == 1
    assert len(details["results"]) == 1
    assert low["status"] == "queued"


def test_quota_cancel_retry_priority_and_batch_interventions(client):
    create_template(client)
    quota = client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    one = client.post("/api/compute/tasks", json=submit_payload("quota-one", user="limited")).json()
    blocked = client.post("/api/compute/tasks", json=submit_payload("quota-two", user="limited"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/compute/tasks/{one['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    retried = client.post(f"/api/compute/tasks/{one['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 95})
    assert retried.status_code == 200 and retried.json()["priority"] == 95
    other = client.post("/api/compute/tasks", json=submit_payload("batch-other", user="other-user")).json()
    batch = client.post(
        "/api/compute/tasks/batch",
        json={"task_ids": [one["id"], other["id"]], "operation": "priority", "actor": "administrator", "reason": "紧急算例", "priority": 99},
    )
    assert batch.status_code == 200
    assert len(batch.json()["succeeded"]) == 2
    details = client.get(f"/api/compute/task-details/{one['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "retry", "priority"]


def test_failure_backoff_and_expired_lease_recovery(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("worker-a", ["solver-a"], 10)
    assert claimed and claimed["id"] == first["id"]
    failed = service.fail(first["id"], "worker-a", "numeric_error", "数值不收敛", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("worker-a", ["solver-a"], 10)
    assert claimed_again and claimed_again["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_task(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"


def set_user_quota(client, subject_key: str, *, max_queued: int = 10, max_running: int = 10, daily_submissions: int = 100) -> None:
    response = client.put(
        "/api/compute/quotas?actor=administrator",
        json={
            "subject_type": "user",
            "subject_key": subject_key,
            "max_queued": max_queued,
            "max_running": max_running,
            "daily_submissions": daily_submissions,
        },
    )
    assert response.status_code == 200, response.text


def quota_usage(client, subject_key: str) -> dict:
    response = client.get("/api/compute/quotas/usage", params={"subject_key": subject_key})
    assert response.status_code == 200, response.text
    return response.json()


def test_repeated_submission_counts_demand_once(client):
    create_template(client)
    set_user_quota(client, "family-repeat", max_queued=5, max_running=2, daily_submissions=5)
    for _ in range(3):
        response = client.post("/api/compute/tasks", json=submit_payload("repeat-000001", user="family-repeat"))
        assert response.status_code == 202
    first_id = response.json()["id"]
    usage = quota_usage(client, "family-repeat")
    assert usage["dimensions"]["queued"]["used"] == 1
    assert usage["dimensions"]["daily_submissions"]["used"] == 1
    tasks = client.get("/api/compute/tasks", params={"requested_by": "family-repeat"}).json()["items"]
    assert [task["id"] for task in tasks] == [first_id]
    events = client.get("/api/compute/quotas/events", params={"subject_key": "family-repeat"}).json()["items"]
    assert len(events) == 2
    assert {(event["dimension"], event["action"]) for event in events} == {("queued", "acquire"), ("daily_submissions", "acquire")}


def test_concurrent_duplicate_submissions_create_single_task(client):
    create_template(client)
    clock = FrozenClock(datetime(2026, 10, 3, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    results: list[dict] = []
    errors: list[Exception] = []

    def work() -> None:
        try:
            results.append(service.submit(submit_payload("race-submit-0001", user="family-race")))
        except Exception as exc:  # noqa: BLE001 - 测试需要捕获全部并发异常
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(results) == 8
    assert len({task["id"] for task in results}) == 1
    tasks = service.list_tasks(requested_by="family-race")
    assert len(tasks) == 1


def test_concurrent_claims_respect_running_quota(client):
    create_template(client)
    set_user_quota(client, "family-claim", max_queued=10, max_running=1, daily_submissions=10)
    clock = FrozenClock(datetime(2026, 10, 3, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    for index in range(3):
        service.submit(submit_payload(f"race-claim-{index:04d}", user="family-claim"))
    claimed: list[dict | None] = []
    lock = threading.Lock()

    def work(worker_id: str) -> None:
        task = service.claim(worker_id, ["solver-a"], 60)
        with lock:
            claimed.append(task)

    threads = [threading.Thread(target=work, args=(f"worker-{index}",)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    winners = [task for task in claimed if task is not None]
    assert len(winners) == 1
    usage = quota_usage(client, "family-claim")
    assert usage["dimensions"]["running"]["used"] == 1
    assert usage["dimensions"]["running"]["remaining"] == 0
    assert usage["dimensions"]["queued"]["used"] == 2


def test_claim_skips_family_with_exhausted_running_quota(client):
    create_template(client)
    set_user_quota(client, "family-full", max_queued=10, max_running=1, daily_submissions=10)
    first = client.post("/api/compute/tasks", json=submit_payload("skip-a-0001", user="family-full", priority=90)).json()
    client.post("/api/compute/tasks", json=submit_payload("skip-a-0002", user="family-full", priority=80))
    other = client.post("/api/compute/tasks", json=submit_payload("skip-b-0001", user="family-other", priority=10)).json()
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"]
    assert claimed["id"] == first["id"]
    # family-full 运行额度已用尽，即使其订单优先级更高，也领取 family-other 的订单。
    claimed_next = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"]
    assert claimed_next["id"] == other["id"]
    empty = client.post("/api/compute/tasks/claim", json={"worker_id": "w3", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"]
    assert empty is None


def test_cancel_requested_holds_running_quota_until_recovery(client):
    create_template(client)
    set_user_quota(client, "family-hold", max_queued=10, max_running=1, daily_submissions=10)
    clock = FrozenClock(datetime(2026, 10, 3, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    first = service.submit(submit_payload("cancel-hold-0001", user="family-hold"))
    second = service.submit(submit_payload("cancel-hold-0002", user="family-hold"))
    claimed = service.claim("w1", ["solver-a"], 30)
    assert claimed and claimed["id"] == first["id"]
    cancelled = service.cancel(first["id"], "dispatcher", "车队临时调拨")
    assert cancelled["status"] == "cancel_requested"
    # 取消请求中的订单仍持有车辆与人工，同家庭其他订单不能绕过额度进入执行队列。
    assert service.claim("w2", ["solver-a"], 30) is None
    usage = service.quota_usage("user", "family-hold")
    assert usage["dimensions"]["running"]["used"] == 1
    clock.advance(seconds=31)
    recovered = service.recover_expired()
    assert recovered["cancelled"] == [first["id"]]
    assert service.get_task(first["id"])["status"] == "cancelled"
    assert service.quota_usage("user", "family-hold")["dimensions"]["running"]["used"] == 0
    claimed_next = service.claim("w2", ["solver-a"], 30)
    assert claimed_next and claimed_next["id"] == second["id"]


def test_cancel_and_retry_keep_quota_effects_consistent(client):
    create_template(client)
    set_user_quota(client, "family-cycle", max_queued=1, max_running=1, daily_submissions=5)
    one = client.post("/api/compute/tasks", json=submit_payload("cycle-000001", user="family-cycle")).json()
    blocked = client.post("/api/compute/tasks", json=submit_payload("cycle-000002", user="family-cycle"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/compute/tasks/{one['id']}/cancel", json={"actor": "dispatcher", "reason": "婚礼改期"})
    assert cancelled.status_code == 200
    usage = quota_usage(client, "family-cycle")
    assert usage["dimensions"]["queued"]["used"] == 0
    assert usage["dimensions"]["daily_submissions"]["used"] == 1
    retried = client.post(f"/api/compute/tasks/{one['id']}/retry", json={"actor": "dispatcher", "reason": "重新排期"})
    assert retried.status_code == 200 and retried.json()["status"] == "queued"
    usage = quota_usage(client, "family-cycle")
    assert usage["dimensions"]["queued"]["used"] == 1
    # 取消后重试复用同一服务单，不重复计入当日提交额度。
    assert usage["dimensions"]["daily_submissions"]["used"] == 1
    blocked_again = client.post("/api/compute/tasks", json=submit_payload("cycle-000002", user="family-cycle"))
    assert blocked_again.status_code == 409


def test_over_quota_response_explains_remaining_quota(client):
    create_template(client)
    set_user_quota(client, "family-explain", max_queued=1, max_running=2, daily_submissions=5)
    created = client.post("/api/compute/tasks", json=submit_payload("explain-000001", user="family-explain"))
    assert created.status_code == 202
    blocked = client.post("/api/compute/tasks", json=submit_payload("explain-000002", user="family-explain"))
    assert blocked.status_code == 409
    error = blocked.json()["error"]
    assert error["code"] == "quota_exceeded"
    assert error["context"]["dimension"] == "queued"
    assert error["context"]["limit"] == 1
    assert error["context"]["used"] == 1
    assert error["context"]["remaining"] == 0


def test_daily_quota_resets_on_unified_utc_day_boundary(client):
    create_template(client)
    set_user_quota(client, "family-daily", max_queued=10, max_running=2, daily_submissions=1)
    clock = FrozenClock(datetime(2026, 10, 3, 23, 59, 50, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    first = service.submit(submit_payload("daily-000001", user="family-daily"))
    assert first["status"] == "queued"
    with pytest.raises(QuotaExceededError):
        service.submit(submit_payload("daily-000002", user="family-daily"))
    clock.advance(seconds=20)
    second = service.submit(submit_payload("daily-000002", user="family-daily"))
    assert second["status"] == "queued"
    usage = service.quota_usage("user", "family-daily")
    assert usage["day_start"] == "2026-10-04T00:00:00+00:00"
    assert usage["dimensions"]["daily_submissions"]["used"] == 1
    assert usage["dimensions"]["queued"]["used"] == 2


def test_quota_ledger_records_full_lifecycle(client):
    create_template(client)
    set_user_quota(client, "family-ledger", max_queued=5, max_running=1, daily_submissions=5)
    clock = FrozenClock(datetime(2026, 10, 3, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    task = service.submit(submit_payload("ledger-000001", user="family-ledger"))
    claimed = service.claim("w1", ["solver-a"], 30)
    assert claimed and claimed["id"] == task["id"]
    service.complete(task["id"], "w1", {"value": 1}, {})
    events = service.quota_events("user", "family-ledger")
    sequence = [(event["dimension"], event["action"]) for event in reversed(events)]
    assert sequence == [
        ("queued", "acquire"),
        ("daily_submissions", "acquire"),
        ("queued", "release"),
        ("running", "acquire"),
        ("running", "release"),
    ]
    assert all(event["day_bucket"] == "2026-10-03" for event in events)
    usage = service.quota_usage("user", "family-ledger")
    assert usage["dimensions"]["queued"]["used"] == 0
    assert usage["dimensions"]["running"]["used"] == 0
    assert usage["dimensions"]["daily_submissions"]["used"] == 1


def test_cli_quota_report_reflects_stable_usage(client, capsys):
    from app.cli import command_compute_quota_report

    create_template(client)
    set_user_quota(client, "family-report", max_queued=5, max_running=2, daily_submissions=5)
    client.post("/api/compute/tasks", json=submit_payload("report-000001", user="family-report"))
    assert command_compute_quota_report() == 0
    report = json.loads(capsys.readouterr().out)
    subject = next(item for item in report["subjects"] if item["subject_key"] == "family-report")
    assert subject["dimensions"]["queued"] == {"limit": 5, "used": 1, "remaining": 4}
    assert subject["dimensions"]["daily_submissions"] == {"limit": 5, "used": 1, "remaining": 4}
