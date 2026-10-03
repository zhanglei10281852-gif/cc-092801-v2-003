from __future__ import annotations

import threading
from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, init_db, transaction


TEMPLATE = {
    "code": "fleet-a",
    "name": "婚礼车队模板",
    "algorithm": "fleet-a",
    "parameter_schema": {
        "cars": {"type": "integer", "required": True, "minimum": 1, "maximum": 100},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def payload(key: str, *, user: str = "family-a", priority: int = 50) -> dict:
    return {
        "template_code": "fleet-a",
        "project_code": "wedding-season",
        "requested_by": user,
        "parameters": {"cars": 6},
        "priority": priority,
        "idempotency_key": key,
    }


@pytest.fixture()
def service(tmp_path, monkeypatch) -> ComputeOperationsService:
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "quota-ledger.db"))
    from app.database import close_connection
    close_connection()
    init_db()
    svc = ComputeOperationsService(get_connection(), FrozenClock(datetime(2026, 10, 3, 23, 30, tzinfo=UTC)))
    svc.create_template(TEMPLATE, "administrator")
    return svc


def set_quota(svc: ComputeOperationsService, user: str, *, queued: int, running: int, daily: int) -> None:
    svc.set_quota(
        {"subject_type": "user", "subject_key": user, "max_queued": queued, "max_running": running, "daily_submissions": daily},
        "administrator",
    )


def test_duplicate_submission_creates_single_trackable_order(service: ComputeOperationsService) -> None:
    first = service.submit(payload("idem-000001"))
    second = service.submit(payload("idem-000001"))
    third = service.submit(payload("idem-000001"))
    assert first["id"] == second["id"] == third["id"]

    tasks = service.list_tasks(requested_by="family-a")
    assert len(tasks) == 1
    usage = service.quota_status("family-a")
    assert usage["used"] == {"queued": 1, "running": 0, "daily": 1}

    ledger = service.repository.ledger_entries_for_task(first["id"])
    assert [(row["bucket"], row["delta"], row["reason"]) for row in ledger] == [
        ("daily", 1, "submit"),
        ("queued", 1, "submit"),
    ]


def test_concurrent_duplicate_submission_still_single_order(service: ComputeOperationsService) -> None:
    created: list[int] = []
    lock = threading.Lock()

    def submit() -> None:
        local = ComputeOperationsService(get_connection(), service.clock)
        with lock:
            created.append(local.submit(payload("idem-concurrent"))["id"])

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(created) == 8
    assert set(created) == {created[0]}
    assert service.quota_status("family-a")["used"] == {"queued": 1, "running": 0, "daily": 1}


def test_over_quota_returns_explainable_remaining_limits(service: ComputeOperationsService) -> None:
    from app.core.errors import QuotaExceededError

    set_quota(service, "family-limited", queued=1, running=1, daily=3)
    service.submit(payload("q-one", user="family-limited"))

    with pytest.raises(QuotaExceededError) as exc_info:
        service.submit(payload("q-two", user="family-limited"))

    context = exc_info.value.context
    assert context["remaining"] == {"queued": 0, "running": 1, "daily": 2}
    assert context["limits"] == {"queued": 1, "running": 1, "daily": 3}
    assert context["used"] == {"queued": 1, "running": 0, "daily": 1}
    assert context["business_day"] == "2026-10-03"
    assert [item["bucket"] for item in context["violations"]] == ["queued"]
    assert "排队额度" in exc_info.value.message


def test_over_quota_api_response_shape(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201
    client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "fam", "max_queued": 1, "max_running": 1, "daily_submissions": 1},
    )
    assert client.post("/api/compute/tasks", json=payload("api-one", user="fam")).status_code == 202
    blocked = client.post("/api/compute/tasks", json=payload("api-two", user="fam"))
    assert blocked.status_code == 409
    body = blocked.json()["error"]
    assert body["code"] == "quota_exceeded"
    assert body["context"]["remaining"] == {"queued": 0, "running": 1, "daily": 0}

    status = client.get("/api/compute/quotas/user/fam")
    assert status.status_code == 200
    assert status.json()["remaining"] == {"queued": 0, "running": 1, "daily": 0}


def test_concurrent_claim_has_single_winner_and_moves_hold(service: ComputeOperationsService) -> None:
    task = service.submit(payload("claim-race"))
    winners: list[str] = []
    lock = threading.Lock()

    def claim(worker: str) -> None:
        local = ComputeOperationsService(get_connection(), service.clock)
        claimed = local.claim(worker, ["fleet-a"], 60)
        if claimed is not None:
            with lock:
                winners.append(worker)

    threads = [threading.Thread(target=claim, args=(f"worker-{i}",)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert winners and len(winners) == 1
    details = service.get_task(task["id"])
    assert details["status"] == "running"
    assert details["lease_owner"] == winners[0]
    assert service.quota_status("family-a")["used"] == {"queued": 0, "running": 1, "daily": 1}


def test_claim_skips_families_without_running_capacity(client) -> None:
    client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "full", "max_queued": 5, "max_running": 1, "daily_submissions": 5},
    )
    running_task = client.post("/api/compute/tasks", json=payload("running", user="full")).json()
    first_claim = client.post(
        "/api/compute/tasks/claim", json={"worker_id": "w0", "capabilities": ["fleet-a"], "lease_seconds": 60}
    ).json()["task"]
    assert first_claim["id"] == running_task["id"]

    # 队列首位的家庭运行额度已占满，应跳过它领取后面的家庭。
    blocked_task = client.post("/api/compute/tasks", json=payload("blocked-task", user="full")).json()
    open_task = client.post("/api/compute/tasks", json=payload("open-task-01", user="free")).json()

    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w", "capabilities": ["fleet-a"], "lease_seconds": 60})
    assert claimed.status_code == 200
    assert claimed.json()["task"]["id"] == open_task["id"]
    assert client.get(f"/api/compute/task-details/{blocked_task['id']}").json()["status"] == "queued"

    # 运行中的单完成释放额度后，被跳过的排队单可被领取。
    client.post(
        f"/api/compute/tasks/{running_task['id']}/complete",
        json={"worker_id": "w0", "result": {"ok": True}, "metrics": {}},
    )
    reclaimed = client.post(
        "/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["fleet-a"], "lease_seconds": 60}
    ).json()["task"]
    assert reclaimed["id"] == blocked_task["id"]


def test_cancel_release_retry_reacquires_and_retry_respects_quota(service: ComputeOperationsService) -> None:
    from app.core.errors import QuotaExceededError

    set_quota(service, "family-limited", queued=1, running=1, daily=5)
    task = service.submit(payload("cancel-retry", user="family-limited"))

    cancelled = service.cancel(task["id"], "dispatcher", "档期冲突")
    assert cancelled["status"] == "cancelled"
    assert service.quota_status("family-limited")["used"] == {"queued": 0, "running": 0, "daily": 1}

    # 排队额度被别的服务单占满时，人工重试必须被拒绝并返回剩余额度。
    holder = service.submit(payload("holder", user="family-limited"))
    with pytest.raises(QuotaExceededError) as exc_info:
        service.retry(task["id"], "dispatcher", "档期恢复")
    assert exc_info.value.context["remaining"]["queued"] == 0
    assert service.get_task(task["id"])["status"] == "cancelled"

    service.cancel(holder["id"], "dispatcher", "腾出额度")
    retried = service.retry(task["id"], "dispatcher", "档期恢复")
    assert retried["status"] == "queued"
    # 重试不是新提交：当日提交计数仍只是两笔原始提交（任务单 + 占额单）。
    assert service.quota_status("family-limited")["used"] == {"queued": 1, "running": 0, "daily": 2}

    reasons = [row["reason"] for row in service.repository.ledger_entries_for_task(task["id"])]
    assert reasons == ["submit", "submit", "cancel", "retry"]


def test_running_cancel_releases_slot_immediately_and_acknowledges(service: ComputeOperationsService) -> None:
    task = service.submit(payload("running-cancel"))
    service.claim("worker-1", ["fleet-a"], 60)
    assert service.quota_status("family-a")["used"]["running"] == 1

    requesting = service.cancel(task["id"], "dispatcher", "天气原因")
    assert requesting["status"] == "cancel_requested"
    assert service.quota_status("family-a")["used"] == {"queued": 0, "running": 0, "daily": 1}

    # 释放出的运行额度可立即被同一家庭的下一单使用。
    set_quota(service, "family-a", queued=5, running=1, daily=5)
    other = service.submit(payload("running-cancel-next"))
    next_claim = service.claim("worker-2", ["fleet-a"], 60)
    assert next_claim["id"] == other["id"]

    acknowledged = service.acknowledge_cancel(task["id"], "worker-1", "现场已收尾")
    assert acknowledged["status"] == "cancelled"
    actions = [row["action"] for row in service.get_task(task["id"])["interventions"]]
    assert actions == ["cancel", "cancel_ack"]


def test_failure_retry_keeps_quota_consistent(service: ComputeOperationsService) -> None:
    set_quota(service, "family-a", queued=1, running=1, daily=5)
    task = service.submit(payload("fail-retry"))
    service.claim("worker-1", ["fleet-a"], 60)
    failed = service.fail(task["id"], "worker-1", "transient", "司机迟到", True)
    assert failed["status"] == "queued"
    assert service.quota_status("family-a")["used"] == {"queued": 1, "running": 0, "daily": 1}

    service.clock.advance(seconds=2)
    service.claim("worker-1", ["fleet-a"], 60)
    service.fail(task["id"], "worker-1", "transient", "再次故障", True)
    service.clock.advance(seconds=4)
    service.claim("worker-1", ["fleet-a"], 60)
    terminal = service.fail(task["id"], "worker-1", "fatal", "车辆无法到场", True)
    assert terminal["status"] == "failed"
    assert service.quota_status("family-a")["used"] == {"queued": 0, "running": 0, "daily": 1}


def test_failure_requeue_blocked_when_queued_quota_full(service: ComputeOperationsService) -> None:
    set_quota(service, "family-a", queued=1, running=1, daily=5)
    task = service.submit(payload("fail-blocked"))
    service.claim("worker-1", ["fleet-a"], 60)

    # 任务运行中排队额度被其他单占满（额度被调小），失败后不得强行回队。
    service.set_quota(
        {"subject_type": "user", "subject_key": "family-a", "max_queued": 0, "max_running": 1, "daily_submissions": 5},
        "administrator",
    )
    terminal = service.fail(task["id"], "worker-1", "transient", "无法回队", True)
    assert terminal["status"] == "failed"
    assert terminal["last_error_code"] == "retry_quota_exhausted"
    assert service.quota_status("family-a")["used"] == {"queued": 0, "running": 0, "daily": 1}


def test_daily_quota_uses_unified_utc_day_boundary(service: ComputeOperationsService) -> None:
    set_quota(service, "family-a", queued=10, running=10, daily=1)
    service.submit(payload("day-one"))
    assert service.quota_status("family-a")["used"]["daily"] == 1

    # 23:30 + 20 分钟 = 次日 00:10（UTC），当日提交额度按统一边界重置。
    service.clock.advance(minutes=40)
    assert service.quota_status("family-a")["business_day"] == "2026-10-04"
    next_day = service.submit(payload("day-two"))
    assert next_day["status"] == "queued"
    assert service.quota_status("family-a")["used"]["daily"] == 1

    ledger_days = {row["business_day"] for row in service.repository.ledger_entries_for_task(next_day["id"])}
    assert ledger_days == {"2026-10-04"}


def test_replay_after_completion_returns_same_order_without_new_hold(service: ComputeOperationsService) -> None:
    task = service.submit(payload("replay-done"))
    claimed = service.claim("worker-1", ["fleet-a"], 60)
    service.complete(task["id"], "worker-1", {"delivered": True}, {})
    assert service.quota_status("family-a")["used"] == {"queued": 0, "running": 0, "daily": 1}

    replayed = service.submit(payload("replay-done"))
    assert replayed["id"] == task["id"]
    assert replayed["status"] == "succeeded"
    assert service.quota_status("family-a")["used"] == {"queued": 0, "running": 0, "daily": 1}
    assert claimed["attempt_count"] == 1


def test_lease_recovery_releases_and_reattaches_holds(service: ComputeOperationsService) -> None:
    set_quota(service, "family-a", queued=1, running=1, daily=5)
    task = service.submit(payload("lease-expired"))
    service.claim("worker-1", ["fleet-a"], 10)
    service.clock.advance(seconds=11)

    result = service.recover_expired()
    assert result["recovered"] == [task["id"]]
    assert service.quota_status("family-a")["used"] == {"queued": 1, "running": 0, "daily": 1}
    details = service.get_task(task["id"])
    assert details["status"] == "queued"
    assert details["interventions"][-1]["action"] == "lease_recovery"


def test_ledger_is_the_audit_basis_for_every_hold_change(client) -> None:
    client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    task = client.post("/api/compute/tasks", json=payload("audit-trail")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["fleet-a"], "lease_seconds": 60})
    client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "dispatcher", "reason": "取消"})
    client.post(f"/api/compute/tasks/{task['id']}/cancel-ack", json={"worker_id": "w1", "note": "确认"})

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    ledger = details["quota_ledger"]
    movements = [(row["bucket"], row["delta"], row["reason"]) for row in ledger]
    assert movements == [
        ("daily", 1, "submit"),
        ("queued", 1, "submit"),
        ("queued", -1, "claim"),
        ("running", 1, "claim"),
        ("running", -1, "cancel_requested"),
    ]
    for row in ledger:
        assert row["task_id"] == task["id"]
        assert row["business_day"]
        assert row["created_at"]
        assert row["idempotency_key"] == "audit-trail"


def test_legacy_database_backfills_ledger_from_existing_tasks(tmp_path, monkeypatch) -> None:
    import os
    import sqlite3

    db_path = tmp_path / "legacy.db"
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(db_path))
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        CREATE TABLE compute_templates (
            id INTEGER PRIMARY KEY, code TEXT, name TEXT, algorithm TEXT, version INTEGER,
            parameter_schema_json TEXT, default_parameters_json TEXT, max_runtime_seconds INTEGER,
            max_attempts INTEGER, active INTEGER, created_by TEXT, created_at TEXT, updated_at TEXT
        );
        CREATE TABLE compute_tasks (
            id INTEGER PRIMARY KEY, template_id INTEGER, project_code TEXT, requested_by TEXT,
            parameters_json TEXT, parameter_digest TEXT, priority INTEGER, idempotency_key TEXT,
            status TEXT, attempt_count INTEGER, max_attempts INTEGER, available_at TEXT,
            lease_owner TEXT DEFAULT '', lease_expires_at TEXT DEFAULT '', current_result_version INTEGER,
            last_error_code TEXT DEFAULT '', last_error_message TEXT DEFAULT '',
            version INTEGER DEFAULT 1, started_at TEXT, finished_at TEXT, created_at TEXT, updated_at TEXT
        );
        INSERT INTO compute_templates VALUES(1,'legacy','legacy','legacy',1,'{}','{}',300,3,1,'m','t','t');
        INSERT INTO compute_tasks(id,template_id,project_code,requested_by,parameters_json,parameter_digest,
            priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at)
        VALUES(1,1,'p','legacy-family','{}','d',50,'legacy-key','queued',0,3,'t','2026-10-02T08:00:00+00:00','2026-10-02T08:00:00+00:00');
        """
    )
    raw.commit()
    raw.close()

    from app.database import close_connection
    close_connection()
    init_db()
    close_connection()

    reopened = sqlite3.connect(db_path)
    reopened.row_factory = sqlite3.Row
    rows = reopened.execute(
        "SELECT bucket,delta,business_day,reason FROM compute_quota_ledger WHERE subject_key='legacy-family' ORDER BY id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("daily", 1, "2026-10-02", "legacy_backfill"),
        ("queued", 1, "2026-10-02", "legacy_backfill"),
    ]
    columns = {row[1] for row in reopened.execute("PRAGMA table_info(compute_tasks)").fetchall()}
    assert {"quota_subject_type", "quota_subject_key"} <= columns
    reopened.close()
    # 恢复全局连接，避免影响后续用例。
    get_connection()
    close_connection()
