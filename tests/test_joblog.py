import hashlib
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from joblog.main import create_app


@pytest.fixture()
def env(tmp_path: Path):
    app = create_app(data_dir=tmp_path)
    client = TestClient(app)
    with client:
        yield {
            "app": app,
            "client": client,
            "root": tmp_path,
            "db": app.state.database,
            "manager": app.state.manager,
            "restores": app.state.restore_dir,
        }


def append(client: TestClient, job_id: str, message: str) -> dict:
    response = client.post("/logs", json={"job_id": job_id, "message": message})
    assert response.status_code == 201, response.text
    return response.json()


def make_backup(client: TestClient) -> dict:
    response = client.post("/backups")
    assert response.status_code == 201, response.text
    return response.json()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_append_and_query(env):
    client = env["client"]
    first = append(client, "job-a", "first")
    second = append(client, "job-b", "second")
    assert second["id"] > first["id"]

    page = client.get("/logs", params={"job_id": "job-a"}).json()
    assert [item["message"] for item in page["items"]] == ["first"]

    after = client.get("/logs", params={"after_id": second["id"]}).json()
    assert after["items"] == []


def test_backup_publishes_verified_manifest_and_download(env):
    client = env["client"]
    append(client, "job-a", "before backup")
    backup = make_backup(client)

    backup_dir = env["root"] / "backups" / backup["id"]
    manifest = backup_dir / "manifest.json"
    database = backup_dir / "joblogs.db"
    assert manifest.is_file() and database.is_file()
    assert sha256(database) == backup["sha256"]["joblogs.db"]
    assert sha256(manifest) == backup["manifest_sha256"]
    # Published files must be read-only.
    assert database.stat().st_mode & 0o222 == 0

    verify = client.get(f"/backups/{backup['id']}/verify").json()
    assert verify["verified"] is True
    assert verify["snapshot_max_id"] == 1

    download = client.get(f"/backups/{backup['id']}/download")
    assert download.status_code == 200
    assert (
        hashlib.sha256(download.content).hexdigest() == backup["sha256"]["joblogs.db"]
    )

    listing = client.get("/backups").json()["items"]
    assert [item["id"] for item in listing] == [backup["id"]]


def test_snapshot_is_frozen_against_concurrent_appends(env):
    """Appends committed after the snapshot point must not enter the backup."""
    client = env["client"]
    manager = env["manager"]
    db = env["db"]
    for i in range(20):
        append(client, "job-a", f"pre-{i}")

    def write_after_freeze():
        # Runs synchronously inside create() after BEGIN fixed the snapshot.
        with db.writer() as conn:
            conn.execute(
                "INSERT INTO job_logs(job_id, message) VALUES ('job-a', 'during-backup')"
            )

    result = manager.create(after_freeze=write_after_freeze)
    assert result.snapshot_row_count == 20
    assert result.snapshot_max_id == 20

    append(client, "job-a", "after-backup")
    assert client.get("/logs").json()["next_after_id"] == 22

    restored = client.post(
        f"/backups/{result.id}/restore", json={"target_name": "frozen.db"}
    )
    assert restored.status_code == 200, restored.text
    assert restored.json()["row_count"] == 20
    logs = client.get("/restores/frozen.db/logs").json()["items"]
    assert len(logs) == 20
    assert all(item["id"] <= 20 for item in logs)
    assert "during-backup" not in {item["message"] for item in logs}
    assert "after-backup" not in {item["message"] for item in logs}


def test_concurrent_appends_do_not_corrupt_backup(env):
    client = env["client"]
    manager = env["manager"]
    db = env["db"]
    append(client, "job-a", "seed")

    stop = threading.Event()
    errors: list[BaseException] = []

    def writer(n: int):
        try:
            while not stop.is_set():
                with db.writer() as conn:
                    conn.execute(
                        "INSERT INTO job_logs(job_id, message) VALUES (?, ?)",
                        ("load", f"w{n}"),
                    )
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    try:
        result = manager.create(sleep_seconds=0.001)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert not errors, errors
    assert manager.verify(result.id)["verified"] is True

    with sqlite3.connect(env["root"] / "backups" / result.id / "joblogs.db") as conn:
        rows = conn.execute("SELECT COUNT(*), MAX(id) FROM job_logs").fetchone()
    assert rows[0] == result.snapshot_row_count
    assert rows[1] == result.snapshot_max_id
    # Source is still writable after the backup.
    append(client, "job-a", "still-writable")


def test_published_backup_does_not_change_with_source_writes(env):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "v1")
    backup = make_backup(client)
    db_path = env["root"] / "backups" / backup["id"] / "joblogs.db"
    before = sha256(db_path)

    for i in range(5):
        append(client, "job-a", f"later-{i}")
    assert manager.verify(backup["id"])["verified"] is True
    assert sha256(db_path) == before

    client.post(f"/backups/{backup['id']}/restore", json={"target_name": "old.db"})
    logs = client.get("/restores/old.db/logs").json()["items"]
    assert [item["message"] for item in logs] == ["v1"]


def test_restore_is_independent_and_keeps_source_writable(env):
    client = env["client"]
    append(client, "job-a", "committed-before")
    backup = make_backup(client)
    append(client, "job-a", "committed-after")

    result = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "target.db"}
    )
    assert result.status_code == 200, result.text
    body = result.json()
    assert body["row_count"] == 1
    assert body["max_id"] == 1

    # Append after restore: source and restored target must stay independent.
    append(client, "job-a", "source-only")
    target = env["restores"] / "target.db"
    with sqlite3.connect(target) as conn:
        assert conn.execute("SELECT COUNT(*) FROM job_logs").fetchone()[0] == 1
        conn.execute(
            "INSERT INTO job_logs(job_id, message) VALUES ('x', 'target-only')"
        )
        conn.commit()
    source = client.get("/logs").json()["items"]
    assert "target-only" not in {item["message"] for item in source}
    assert client.get("/restores/target.db/logs").json()["row_count"] == 2

    # Existing target is protected unless overwrite is requested.
    conflict = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "target.db"}
    )
    assert conflict.status_code == 409
    assert client.get("/restores/target.db/logs").json()["row_count"] == 2


def test_corrupt_backup_cannot_be_restored_and_target_is_preserved(env):
    client = env["client"]
    append(client, "job-a", "good")
    backup = make_backup(client)
    target = env["restores"] / "keep.db"
    target.write_bytes(b"original-target-bytes")
    original = sha256(target)

    db_path = env["root"] / "backups" / backup["id"] / "joblogs.db"
    db_path.chmod(0o644)
    db_path.write_bytes(db_path.read_bytes()[:4096])  # truncate -> corrupt

    verify = client.get(f"/backups/{backup['id']}/verify")
    assert verify.status_code == 409

    restore = client.post(
        f"/backups/{backup['id']}/restore",
        json={"target_name": "keep.db", "overwrite": True},
    )
    assert restore.status_code == 409, restore.text
    assert sha256(target) == original
    # Source keeps accepting writes.
    append(client, "job-a", "after-corruption")


def test_interrupted_backup_is_never_restorable(env):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "seed")
    backup_id = manager._new_id()
    staging = manager.staging / f".staging-{backup_id}"
    staging.mkdir()
    (staging / "joblogs.db").write_bytes(b"partial")
    with env["db"].writer() as conn:
        conn.execute(
            "INSERT INTO backups (id, status, root, db_file, db_sha256, manifest_sha256)"
            " VALUES (?, 'staging', ?, 'joblogs.db', '', '')",
            (backup_id, staging.as_posix()),
        )

    # Simulated restart.
    stats = manager.recover_incomplete()
    assert stats["failed_marked"] >= 1
    assert not staging.exists()
    assert client.get(f"/backups/{backup_id}").status_code == 404
    assert client.get("/backups").json()["items"] == []
    restore = client.post(
        f"/backups/{backup_id}/restore", json={"target_name": "nope.db"}
    )
    assert restore.status_code == 404

    with env["db"].reader() as conn:
        row = conn.execute(
            "SELECT status FROM backups WHERE id=?", (backup_id,)
        ).fetchone()
    assert row["status"] == "failed"
    append(client, "job-a", "source-still-writable")


def test_ready_row_with_missing_files_is_demoted(env):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "seed")
    backup = make_backup(client)
    backup_dir = env["root"] / "backups" / backup["id"]
    for file in backup_dir.iterdir():
        file.chmod(0o644)
    (backup_dir / "manifest.json").unlink()

    manager.recover_incomplete()
    assert client.get(f"/backups/{backup['id']}").status_code == 404
    assert client.get("/backups").json()["items"] == []
    append(client, "job-a", "writable-after-recovery")


def test_backup_failure_leaves_no_published_version(env, monkeypatch):
    client = env["client"]
    manager = env["manager"]
    append(client, "job-a", "seed")

    def explode(path):
        raise OSError("disk full")

    monkeypatch.setattr(manager, "_finalize_snapshot_db", explode)
    response = client.post("/backups")
    assert response.status_code == 500
    assert client.get("/backups").json()["items"] == []
    assert list(manager.staging.iterdir()) == []
    append(client, "job-a", "still-writable")

    # After the fault is cleared a new backup succeeds and restores cleanly.
    monkeypatch.undo()
    backup = make_backup(client)
    restored = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "ok.db"}
    )
    assert restored.status_code == 200
    assert restored.json()["row_count"] == 2


def test_backups_survive_service_restart(env):
    client = env["client"]
    append(client, "job-a", "before-restart")
    backup = make_backup(client)
    root = env["root"]

    restarted = create_app(data_dir=root)
    with TestClient(restarted) as client2:
        assert client2.get(f"/backups/{backup['id']}/verify").json()["verified"] is True
        assert client2.get("/backups").json()["items"][0]["id"] == backup["id"]
        restored = client2.post(
            f"/backups/{backup['id']}/restore", json={"target_name": "after.db"}
        )
        assert restored.status_code == 200
        assert restored.json()["max_id"] == 1
        append(client2, "job-a", "after-restart")


def test_docs_and_openapi_available(env):
    client = env["client"]
    assert client.get("/docs").status_code == 200
    schema = client.get("/openapi.json").json()
    for path in (
        "/logs",
        "/backups",
        "/backups/{backup_id}",
        "/backups/{backup_id}/restore",
        "/backups/{backup_id}/recover",
    ):
        assert path in schema["paths"]


# --------------------------------------------------------------------- recovery
def delete_rows(db, ids: list[int]) -> None:
    with db.writer() as conn:
        for row_id in ids:
            conn.execute("DELETE FROM job_logs WHERE id = ?", (row_id,))


def source_rows(db) -> list[dict]:
    with db.reader() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT id, job_id, message, created_at FROM job_logs ORDER BY id"
            ).fetchall()
        ]


def test_recover_deleted_job_rows_with_original_ids_and_timestamps(env):
    client = env["client"]
    db = env["db"]
    a1 = append(client, "job-a", "a-one")
    b1 = append(client, "job-b", "b-one")
    a2 = append(client, "job-a", "a-two")
    backup = make_backup(client)
    b2 = append(client, "job-b", "b-two")
    a3 = append(client, "job-a", "a-three")  # appended after the backup

    delete_rows(db, [a1["id"], a2["id"]])
    before = {row["id"]: row for row in source_rows(db)}

    response = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["backup_row_count"] == 2
    assert body["inserted_count"] == 2
    assert body["skipped_count"] == 0
    assert body["inserted_min_id"] == a1["id"]
    assert body["inserted_max_id"] == a2["id"]
    assert body["max_id"] == a3["id"]

    after = {row["id"]: row for row in source_rows(db)}
    # Original IDs and creation timestamps are restored exactly.
    assert after[a1["id"]] == a1
    assert after[a2["id"]] == a2
    # Rows appended after the backup and other jobs are untouched.
    assert after[a3["id"]] == a3
    assert after[b1["id"]] == b1
    assert after[b2["id"]] == b2
    assert set(after) == set(before) | {a1["id"], a2["id"]}

    # /logs pages by the original id, in id order.
    page = client.get("/logs", params={"job_id": "job-a", "limit": 2}).json()
    assert [item["id"] for item in page["items"]] == [a1["id"], a2["id"]]
    rest = client.get(
        "/logs", params={"job_id": "job-a", "after_id": page["next_after_id"]}
    ).json()
    assert [item["message"] for item in rest["items"]] == ["a-three"]

    # A backup taken after recovery contains the recovered rows.
    later = make_backup(client)
    assert client.get(f"/backups/{later['id']}/verify").json()["verified"] is True
    restored = client.post(
        f"/backups/{later['id']}/restore", json={"target_name": "after-recover.db"}
    )
    assert restored.status_code == 200, restored.text
    restored_rows = client.get("/restores/after-recover.db/logs").json()["items"]
    assert [(r["id"], r["message"]) for r in restored_rows] == [
        (a1["id"], "a-one"),
        (b1["id"], "b-one"),
        (a2["id"], "a-two"),
        (b2["id"], "b-two"),
        (a3["id"], "a-three"),
    ]

    # The already-published backup is unchanged and still restores.
    old = client.post(
        f"/backups/{backup['id']}/restore", json={"target_name": "old-backup.db"}
    )
    assert old.status_code == 200
    assert old.json()["row_count"] == 3
    assert client.get(f"/backups/{backup['id']}/verify").json()["verified"] is True


def test_recover_skips_identical_rows_and_is_idempotent(env):
    client = env["client"]
    db = env["db"]
    a1 = append(client, "job-a", "one")
    a2 = append(client, "job-a", "two")
    backup = make_backup(client)
    for _ in range(3):
        response = client.post(
            f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["inserted_count"] == 0
        assert body["skipped_count"] == 2
    assert [row["id"] for row in source_rows(db)] == [a1["id"], a2["id"]]


def test_recover_retry_after_partial_deletion_inserts_only_missing(env):
    client = env["client"]
    db = env["db"]
    a1 = append(client, "job-a", "one")
    a2 = append(client, "job-a", "two")
    backup = make_backup(client)
    delete_rows(db, [a2["id"]])

    body = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    ).json()
    assert body["inserted_count"] == 1
    assert body["skipped_count"] == 1
    assert body["inserted_min_id"] == a2["id"]

    again = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    ).json()
    assert again["inserted_count"] == 0
    assert [row["id"] for row in source_rows(db)] == [a1["id"], a2["id"]]


def test_recover_rejects_conflicting_content_without_writing(env):
    client = env["client"]
    db = env["db"]
    a1 = append(client, "job-a", "one")
    a2 = append(client, "job-a", "two")
    backup = make_backup(client)

    # id of a2 is gone; a *different* row now lives at a1's id.
    delete_rows(db, [a2["id"]])
    with db.writer() as conn:
        conn.execute("DELETE FROM job_logs WHERE id = ?", (a1["id"],))
        conn.execute(
            "INSERT INTO job_logs (id, job_id, message, created_at)"
            " VALUES (?, 'job-a', 'tampered', ?)",
            (a1["id"], a1["created_at"]),
        )
    before = source_rows(db)

    response = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    )
    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["conflicting_ids"] == [a1["id"]]
    assert detail["source_unchanged"] is True
    # Whole request rejected: the missing a2 row was not inserted either.
    assert source_rows(db) == before

    # A conflicting job_id at the same id is also a conflict.
    with db.writer() as conn:
        conn.execute("UPDATE job_logs SET job_id='job-z' WHERE id = ?", (a1["id"],))
    tampered = source_rows(db)
    response = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    )
    assert response.status_code == 409
    assert response.json()["detail"]["conflicting_ids"] == [a1["id"]]
    assert source_rows(db) == tampered


def test_recover_is_atomic_and_interleaves_safely_with_appends(env):
    """A recovery either inserts every missing row or none, even with writers."""
    client = env["client"]
    db = env["db"]
    manager = env["manager"]
    original = [append(client, "job-a", f"row-{i}") for i in range(10)]
    backup = make_backup(client)
    delete_rows(db, [row["id"] for row in original[3:7]])
    before_ids = [row["id"] for row in source_rows(db)]

    appended: list[int] = []
    errors: list[BaseException] = []

    def appender():
        try:
            while True:
                with db.writer() as conn:
                    cur = conn.execute(
                        "INSERT INTO job_logs(job_id, message) VALUES ('job-b', 'x')"
                    )
                    appended.append(int(cur.lastrowid))
                    if len(appended) >= 30:
                        break
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    thread = threading.Thread(target=appender)
    thread.start()
    try:
        result = manager.recover_job(backup["id"], "job-a")
    finally:
        thread.join()
    assert not errors, errors

    rows = {row["id"]: row for row in source_rows(db)}
    # Every missing row was restored, no ID was reused for an append, and no
    # append was lost or overwritten.
    assert result.inserted_count == 4
    assert {row["id"] for row in original}.issubset(rows)
    for row_id in original[3:7]:
        assert rows[row_id["id"]]["message"] == row_id["message"]
    assert set(appended).issubset(rows)
    assert len(appended) == 30
    assert len(rows) == len(before_ids) + 4 + 30


def test_append_waits_for_recovery_and_never_gets_a_recovered_id(env):
    """An append started mid-recovery blocks and commits after it, new id."""
    client = env["client"]
    db = env["db"]
    manager = env["manager"]
    rows = [append(client, "job-a", f"row-{i}") for i in range(4)]
    backup = make_backup(client)
    delete_rows(db, [rows[1]["id"], rows[2]["id"]])

    started = threading.Event()
    appended: dict = {}
    appender_thread: list[threading.Thread] = []

    def append_during_recovery():
        started.set()
        with db.writer() as conn:
            cur = conn.execute(
                "INSERT INTO job_logs(job_id, message) VALUES ('job-b', 'mid')"
            )
            appended["id"] = int(cur.lastrowid)

    def hook():
        thread = threading.Thread(target=append_during_recovery)
        appender_thread.append(thread)
        thread.start()
        assert started.wait(timeout=5)
        # While the recovery transaction holds the write lock the append
        # cannot commit, so no partially recovered state is observable.
        thread.join(timeout=0.3)
        assert thread.is_alive(), "append committed inside a recovery"
        mid = source_rows(db)
        assert [row["id"] for row in mid] == [rows[0]["id"], rows[3]["id"]]

    result = manager.recover_job(backup["id"], "job-a", after_validate=hook)
    assert result.inserted_count == 2

    thread = appender_thread[0]
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert "id" in appended
    all_ids = [row["id"] for row in source_rows(db)]
    assert sorted(all_ids) == sorted(
        [row["id"] for row in rows] + [appended["id"]]
    )
    # The append got a fresh id above every existing row, never a deleted one.
    assert appended["id"] > rows[-1]["id"]
    assert appended["id"] not in {row["id"] for row in rows[:3]}


def test_recover_failure_rolls_back_every_insert(env, monkeypatch):
    client = env["client"]
    db = env["db"]
    manager = env["manager"]
    a1 = append(client, "job-a", "one")
    a2 = append(client, "job-a", "two")
    backup = make_backup(client)
    delete_rows(db, [a1["id"], a2["id"]])
    before = source_rows(db)

    real_writer = db.writer

    class FaultyConnection:
        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def execute(self, sql, params=()):
            # Fail once the second recovered row is being inserted.
            if sql.startswith("INSERT INTO job_logs") and params[:1] == (a2["id"],):
                raise sqlite3.OperationalError("simulated fault")
            return self._conn.execute(sql, params)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

    from contextlib import contextmanager

    @contextmanager
    def faulty_writer():
        with real_writer() as conn:
            yield FaultyConnection(conn)

    monkeypatch.setattr(db, "writer", faulty_writer)
    with pytest.raises(sqlite3.OperationalError):
        manager.recover_job(backup["id"], "job-a")
    monkeypatch.undo()

    assert source_rows(db) == before
    # The service still works and a retry restores both rows exactly once.
    body = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    ).json()
    assert body["inserted_count"] == 2
    assert [row["id"] for row in source_rows(db)] == [a1["id"], a2["id"]]


def test_recover_tail_rows_does_not_cause_id_reuse(env):
    """Re-inserting the highest ids must not let a later append reuse an id."""
    client = env["client"]
    db = env["db"]
    rows = [append(client, "job-a", f"row-{i}") for i in range(6)]
    backup = make_backup(client)
    delete_rows(db, [rows[-1]["id"], rows[-2]["id"]])
    assert max(row["id"] for row in source_rows(db)) == rows[-3]["id"]

    body = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    ).json()
    assert body["inserted_count"] == 2
    assert body["max_id"] == rows[-1]["id"]

    fresh = append(client, "job-a", "after-recovery")
    assert fresh["id"] > rows[-1]["id"]
    assert fresh["id"] == rows[-1]["id"] + 1
    ids = [row["id"] for row in source_rows(db)]
    assert len(ids) == len(set(ids)) == 7


def test_recover_unknown_backup_or_job(env):
    client = env["client"]
    append(client, "job-a", "one")
    backup = make_backup(client)

    missing_backup = client.post(
        "/backups/bkp-20260101T000000Z-000000000000/recover",
        json={"job_id": "job-a"},
    )
    assert missing_backup.status_code == 404

    missing_job = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "nope"}
    )
    assert missing_job.status_code == 404
    assert client.get("/logs").json()["items"][0]["message"] == "one"


def test_recover_refuses_corrupt_backup(env):
    client = env["client"]
    db = env["db"]
    a1 = append(client, "job-a", "one")
    backup = make_backup(client)
    delete_rows(db, [a1["id"]])
    before = source_rows(db)

    db_path = env["root"] / "backups" / backup["id"] / "joblogs.db"
    db_path.chmod(0o644)
    db_path.write_bytes(db_path.read_bytes()[:4096])

    response = client.post(
        f"/backups/{backup['id']}/recover", json={"job_id": "job-a"}
    )
    assert response.status_code == 409, response.text
    assert source_rows(db) == before
    append(client, "job-a", "still-writable")

