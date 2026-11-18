# Job Log Service

FastAPI + SQLite service for an append-only job log with crash-safe backup and
verified restore.  All key flows are usable from the interactive docs at
`/docs`.

## Run

```bash
docker compose up --build
```

The API listens on `http://localhost:8000` (`JOBLOG_PORT` overrides the port),
docs at `http://localhost:8000/docs`.  State lives in the `joblog-data` volume:
source database `/data/joblogs/source.db`, backups `/data/backups`, restored
targets `/data/restores`.

Without Docker (Python 3.11+):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
JOBLOG_DATA_DIR=./data uvicorn joblog.main:app --port 8000
```

## API

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `POST` | `/logs` | Append a log entry; returns only after the commit is durable |
| `GET`  | `/logs` | Query committed logs (`job_id`, `after_id`, `limit`) |
| `POST` | `/backups` | Create, verify and publish a backup; returns its backup id |
| `GET`  | `/backups` | List published backups |
| `GET`  | `/backups/{id}` | Get one published backup (id, SHA-256s, snapshot row count) |
| `GET`  | `/backups/{id}/verify` | Re-check manifest, SHA-256 and SQLite integrity |
| `GET`  | `/backups/{id}/download` | Download the verified `.db` file |
| `POST` | `/backups/{id}/restore` | Restore into an independent target (`target_name`, `overwrite`) |
| `POST` | `/backups/{id}/recover` | Copy a deleted job's rows from the backup back into the source (`job_id`) |
| `GET`  | `/restores/{target_name}/logs` | Read logs from a restored target (read-only) |
| `GET`  | `/health` | Liveness/readiness |

### Typical flow (`/docs` order)

1. `POST /logs` a few entries.
2. `POST /backups` — the response contains the published `id`, the database
   SHA-256 and the manifest SHA-256.
3. `POST /logs` more entries (they must not appear in the backup).
4. `GET /backups/{id}/verify` — confirms the manifest still matches.
5. `POST /backups/{id}/restore` with `{"target_name": "incident-2026.db"}`.
6. `GET /restores/incident-2026.db/logs` — shows the logs committed before the
   backup and none appended afterwards.

### Recovering deleted rows into the source

`POST /backups/{id}/recover` with `{"job_id": "..."}` copies that job's rows
from a published backup back into the live source database, keeping their
original `id` and `created_at`:

1. The backup is fully re-verified (manifest hash, database hash,
   `integrity_check`) before the source is opened for writing.
2. In one `BEGIN IMMEDIATE` transaction every backup row of the job is compared
   by id with the source:
   * id missing → re-inserted with the original `id` and `created_at`;
   * id present with identical `(job_id, message, created_at)` → skipped;
   * id present with different content → the whole request is rejected with
     `409` and nothing is written.
3. The response reports `inserted_count`, `skipped_count` and the resulting
   source row count. Retrying the same request inserts nothing.

Rows appended after the backup and all other jobs are untouched. Recovered rows
are ordinary source rows, so `GET /logs` pages them by their original id and any
later `POST /backups` includes them; already-published backups and their
independent restores are unaffected.

## Guarantees and how they are achieved

* **Consistent snapshot under concurrent appends.**  A backup opens a
  read-only connection, starts a read transaction (`BEGIN` + a `SELECT`, which
  pins one committed WAL snapshot) and copies with SQLite's online-backup API.
  Writers keep appending on `BEGIN IMMEDIATE` transactions; rows committed
  after the snapshot are excluded, rows committed before are included.  The
  copy's `COUNT(*)`/`MAX(id)` are cross-checked against the frozen snapshot.
* **Backup id published only after verification.**  The copy is staged under
  `backups/.staging/`, checked with `PRAGMA integrity_check`, a
  `manifest.json` with SHA-256 digests is written, and the whole directory is
  re-verified from disk.  Only then is it atomically renamed to
  `backups/<id>` and the database row flipped to `ready`.  The API only ever
  exposes `ready` backups.
* **Interruption / restart safety.**  A backup row is inserted as `staging`
  before any copying.  Startup runs `recover_incomplete()`: staging directories
  are deleted, non-`ready` rows become `failed`, and `ready` rows whose files
  vanished are demoted to `failed`.  A partial or interrupted backup can never
  be listed, downloaded or restored.
* **Published backups are immutable.**  Backup files are made read-only
  (`0444`), stored outside the source database, and never touched by later
  appends.  The source is a normal WAL database and stays writable.
* **Verified restore.**  `POST /backups/{id}/restore` re-verifies the manifest
  hash, the database hash and `integrity_check` *before* touching the target.
  The restored copy is prepared and hashed in a private temp directory, then
  renamed into place.  An existing target (when `overwrite=true`) is moved
  aside first and put back on any failure, so the previous target survives
  bit-for-bit.
* **Independent target.**  Restore writes a separate database file under
  `/data/restores`; later appends to the source and later writes to the target
  are isolated from each other.  The restore endpoint never opens the source
  for writing.
* **Atomic, idempotent row recovery.**  Validating every backup row against the
  source and re-inserting the missing ones happen inside a single
  `BEGIN IMMEDIATE` transaction, the same lock appends take.  A concurrent
  append therefore either finishes before the validation (and its id is seen)
  or waits until the recovery commits (and gets a new id via `AUTOINCREMENT`),
  never a recovered id.  A conflict or any error rolls the whole transaction
  back, so the source is never left partially recovered, and re-running the
  request skips rows that are already identical.

## Tests

```bash
pip install -r requirements.txt pytest httpx
python -m pytest
```

The suite covers concurrent appends during a backup, frozen-snapshot
behaviour, immutable published backups, independent restore, corrupt-backup
restore with the target preserved, interrupted backups after restart,
`ready`-row demotion, backup failure, restart persistence, `/docs`, and row
recovery (restored ids/timestamps, post-backup rows and other jobs untouched,
idempotent retry, conflict rejection with nothing written, rollback on a
mid-recovery fault, and appends interleaved with a recovery).

## Notes / limits

* The service is a single process.  `recover_incomplete()` assumes it is the
  only writer to the backup directory at startup; do not run several API
  instances against the same `/data` volume.
* Backup files are hash-checked on every verify/download/restore, which is
  O(backup size).  Fine for job-log volumes; for very large databases this
  should be cached.
* Restore takes an explicit `target_name` (a filename under `/data/restores`);
  absolute host paths are deliberately not accepted through the API.
