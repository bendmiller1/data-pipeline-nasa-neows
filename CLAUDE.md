# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Setup**
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Run the pipeline**
```bash
# Demo mode (offline, uses sample_data/feed_sample.json; supports dates 2025-01-01 to 2025-10-31)
python -m src.pipeline --mode feed --start 2025-10-01 --end 2025-10-07 --demo

# Live mode (real NASA API)
python -m src.pipeline --mode feed --start 2025-10-01 --end 2025-10-07 --live

# Run migrations
python -m src.pipeline --mode migrate
python -m src.pipeline --mode migrate --dry-run
python -m src.pipeline --mode migrate --rollback --target 001
```

**Run tests**
```bash
# All tests
python -m pytest tests/ -v

# Single test file
python -m pytest tests/test_transform.py -v

# Single test
python -m pytest tests/test_transform.py::test_function_name -v

# Skip PostgreSQL tests (when Postgres is unavailable)
python -m pytest tests/ -v -m "not postgres_only"
```

**Run individual modules manually**
```bash
python -m src.fetch      # Test fetch stage in isolation
python -m src.transform  # Test transform + CSV output
python -m src.load       # Test load stage (reads from data/processed/neows_latest.csv)
python -m src.config     # Print current configuration values
```

## Architecture

The pipeline is a classic ETL with three stages wired by `src/pipeline.py`:

1. **Extract** (`src/fetch.py`) — Either loads `sample_data/feed_sample.json` (demo) or calls the NASA NeoWs `/feed` endpoint with exponential backoff retry logic. Mode is read dynamically from the `DEMO_MODE` env var at call time, not at import time.

2. **Transform** (`src/transform.py`) — Flattens the nested JSON structure (`near_earth_objects[date][asteroid][close_approach_data]`) into one row per close-approach event. Produces a 10-column DataFrame and writes it to `data/processed/neows_latest.csv`.

3. **Load** (`src/load.py`) — Inserts the DataFrame into the database using SQLAlchemy. Idempotency is achieved by deleting the target date range before inserting. The `DatabaseManager` class abstracts over SQLite and PostgreSQL — it detects the backend from the URL and configures engine settings accordingly (connection pooling, thread safety, schema DDL).

`src/pipeline.py` also runs auto-migration before the ETL: it checks for pending migrations and applies them automatically before the feed runs.

## Configuration

All config is in `src/config.py`, loaded from `.env` (see `.env.example`). Key variables:

| Variable | Effect |
|---|---|
| `DEMO_MODE=1` | Use local sample data instead of the NASA API |
| `NASA_API_KEY` | Defaults to `DEMO_KEY` (NASA's free public key) |
| `USE_POSTGRES=true` | Switch from SQLite to PostgreSQL |
| `POSTGRES_*` | Connection settings when `USE_POSTGRES=true` |

`--demo` and `--live` CLI flags override `.env` at runtime via `src/utils/mode_toggle.py`.

## Database

- **SQLite** (default): `data/warehouse/neows_data.db`
- **PostgreSQL** (optional): configured via env vars

The `neows` table has a composite primary key `(close_approach_date, id)`. The schemas differ between backends — SQLite uses `TEXT`/`INTEGER`/`REAL`; PostgreSQL uses `DATE`/`BOOLEAN`/`VARCHAR`. The correct DDL is selected by `DatabaseManager.get_schema_sql()`.

Migrations live in `src/migrations/` as `NNN_description.py` files. Each must subclass `BaseMigration` (from `src/migration_manager.py`) and implement `up()`, `down()`, and `description`. Applied versions are tracked in the `schema_migrations` table.

## Testing

`tests/conftest.py` defines key shared fixtures:
- `sqlite_test_db` — session-scoped temp SQLite DB
- `postgres_test_db` — session-scoped PostgreSQL DB; auto-skipped if unavailable
- `dual_database` — parameterized fixture that runs tests against both backends
- `sample_neo_data` — 5 hardcoded NEO records for insertion tests

Markers: `sqlite_only`, `postgres_only`, `performance`, `integration`.

PostgreSQL tests skip gracefully when no Postgres server is reachable — they are not expected to pass in all environments.
