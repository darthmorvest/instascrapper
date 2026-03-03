from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def using_postgres() -> bool:
    url = _database_url().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


def db_path() -> Path:
    if os.getenv("VERCEL") == "1":
        return Path("/tmp/instagram_scrubber.db")
    data_dir = Path.cwd() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "instagram_scrubber.db"


def is_ephemeral_storage() -> bool:
    return os.getenv("VERCEL") == "1" and not using_postgres()


def storage_mode_label() -> str:
    if using_postgres():
        return "managed-postgres (DATABASE_URL)"
    if os.getenv("VERCEL") == "1":
        return f"ephemeral ({db_path()})"
    return str(db_path())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect():
    if using_postgres():
        try:
            import psycopg
            from psycopg.rows import dict_row
        except Exception as err:  # noqa: BLE001
            raise RuntimeError(
                "DATABASE_URL is set, but Postgres driver is missing. Install psycopg[binary]."
            ) from err
        return psycopg.connect(_database_url(), row_factory=dict_row)

    conn = sqlite3.connect(db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _bind(sql: str) -> str:
    if using_postgres():
        return sql.replace("?", "%s")
    return sql


def _execute(conn, sql: str, params: tuple[Any, ...] = ()):
    return conn.execute(_bind(sql), params)


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if isinstance(row, sqlite3.Row):
        return {k: row[k] for k in row.keys()}
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}
    raise TypeError(f"Unsupported row type: {type(row)!r}")


def _has_column_sqlite(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def _has_column_postgres(conn, table: str, column: str) -> bool:
    row = _execute(
        conn,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        (table, column),
    ).fetchone()
    return row is not None


def _migrate_runs_table_sqlite(conn: sqlite3.Connection) -> None:
    additions = [
        ("phase", "TEXT NOT NULL DEFAULT 'queued'"),
        ("progress_message", "TEXT"),
        ("progress_current", "INTEGER NOT NULL DEFAULT 0"),
        ("progress_total", "INTEGER NOT NULL DEFAULT 0"),
        ("state_json", "TEXT"),
        ("preview_json", "TEXT"),
    ]
    for column, definition in additions:
        if _has_column_sqlite(conn, "runs", column):
            continue
        conn.execute(f"ALTER TABLE runs ADD COLUMN {column} {definition}")


def _migrate_runs_table_postgres(conn) -> None:
    additions = [
        ("phase", "TEXT NOT NULL DEFAULT 'queued'"),
        ("progress_message", "TEXT"),
        ("progress_current", "INTEGER NOT NULL DEFAULT 0"),
        ("progress_total", "INTEGER NOT NULL DEFAULT 0"),
        ("state_json", "TEXT"),
        ("preview_json", "TEXT"),
    ]
    for column, definition in additions:
        if _has_column_postgres(conn, "runs", column):
            continue
        _execute(conn, f"ALTER TABLE runs ADD COLUMN IF NOT EXISTS {column} {definition}")


def init_db() -> None:
    with _connect() as conn:
        if using_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS profiles (
                  id BIGSERIAL PRIMARY KEY,
                  name TEXT NOT NULL UNIQUE,
                  access_token TEXT NOT NULL,
                  business_account_id TEXT NOT NULL,
                  graph_version TEXT NOT NULL DEFAULT 'v21.0',
                  timeout_seconds INTEGER NOT NULL DEFAULT 25,
                  retry_count INTEGER NOT NULL DEFAULT 3,
                  retry_backoff_seconds DOUBLE PRECISION NOT NULL DEFAULT 1.5,
                  default_media_limit INTEGER NOT NULL DEFAULT 25,
                  default_comments_per_media INTEGER NOT NULL DEFAULT 200,
                  default_lookback_days INTEGER NOT NULL DEFAULT 90,
                  default_max_profiles INTEGER,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS runs (
                  id BIGSERIAL PRIMARY KEY,
                  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                  started_at TEXT NOT NULL,
                  completed_at TEXT,
                  status TEXT NOT NULL,
                  phase TEXT NOT NULL DEFAULT 'queued',
                  progress_message TEXT,
                  progress_current INTEGER NOT NULL DEFAULT 0,
                  progress_total INTEGER NOT NULL DEFAULT 0,
                  media_limit INTEGER NOT NULL,
                  comments_per_media INTEGER NOT NULL,
                  lookback_days INTEGER NOT NULL,
                  max_profiles INTEGER,
                  lead_count INTEGER,
                  output_filename TEXT,
                  error_message TEXT,
                  state_json TEXT,
                  preview_json TEXT
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS report_files (
                  id BIGSERIAL PRIMARY KEY,
                  run_id BIGINT NOT NULL UNIQUE REFERENCES runs(id) ON DELETE CASCADE,
                  output_filename TEXT NOT NULL UNIQUE,
                  csv_content TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """,
            )
            _migrate_runs_table_postgres(conn)
            return

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS profiles (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              access_token TEXT NOT NULL,
              business_account_id TEXT NOT NULL,
              graph_version TEXT NOT NULL DEFAULT 'v21.0',
              timeout_seconds INTEGER NOT NULL DEFAULT 25,
              retry_count INTEGER NOT NULL DEFAULT 3,
              retry_backoff_seconds REAL NOT NULL DEFAULT 1.5,
              default_media_limit INTEGER NOT NULL DEFAULT 25,
              default_comments_per_media INTEGER NOT NULL DEFAULT 200,
              default_lookback_days INTEGER NOT NULL DEFAULT 90,
              default_max_profiles INTEGER,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              profile_id INTEGER NOT NULL,
              started_at TEXT NOT NULL,
              completed_at TEXT,
              status TEXT NOT NULL,
              phase TEXT NOT NULL DEFAULT 'queued',
              progress_message TEXT,
              progress_current INTEGER NOT NULL DEFAULT 0,
              progress_total INTEGER NOT NULL DEFAULT 0,
              media_limit INTEGER NOT NULL,
              comments_per_media INTEGER NOT NULL,
              lookback_days INTEGER NOT NULL,
              max_profiles INTEGER,
              lead_count INTEGER,
              output_filename TEXT,
              error_message TEXT,
              state_json TEXT,
              preview_json TEXT,
              FOREIGN KEY(profile_id) REFERENCES profiles(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS report_files (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id INTEGER NOT NULL UNIQUE,
              output_filename TEXT NOT NULL UNIQUE,
              csv_content TEXT NOT NULL,
              created_at TEXT NOT NULL,
              FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            """
        )
        _migrate_runs_table_sqlite(conn)


def list_profiles() -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT
              id,
              name,
              business_account_id,
              graph_version,
              timeout_seconds,
              retry_count,
              retry_backoff_seconds,
              default_media_limit,
              default_comments_per_media,
              default_lookback_days,
              default_max_profiles,
              created_at,
              updated_at
            FROM profiles
            ORDER BY name ASC
            """,
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_profile(profile_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT
              id,
              name,
              access_token,
              business_account_id,
              graph_version,
              timeout_seconds,
              retry_count,
              retry_backoff_seconds,
              default_media_limit,
              default_comments_per_media,
              default_lookback_days,
              default_max_profiles,
              created_at,
              updated_at
            FROM profiles
            WHERE id = ?
            """,
            (profile_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_active_profile() -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT
              id,
              name,
              access_token,
              business_account_id,
              graph_version,
              timeout_seconds,
              retry_count,
              retry_backoff_seconds,
              default_media_limit,
              default_comments_per_media,
              default_lookback_days,
              default_max_profiles,
              created_at,
              updated_at
            FROM profiles
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def create_profile(
    *,
    name: str,
    access_token: str,
    business_account_id: str,
    graph_version: str,
    timeout_seconds: int,
    retry_count: int,
    retry_backoff_seconds: float,
    default_media_limit: int,
    default_comments_per_media: int,
    default_lookback_days: int,
    default_max_profiles: int | None,
) -> int:
    created_at = _utc_now_iso()
    values = (
        name.strip(),
        access_token.strip(),
        business_account_id.strip(),
        graph_version.strip() or "v21.0",
        timeout_seconds,
        retry_count,
        retry_backoff_seconds,
        default_media_limit,
        default_comments_per_media,
        default_lookback_days,
        default_max_profiles,
        created_at,
        created_at,
    )

    with _connect() as conn:
        if using_postgres():
            row = _execute(
                conn,
                """
                INSERT INTO profiles (
                  name,
                  access_token,
                  business_account_id,
                  graph_version,
                  timeout_seconds,
                  retry_count,
                  retry_backoff_seconds,
                  default_media_limit,
                  default_comments_per_media,
                  default_lookback_days,
                  default_max_profiles,
                  created_at,
                  updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                values,
            ).fetchone()
            if row is None:
                raise RuntimeError("Failed to create profile")
            return int(_row_to_dict(row)["id"])

        cursor = _execute(
            conn,
            """
            INSERT INTO profiles (
              name,
              access_token,
              business_account_id,
              graph_version,
              timeout_seconds,
              retry_count,
              retry_backoff_seconds,
              default_media_limit,
              default_comments_per_media,
              default_lookback_days,
              default_max_profiles,
              created_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        return int(cursor.lastrowid)


def update_profile(
    *,
    profile_id: int,
    name: str,
    access_token: str,
    business_account_id: str,
    graph_version: str,
    timeout_seconds: int,
    retry_count: int,
    retry_backoff_seconds: float,
    default_media_limit: int,
    default_comments_per_media: int,
    default_lookback_days: int,
    default_max_profiles: int | None,
) -> None:
    with _connect() as conn:
        _execute(
            conn,
            """
            UPDATE profiles
            SET
              name = ?,
              access_token = ?,
              business_account_id = ?,
              graph_version = ?,
              timeout_seconds = ?,
              retry_count = ?,
              retry_backoff_seconds = ?,
              default_media_limit = ?,
              default_comments_per_media = ?,
              default_lookback_days = ?,
              default_max_profiles = ?,
              updated_at = ?
            WHERE id = ?
            """,
            (
                name.strip(),
                access_token.strip(),
                business_account_id.strip(),
                graph_version.strip() or "v21.0",
                timeout_seconds,
                retry_count,
                retry_backoff_seconds,
                default_media_limit,
                default_comments_per_media,
                default_lookback_days,
                default_max_profiles,
                _utc_now_iso(),
                profile_id,
            ),
        )


def upsert_active_profile(
    *,
    name: str,
    access_token: str,
    business_account_id: str,
    graph_version: str,
    timeout_seconds: int,
    retry_count: int,
    retry_backoff_seconds: float,
    default_media_limit: int,
    default_comments_per_media: int,
    default_lookback_days: int,
    default_max_profiles: int | None,
) -> int:
    active = get_active_profile()
    if active is None:
        return create_profile(
            name=name,
            access_token=access_token,
            business_account_id=business_account_id,
            graph_version=graph_version,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            retry_backoff_seconds=retry_backoff_seconds,
            default_media_limit=default_media_limit,
            default_comments_per_media=default_comments_per_media,
            default_lookback_days=default_lookback_days,
            default_max_profiles=default_max_profiles,
        )

    update_profile(
        profile_id=int(active["id"]),
        name=name,
        access_token=access_token,
        business_account_id=business_account_id,
        graph_version=graph_version,
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
        retry_backoff_seconds=retry_backoff_seconds,
        default_media_limit=default_media_limit,
        default_comments_per_media=default_comments_per_media,
        default_lookback_days=default_lookback_days,
        default_max_profiles=default_max_profiles,
    )
    return int(active["id"])


def delete_profile(profile_id: int) -> None:
    with _connect() as conn:
        _execute(conn, "DELETE FROM profiles WHERE id = ?", (profile_id,))


def create_run(
    *,
    profile_id: int,
    media_limit: int,
    comments_per_media: int,
    lookback_days: int,
    max_profiles: int | None,
) -> int:
    values = (
        profile_id,
        _utc_now_iso(),
        media_limit,
        comments_per_media,
        lookback_days,
        max_profiles,
    )
    with _connect() as conn:
        if using_postgres():
            row = _execute(
                conn,
                """
                INSERT INTO runs (
                  profile_id,
                  started_at,
                  status,
                  phase,
                  progress_message,
                  progress_current,
                  progress_total,
                  media_limit,
                  comments_per_media,
                  lookback_days,
                  max_profiles,
                  state_json,
                  preview_json
                )
                VALUES (?, ?, 'queued', 'queued', 'Queued', 0, 0, ?, ?, ?, ?, '{}', '[]')
                RETURNING id
                """,
                values,
            ).fetchone()
            if row is None:
                raise RuntimeError("Failed to create run")
            return int(_row_to_dict(row)["id"])

        cursor = _execute(
            conn,
            """
            INSERT INTO runs (
              profile_id,
              started_at,
              status,
              phase,
              progress_message,
              progress_current,
              progress_total,
              media_limit,
              comments_per_media,
              lookback_days,
              max_profiles,
              state_json,
              preview_json
            )
            VALUES (?, ?, 'queued', 'queued', 'Queued', 0, 0, ?, ?, ?, ?, '{}', '[]')
            """,
            values,
        )
        return int(cursor.lastrowid)


def get_run(run_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT
              runs.id,
              runs.profile_id,
              profiles.name AS profile_name,
              runs.started_at,
              runs.completed_at,
              runs.status,
              runs.phase,
              runs.progress_message,
              runs.progress_current,
              runs.progress_total,
              runs.media_limit,
              runs.comments_per_media,
              runs.lookback_days,
              runs.max_profiles,
              runs.lead_count,
              runs.output_filename,
              runs.error_message,
              runs.state_json,
              runs.preview_json
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            WHERE runs.id = ?
            """,
            (run_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def update_run_progress(
    run_id: int,
    *,
    status: str | None = None,
    phase: str | None = None,
    progress_message: str | None = None,
    progress_current: int | None = None,
    progress_total: int | None = None,
    state_json: str | None = None,
    preview_json: str | None = None,
) -> None:
    updates: list[str] = []
    values: list[Any] = []

    if status is not None:
        updates.append("status = ?")
        values.append(status)
    if phase is not None:
        updates.append("phase = ?")
        values.append(phase)
    if progress_message is not None:
        updates.append("progress_message = ?")
        values.append(progress_message[:1000])
    if progress_current is not None:
        updates.append("progress_current = ?")
        values.append(max(0, int(progress_current)))
    if progress_total is not None:
        updates.append("progress_total = ?")
        values.append(max(0, int(progress_total)))
    if state_json is not None:
        updates.append("state_json = ?")
        values.append(state_json)
    if preview_json is not None:
        updates.append("preview_json = ?")
        values.append(preview_json)

    if not updates:
        return

    values.append(run_id)
    with _connect() as conn:
        _execute(
            conn,
            f"""
            UPDATE runs
            SET {", ".join(updates)}
            WHERE id = ?
            """,
            tuple(values),
        )


def finish_run_success(
    run_id: int,
    lead_count: int,
    output_filename: str,
    *,
    preview_json: str | None = None,
) -> None:
    with _connect() as conn:
        _execute(
            conn,
            """
            UPDATE runs
            SET
              status = 'success',
              phase = 'completed',
              progress_message = 'Completed',
              progress_current = CASE WHEN progress_total > 0 THEN progress_total ELSE progress_current END,
              lead_count = ?,
              output_filename = ?,
              state_json = '{}',
              preview_json = COALESCE(?, preview_json),
              completed_at = ?
            WHERE id = ?
            """,
            (lead_count, output_filename, preview_json, _utc_now_iso(), run_id),
        )


def finish_run_failure(run_id: int, error_message: str) -> None:
    with _connect() as conn:
        _execute(
            conn,
            """
            UPDATE runs
            SET
              status = 'failed',
              phase = 'failed',
              progress_message = ?,
              error_message = ?,
              state_json = '{}',
              completed_at = ?
            WHERE id = ?
            """,
            (error_message[:500], error_message[:2000], _utc_now_iso(), run_id),
        )


def save_report_file(*, run_id: int, output_filename: str, csv_content: str) -> None:
    created_at = _utc_now_iso()
    with _connect() as conn:
        _execute(
            conn,
            """
            INSERT INTO report_files (
              run_id,
              output_filename,
              csv_content,
              created_at
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(output_filename)
            DO UPDATE SET
              run_id = excluded.run_id,
              csv_content = excluded.csv_content,
              created_at = excluded.created_at
            """,
            (run_id, output_filename, csv_content, created_at),
        )


def get_report_file(output_filename: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT
              run_id,
              output_filename,
              csv_content,
              created_at
            FROM report_files
            WHERE output_filename = ?
            LIMIT 1
            """,
            (output_filename,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT
              runs.id,
              runs.profile_id,
              profiles.name AS profile_name,
              runs.started_at,
              runs.completed_at,
              runs.status,
              runs.phase,
              runs.progress_message,
              runs.progress_current,
              runs.progress_total,
              runs.media_limit,
              runs.comments_per_media,
              runs.lookback_days,
              runs.max_profiles,
              runs.lead_count,
              runs.output_filename,
              runs.error_message,
              runs.state_json,
              runs.preview_json
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            ORDER BY runs.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]
