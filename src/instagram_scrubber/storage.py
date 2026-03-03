from __future__ import annotations

import json
import os
import secrets
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


def _new_token() -> str:
    return secrets.token_urlsafe(24)


def _connect():
    if using_postgres():
        try:
            import psycopg
            from psycopg.rows import dict_row
        except Exception as err:  # noqa: BLE001
            raise RuntimeError(
                "DATABASE_URL is set, but Postgres driver is missing. Install psycopg[binary]."
            ) from err
        # Supabase transaction poolers (PgBouncer) can error on prepared statements.
        # Disable automatic prepare to avoid DuplicatePreparedStatement failures.
        return psycopg.connect(
            _database_url(),
            row_factory=dict_row,
            prepare_threshold=None,
        )

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
        ("workspace_id", "INTEGER NOT NULL DEFAULT 1"),
        ("phase", "TEXT NOT NULL DEFAULT 'queued'"),
        ("progress_message", "TEXT"),
        ("progress_current", "INTEGER NOT NULL DEFAULT 0"),
        ("progress_total", "INTEGER NOT NULL DEFAULT 0"),
        ("state_json", "TEXT"),
        ("preview_json", "TEXT"),
        ("selected_media_ids_json", "TEXT NOT NULL DEFAULT '[]'"),
    ]
    for column, definition in additions:
        if _has_column_sqlite(conn, "runs", column):
            continue
        conn.execute(f"ALTER TABLE runs ADD COLUMN {column} {definition}")


def _migrate_runs_table_postgres(conn) -> None:
    additions = [
        ("workspace_id", "BIGINT NOT NULL DEFAULT 1"),
        ("phase", "TEXT NOT NULL DEFAULT 'queued'"),
        ("progress_message", "TEXT"),
        ("progress_current", "INTEGER NOT NULL DEFAULT 0"),
        ("progress_total", "INTEGER NOT NULL DEFAULT 0"),
        ("state_json", "TEXT"),
        ("preview_json", "TEXT"),
        ("selected_media_ids_json", "TEXT NOT NULL DEFAULT '[]'"),
    ]
    for column, definition in additions:
        if _has_column_postgres(conn, "runs", column):
            continue
        _execute(conn, f"ALTER TABLE runs ADD COLUMN IF NOT EXISTS {column} {definition}")


def _migrate_profiles_table_sqlite(conn: sqlite3.Connection) -> None:
    additions = [
        ("workspace_id", "INTEGER NOT NULL DEFAULT 1"),
        ("team_member_ids_json", "TEXT NOT NULL DEFAULT '[]'"),
    ]
    for column, definition in additions:
        if _has_column_sqlite(conn, "profiles", column):
            continue
        conn.execute(f"ALTER TABLE profiles ADD COLUMN {column} {definition}")


def _migrate_profiles_table_postgres(conn) -> None:
    additions = [
        ("workspace_id", "BIGINT NOT NULL DEFAULT 1"),
        ("team_member_ids_json", "TEXT NOT NULL DEFAULT '[]'"),
    ]
    for column, definition in additions:
        if _has_column_postgres(conn, "profiles", column):
            continue
        _execute(conn, f"ALTER TABLE profiles ADD COLUMN IF NOT EXISTS {column} {definition}")


def _bootstrap_workspace_defaults(conn) -> None:
    row = _execute(conn, "SELECT id FROM workspaces ORDER BY id ASC LIMIT 1").fetchone()
    if row is None:
        _execute(
            conn,
            """
            INSERT INTO workspaces (name, created_at, updated_at)
            VALUES (?, ?, ?)
            """,
            ("Default Workspace", _utc_now_iso(), _utc_now_iso()),
        )


def init_db() -> None:
    with _connect() as conn:
        if using_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS users (
                  id BIGSERIAL PRIMARY KEY,
                  email TEXT NOT NULL UNIQUE,
                  password_hash TEXT NOT NULL,
                  full_name TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS workspaces (
                  id BIGSERIAL PRIMARY KEY,
                  name TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS workspace_members (
                  id BIGSERIAL PRIMARY KEY,
                  workspace_id BIGINT NOT NULL,
                  user_id BIGINT NOT NULL,
                  role TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  UNIQUE(workspace_id, user_id)
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS workspace_invites (
                  id BIGSERIAL PRIMARY KEY,
                  workspace_id BIGINT NOT NULL,
                  email TEXT NOT NULL,
                  role TEXT NOT NULL,
                  token TEXT NOT NULL UNIQUE,
                  invited_by_user_id BIGINT,
                  expires_at TEXT NOT NULL,
                  accepted_at TEXT,
                  created_at TEXT NOT NULL
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS profiles (
                  id BIGSERIAL PRIMARY KEY,
                  workspace_id BIGINT NOT NULL DEFAULT 1,
                  name TEXT NOT NULL UNIQUE,
                  team_member_ids_json TEXT NOT NULL DEFAULT '[]',
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
                  workspace_id BIGINT NOT NULL DEFAULT 1,
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
                  preview_json TEXT,
                  selected_media_ids_json TEXT NOT NULL DEFAULT '[]'
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
            _migrate_profiles_table_postgres(conn)
            _migrate_runs_table_postgres(conn)
            _bootstrap_workspace_defaults(conn)
            return

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              full_name TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workspaces (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workspace_members (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              role TEXT NOT NULL,
              created_at TEXT NOT NULL,
              UNIQUE(workspace_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS workspace_invites (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id INTEGER NOT NULL,
              email TEXT NOT NULL,
              role TEXT NOT NULL,
              token TEXT NOT NULL UNIQUE,
              invited_by_user_id INTEGER,
              expires_at TEXT NOT NULL,
              accepted_at TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS profiles (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id INTEGER NOT NULL DEFAULT 1,
              name TEXT NOT NULL UNIQUE,
              team_member_ids_json TEXT NOT NULL DEFAULT '[]',
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
              workspace_id INTEGER NOT NULL DEFAULT 1,
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
              selected_media_ids_json TEXT NOT NULL DEFAULT '[]',
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
        _migrate_profiles_table_sqlite(conn)
        _migrate_runs_table_sqlite(conn)
        _bootstrap_workspace_defaults(conn)


def list_profiles(workspace_id: int | None = None) -> list[dict[str, Any]]:
    where = ""
    params: list[Any] = []
    if workspace_id is not None:
        where = "WHERE workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        rows = _execute(
            conn,
            f"""
            SELECT
              id,
              workspace_id,
              name,
              team_member_ids_json,
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
            {where}
            ORDER BY name ASC
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_profile(profile_id: int, workspace_id: int | None = None) -> dict[str, Any] | None:
    where_workspace = ""
    params: list[Any] = [profile_id]
    if workspace_id is not None:
        where_workspace = "AND workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        row = _execute(
            conn,
            f"""
            SELECT
              id,
              workspace_id,
              name,
              team_member_ids_json,
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
            {where_workspace}
            """,
            tuple(params),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_active_profile(workspace_id: int | None = None) -> dict[str, Any] | None:
    where = ""
    params: list[Any] = []
    if workspace_id is not None:
        where = "WHERE workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        row = _execute(
            conn,
            f"""
            SELECT
              id,
              workspace_id,
              name,
              team_member_ids_json,
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
            {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def create_profile(
    *,
    workspace_id: int = 1,
    name: str,
    team_member_user_ids: list[int] | None = None,
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
    team_member_ids_json = json.dumps(sorted({int(user_id) for user_id in (team_member_user_ids or [])}), ensure_ascii=True)
    values = (
        int(workspace_id),
        name.strip(),
        team_member_ids_json,
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
                  workspace_id,
                  name,
                  team_member_ids_json,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              workspace_id,
              name,
              team_member_ids_json,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        return int(cursor.lastrowid)


def update_profile(
    *,
    workspace_id: int | None = None,
    profile_id: int,
    name: str,
    team_member_user_ids: list[int] | None = None,
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
    team_member_ids_json = json.dumps(sorted({int(user_id) for user_id in (team_member_user_ids or [])}), ensure_ascii=True)
    where_workspace = ""
    params: list[Any] = [
        name.strip(),
        team_member_ids_json,
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
    ]
    if workspace_id is not None:
        where_workspace = "AND workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        _execute(
            conn,
            f"""
            UPDATE profiles
            SET
              name = ?,
              team_member_ids_json = ?,
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
            {where_workspace}
            """,
            tuple(params),
        )


def upsert_active_profile(
    *,
    workspace_id: int = 1,
    name: str,
    team_member_user_ids: list[int] | None = None,
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
    active = get_active_profile(workspace_id=workspace_id)
    if active is None:
        return create_profile(
            workspace_id=workspace_id,
            name=name,
            team_member_user_ids=team_member_user_ids,
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
        workspace_id=workspace_id,
        profile_id=int(active["id"]),
        name=name,
        team_member_user_ids=team_member_user_ids,
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


def delete_profile(profile_id: int, workspace_id: int | None = None) -> None:
    where_workspace = ""
    params: list[Any] = [profile_id]
    if workspace_id is not None:
        where_workspace = "AND workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        _execute(conn, f"DELETE FROM profiles WHERE id = ? {where_workspace}", tuple(params))


def create_run(
    *,
    workspace_id: int = 1,
    profile_id: int,
    media_limit: int,
    comments_per_media: int,
    lookback_days: int,
    max_profiles: int | None,
    selected_media_ids: list[str] | None = None,
) -> int:
    if selected_media_ids:
        deduped = []
        seen: set[str] = set()
        for media_id in selected_media_ids:
            candidate = str(media_id).strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        selected_media_ids_json = json.dumps(deduped, ensure_ascii=True)
    else:
        selected_media_ids_json = "[]"

    values = (
        int(workspace_id),
        profile_id,
        _utc_now_iso(),
        media_limit,
        comments_per_media,
        lookback_days,
        max_profiles,
        selected_media_ids_json,
    )
    with _connect() as conn:
        if using_postgres():
            row = _execute(
                conn,
                """
                INSERT INTO runs (
                  workspace_id,
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
                  preview_json,
                  selected_media_ids_json
                )
                VALUES (?, ?, ?, 'queued', 'queued', 'Queued', 0, 0, ?, ?, ?, ?, '{}', '[]', ?)
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
              workspace_id,
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
              preview_json,
              selected_media_ids_json
            )
            VALUES (?, ?, ?, 'queued', 'queued', 'Queued', 0, 0, ?, ?, ?, ?, '{}', '[]', ?)
            """,
            values,
        )
        return int(cursor.lastrowid)


def get_run(run_id: int, workspace_id: int | None = None) -> dict[str, Any] | None:
    where_workspace = ""
    params: list[Any] = [run_id]
    if workspace_id is not None:
        where_workspace = "AND runs.workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        row = _execute(
            conn,
            f"""
            SELECT
              runs.id,
              runs.workspace_id,
              runs.profile_id,
              profiles.name AS profile_name,
              profiles.team_member_ids_json AS profile_team_member_ids_json,
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
              runs.preview_json,
              runs.selected_media_ids_json
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            WHERE runs.id = ?
            {where_workspace}
            """,
            tuple(params),
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


def get_run_by_output_filename(
    output_filename: str,
    *,
    workspace_id: int | None = None,
) -> dict[str, Any] | None:
    where_workspace = ""
    params: list[Any] = [output_filename]
    if workspace_id is not None:
        where_workspace = "AND runs.workspace_id = ?"
        params.append(int(workspace_id))

    with _connect() as conn:
        row = _execute(
            conn,
            f"""
            SELECT
              runs.id,
              runs.workspace_id,
              runs.profile_id,
              profiles.name AS profile_name,
              profiles.team_member_ids_json AS profile_team_member_ids_json,
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
              runs.preview_json,
              runs.selected_media_ids_json
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            WHERE runs.output_filename = ?
            {where_workspace}
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def list_runs(limit: int = 50, workspace_id: int | None = None) -> list[dict[str, Any]]:
    where = ""
    params: list[Any] = []
    if workspace_id is not None:
        where = "WHERE runs.workspace_id = ?"
        params.append(int(workspace_id))
    params.append(limit)

    with _connect() as conn:
        rows = _execute(
            conn,
            f"""
            SELECT
              runs.id,
              runs.workspace_id,
              runs.profile_id,
              profiles.name AS profile_name,
              profiles.team_member_ids_json AS profile_team_member_ids_json,
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
              runs.preview_json,
              runs.selected_media_ids_json
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            {where}
            ORDER BY runs.id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def count_users() -> int:
    with _connect() as conn:
        row = _execute(conn, "SELECT COUNT(*) AS c FROM users").fetchone()
    if row is None:
        return 0
    return int(_row_to_dict(row).get("c", 0))


def get_workspace(workspace_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT id, name, created_at, updated_at
            FROM workspaces
            WHERE id = ?
            LIMIT 1
            """,
            (int(workspace_id),),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def add_workspace_member(*, workspace_id: int, user_id: int, role: str) -> None:
    with _connect() as conn:
        _execute(
            conn,
            """
            INSERT INTO workspace_members (workspace_id, user_id, role, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(workspace_id, user_id) DO UPDATE SET role = excluded.role
            """,
            (
                int(workspace_id),
                int(user_id),
                role.strip().lower() or "member",
                _utc_now_iso(),
            ),
        )


def get_user_by_email(email: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT id, email, password_hash, full_name, created_at, updated_at
            FROM users
            WHERE lower(email) = lower(?)
            LIMIT 1
            """,
            (email.strip(),),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_user(user_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT id, email, password_hash, full_name, created_at, updated_at
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def create_user(*, email: str, password_hash: str, full_name: str) -> int:
    now = _utc_now_iso()
    values = (email.strip().lower(), password_hash, full_name.strip(), now, now)
    with _connect() as conn:
        if using_postgres():
            row = _execute(
                conn,
                """
                INSERT INTO users (email, password_hash, full_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
                """,
                values,
            ).fetchone()
            if row is None:
                raise RuntimeError("Failed to create user")
            return int(_row_to_dict(row)["id"])
        cursor = _execute(
            conn,
            """
            INSERT INTO users (email, password_hash, full_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            values,
        )
        return int(cursor.lastrowid)


def create_workspace(*, name: str, owner_user_id: int) -> int:
    now = _utc_now_iso()
    with _connect() as conn:
        if using_postgres():
            row = _execute(
                conn,
                """
                INSERT INTO workspaces (name, created_at, updated_at)
                VALUES (?, ?, ?)
                RETURNING id
                """,
                (name.strip(), now, now),
            ).fetchone()
            if row is None:
                raise RuntimeError("Failed to create workspace")
            workspace_id = int(_row_to_dict(row)["id"])
        else:
            cursor = _execute(
                conn,
                """
                INSERT INTO workspaces (name, created_at, updated_at)
                VALUES (?, ?, ?)
                """,
                (name.strip(), now, now),
            )
            workspace_id = int(cursor.lastrowid)

        _execute(
            conn,
            """
            INSERT INTO workspace_members (workspace_id, user_id, role, created_at)
            VALUES (?, ?, 'owner', ?)
            ON CONFLICT(workspace_id, user_id) DO NOTHING
            """,
            (workspace_id, int(owner_user_id), now),
        )
        return workspace_id


def list_user_workspaces(user_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT
              w.id,
              w.name,
              wm.role,
              w.created_at,
              w.updated_at
            FROM workspaces w
            JOIN workspace_members wm ON wm.workspace_id = w.id
            WHERE wm.user_id = ?
            ORDER BY w.name ASC
            """,
            (int(user_id),),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_workspace_membership(workspace_id: int, user_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT workspace_id, user_id, role, created_at
            FROM workspace_members
            WHERE workspace_id = ? AND user_id = ?
            LIMIT 1
            """,
            (int(workspace_id), int(user_id)),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def list_workspace_members(workspace_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT
              wm.workspace_id,
              wm.user_id,
              wm.role,
              wm.created_at,
              u.email,
              u.full_name
            FROM workspace_members wm
            JOIN users u ON u.id = wm.user_id
            WHERE wm.workspace_id = ?
            ORDER BY
              CASE wm.role
                WHEN 'owner' THEN 1
                WHEN 'admin' THEN 2
                WHEN 'member' THEN 3
                ELSE 4
              END,
              u.full_name ASC
            """,
            (int(workspace_id),),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def create_workspace_invite(
    *,
    workspace_id: int,
    email: str,
    role: str,
    invited_by_user_id: int,
    expires_at: str,
) -> str:
    token = _new_token()
    with _connect() as conn:
        _execute(
            conn,
            """
            INSERT INTO workspace_invites (
              workspace_id,
              email,
              role,
              token,
              invited_by_user_id,
              expires_at,
              accepted_at,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
            """,
            (
                int(workspace_id),
                email.strip().lower(),
                role.strip().lower(),
                token,
                int(invited_by_user_id),
                expires_at,
                _utc_now_iso(),
            ),
        )
    return token


def list_workspace_invites(workspace_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT
              id,
              workspace_id,
              email,
              role,
              token,
              invited_by_user_id,
              expires_at,
              accepted_at,
              created_at
            FROM workspace_invites
            WHERE workspace_id = ?
            ORDER BY id DESC
            """,
            (int(workspace_id),),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_workspace_invite(token: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT
              id,
              workspace_id,
              email,
              role,
              token,
              invited_by_user_id,
              expires_at,
              accepted_at,
              created_at
            FROM workspace_invites
            WHERE token = ?
            LIMIT 1
            """,
            (token,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def accept_workspace_invite(*, token: str, user_id: int) -> dict[str, Any] | None:
    invite = get_workspace_invite(token)
    if invite is None:
        return None
    with _connect() as conn:
        _execute(
            conn,
            """
            UPDATE workspace_invites
            SET accepted_at = COALESCE(accepted_at, ?)
            WHERE token = ?
            """,
            (_utc_now_iso(), token),
        )
        _execute(
            conn,
            """
            INSERT INTO workspace_members (workspace_id, user_id, role, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(workspace_id, user_id) DO UPDATE SET role = excluded.role
            """,
            (
                int(invite["workspace_id"]),
                int(user_id),
                str(invite["role"]).lower(),
                _utc_now_iso(),
            ),
        )
    return get_workspace_invite(token)
