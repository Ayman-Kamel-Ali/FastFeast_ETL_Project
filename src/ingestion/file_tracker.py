"""
ingestion/file_tracker.py
=========================
SQLite-backed file tracker for idempotency.
Ensures every input file is processed exactly once.

Table: processed_files
  id            INTEGER PRIMARY KEY AUTOINCREMENT
  file_path     TEXT UNIQUE          -- absolute or project-relative path
  md5_hash      TEXT                 -- MD5 of file contents at processing time
  processed_at  TEXT                 -- ISO timestamp
  status        TEXT                 -- 'success' | 'failed' | 'skipped'
  rows_loaded   INTEGER              -- how many rows made it to the warehouse
  rows_rejected INTEGER              -- how many rows were quarantined

Idempotency logic:
  - Same path + same hash  → skip (already processed successfully)
  - Same path + diff hash  → reprocess (file was updated / regenerated)
  - Same path + status='failed' → reprocess (retry on next run)

Usage:
    from src.ingestion.file_tracker import FileTracker
    tracker = FileTracker()
    if tracker.is_processed("data/input/batch/2026-02-22/customers.csv"):
        return   # skip
    # ... process file ...
    tracker.mark_done("data/input/batch/2026-02-22/customers.csv", rows_loaded=498, rows_rejected=2)
"""

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _compute_md5(file_path: Path) -> str:
    """Compute MD5 hex digest of a file's contents."""
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class FileTracker:
    """
    SQLite-backed tracker.  One instance per pipeline run is fine —
    SQLite connections are not thread-safe by default so we open/close
    per operation to keep it simple and safe.
    """

    def __init__(self, db_path: Optional[str | Path] = None):
        if db_path is None:
            try:
                from config.settings import settings
                db_path = settings.paths.file_tracker_db
            except Exception:
                db_path = "file_tracker.db"

        # Resolve relative to project root
        project_root = Path(__file__).resolve().parent.parent.parent
        self.db_path = project_root / db_path
        self._ensure_table()

    # ── Private helpers ────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self) -> None:
        """Create the processed_files table if it doesn't exist."""
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path     TEXT    NOT NULL UNIQUE,
                    md5_hash      TEXT    NOT NULL,
                    processed_at  TEXT    NOT NULL,
                    status        TEXT    NOT NULL DEFAULT 'success',
                    rows_loaded   INTEGER DEFAULT 0,
                    rows_rejected INTEGER DEFAULT 0
                )
            """)
            conn.commit()

    # ── Public API ─────────────────────────────────────────────────────────────

    def is_processed(self, file_path: str | Path) -> bool:
        """
        Return True if this file has already been processed successfully
        AND its content hasn't changed since (same MD5).

        Returns False (= "process it") if:
          - Never seen before
          - Previously failed
          - File content changed (different MD5)
        """
        path = Path(file_path)
        if not path.exists():
            return False

        try:
            current_hash = _compute_md5(path)
        except Exception as exc:
            logger.warning(
                "Could not hash file — will reprocess",
                extra={"file": str(path), "error": str(exc)},
            )
            return False

        with self._connect() as conn:
            row = conn.execute(
                "SELECT md5_hash, status FROM processed_files WHERE file_path = ?",
                (str(path),),
            ).fetchone()

        if row is None:
            return False  # never seen

        if row["status"] != "success":
            return False  # previous attempt failed — retry

        if row["md5_hash"] != current_hash:
            logger.info(
                "File content changed — will reprocess",
                extra={"file": str(path)},
            )
            return False  # file was regenerated

        logger.info("File already processed — skipping", extra={"file": str(path)})
        return True

    def mark_done(
        self,
        file_path: str | Path,
        status: str = "success",
        rows_loaded: int = 0,
        rows_rejected: int = 0,
    ) -> None:
        """
        Record that a file has been processed.
        Uses INSERT OR REPLACE so re-runs update the existing row.
        """
        path = Path(file_path)
        try:
            md5 = _compute_md5(path) if path.exists() else "unknown"
        except Exception:
            md5 = "unknown"

        now = datetime.now(tz=timezone.utc).isoformat()

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO processed_files
                    (file_path, md5_hash, processed_at, status, rows_loaded, rows_rejected)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_path) DO UPDATE SET
                    md5_hash      = excluded.md5_hash,
                    processed_at  = excluded.processed_at,
                    status        = excluded.status,
                    rows_loaded   = excluded.rows_loaded,
                    rows_rejected = excluded.rows_rejected
                """,
                (str(path), md5, now, status, rows_loaded, rows_rejected),
            )
            conn.commit()

        logger.info(
            "File tracker updated",
            extra={
                "file": str(path),
                "status": status,
                "rows_loaded": rows_loaded,
                "rows_rejected": rows_rejected,
            },
        )

    def get_status(self, file_path: str | Path) -> Optional[dict]:
        """Return the tracker record for a file, or None if not tracked."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM processed_files WHERE file_path = ?",
                (str(Path(file_path)),),
            ).fetchone()
        return dict(row) if row else None

    def list_processed(self, run_date: Optional[str] = None) -> list[dict]:
        """
        Return all processed file records.
        Optionally filter by run_date string (matched as substring of file_path).
        """
        with self._connect() as conn:
            if run_date:
                rows = conn.execute(
                    "SELECT * FROM processed_files WHERE file_path LIKE ? ORDER BY processed_at",
                    (f"%{run_date}%",),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM processed_files ORDER BY processed_at"
                ).fetchall()
        return [dict(r) for r in rows]
