"""
src/report_queue.py – Async Report Queue (Phase 14, Option D)
=============================================================
Background thread worker for heavy report computation.
Allows the UI menu to function at light speed while expensive
analytics (cohort matrices, LTV curves, player master lists)
compute in a detached thread.

Usage:
    from src.report_queue import ReportQueue
    queue = ReportQueue.get_instance()
    job_id = queue.submit("cohort_matrix", {"brand": "ALL"})
    status = queue.get_status(job_id)
    result = queue.get_result(job_id)
"""
from __future__ import annotations

import threading
import time
import uuid
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, Optional


class ReportJob:
    """Represents a single queued report computation."""
    __slots__ = ("id", "report_type", "params", "status", "result",
                 "error", "requested_at", "completed_at", "display_name")

    def __init__(self, report_type: str, params: dict, display_name: str = ""):
        self.id = str(uuid.uuid4())[:8]
        self.report_type = report_type
        self.params = params
        self.status = "pending"       # pending → running → done | error
        self.result: Any = None
        self.error: Optional[str] = None
        self.requested_at = datetime.now()
        self.completed_at: Optional[datetime] = None
        self.display_name = display_name or report_type.replace("_", " ").title()


# ── Registry of report computation functions ────────────────────────────
_REPORT_REGISTRY: Dict[str, Callable] = {}


def register_report(name: str):
    """Decorator to register a report computation function."""
    def decorator(fn: Callable):
        _REPORT_REGISTRY[name] = fn
        return fn
    return decorator


# ── Pre-registered report types ─────────────────────────────────────────

@register_report("full_financial_export")
def _compute_full_financial_export(params: dict) -> Any:
    """Generate the full master Excel export."""
    import pandas as pd
    from src.database import engine
    from src.analytics import (
        generate_monthly_summaries, generate_cohort_matrix,
        generate_segmentation_summary, generate_program_summary,
        generate_both_business_summary
    )
    from src.exporter import export_to_excel

    df = pd.read_sql("SELECT * FROM raw_financial_data", engine)
    if df.empty:
        return None
    df.rename(columns={"player_id": "id"}, inplace=True)
    summary = generate_monthly_summaries(df)
    cohorts = generate_cohort_matrix(df)
    segmentation = generate_segmentation_summary(df)
    both_biz = generate_both_business_summary(summary)
    buf = export_to_excel(summary, cohort_matrices=cohorts,
                          segmentation_df=segmentation,
                          both_business_df=both_biz)
    return buf.getvalue()


@register_report("player_master_list")
def _compute_player_master_list(params: dict) -> Any:
    """Generate the full player master list with CRM profiles."""
    import pandas as pd
    from src.database import engine
    from src.analytics import generate_player_master_list

    df = pd.read_sql("SELECT * FROM raw_financial_data", engine)
    if df.empty:
        return None
    df.rename(columns={"player_id": "id"}, inplace=True)
    return generate_player_master_list(df)


@register_report("cohort_matrix")
def _compute_cohort_matrix(params: dict) -> Any:
    """Generate cohort retention matrices."""
    import pandas as pd
    from src.database import engine
    from src.analytics import generate_cohort_matrix

    df = pd.read_sql("SELECT * FROM raw_financial_data", engine)
    if df.empty:
        return None
    df.rename(columns={"player_id": "id"}, inplace=True)
    return generate_cohort_matrix(df)


@register_report("vip_churn_radar")
def _compute_vip_churn_radar(params: dict) -> Any:
    """Generate VIP churn radar analysis."""
    import pandas as pd
    from src.database import engine
    from src.analytics import generate_vip_churn_radar

    df = pd.read_sql("SELECT * FROM raw_financial_data", engine)
    if df.empty:
        return None
    df.rename(columns={"player_id": "id"}, inplace=True)
    return generate_vip_churn_radar(df)


# ── Queue Singleton ─────────────────────────────────────────────────────

class ReportQueue:
    """Thread-safe singleton report queue with background worker."""

    _instance: Optional["ReportQueue"] = None
    _lock = threading.Lock()

    def __init__(self):
        self._jobs: Dict[str, ReportJob] = {}
        self._queue: list[str] = []  # Job IDs in FIFO order
        self._worker_running = False
        self._worker_thread: Optional[threading.Thread] = None

    @classmethod
    def get_instance(cls) -> "ReportQueue":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def submit(self, report_type: str, params: dict | None = None,
               display_name: str = "") -> str:
        """Submit a report for background computation. Returns job_id."""
        if report_type not in _REPORT_REGISTRY:
            raise ValueError(f"Unknown report type: {report_type}. "
                             f"Available: {list(_REPORT_REGISTRY.keys())}")

        job = ReportJob(report_type, params or {}, display_name)
        self._jobs[job.id] = job
        self._queue.append(job.id)
        self._ensure_worker()
        return job.id

    def get_status(self, job_id: str) -> dict:
        """Get the current status of a job."""
        job = self._jobs.get(job_id)
        if not job:
            return {"status": "not_found"}
        return {
            "id": job.id,
            "report_type": job.report_type,
            "display_name": job.display_name,
            "status": job.status,
            "error": job.error,
            "requested_at": job.requested_at.strftime("%H:%M:%S"),
            "completed_at": (job.completed_at.strftime("%H:%M:%S")
                             if job.completed_at else None),
        }

    def get_result(self, job_id: str) -> Any:
        """Get the result of a completed job. Returns None if not done."""
        job = self._jobs.get(job_id)
        if job and job.status == "done":
            return job.result
        return None

    def get_all_jobs(self) -> list[dict]:
        """Get status of all jobs, most recent first."""
        return [self.get_status(jid) for jid in reversed(list(self._jobs.keys()))]

    def clear_completed(self):
        """Remove all completed/errored jobs from history."""
        to_remove = [jid for jid, j in self._jobs.items()
                     if j.status in ("done", "error")]
        for jid in to_remove:
            del self._jobs[jid]

    def _ensure_worker(self):
        """Start the background worker thread if not already running."""
        if not self._worker_running:
            self._worker_thread = threading.Thread(
                target=self._worker_loop, daemon=True)
            self._worker_thread.start()

    def _worker_loop(self):
        """Process queued jobs one at a time."""
        self._worker_running = True
        try:
            while self._queue:
                job_id = self._queue.pop(0)
                job = self._jobs.get(job_id)
                if not job or job.status != "pending":
                    continue

                job.status = "running"
                try:
                    fn = _REPORT_REGISTRY[job.report_type]
                    job.result = fn(job.params)
                    job.status = "done"
                except Exception as e:
                    job.status = "error"
                    job.error = f"{type(e).__name__}: {str(e)}"
                    traceback.print_exc()
                finally:
                    job.completed_at = datetime.now()
        finally:
            self._worker_running = False
