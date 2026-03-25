import os
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.db import SessionLocal
from app.services import run_sip_job

SCHEDULER_TZ = "Asia/Kolkata"
SCHEDULER_HOUR = int(os.getenv("SIP_SCHEDULER_HOUR", "9"))
SCHEDULER_MINUTE = int(os.getenv("SIP_SCHEDULER_MINUTE", "5"))

_scheduler: BackgroundScheduler | None = None


def _scheduler_enabled() -> bool:
    value = str(os.getenv("ENABLE_SIP_SCHEDULER", "1")).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _run_sip_job_task() -> None:
    db = SessionLocal()
    try:
        run_sip_job(db, trigger="SCHEDULED", force=False)
    finally:
        db.close()


def start_sip_scheduler() -> None:
    global _scheduler

    if not _scheduler_enabled() or _scheduler is not None:
        return

    scheduler = BackgroundScheduler(timezone=ZoneInfo(SCHEDULER_TZ))
    scheduler.add_job(
        _run_sip_job_task,
        trigger=CronTrigger(hour=SCHEDULER_HOUR, minute=SCHEDULER_MINUTE, timezone=ZoneInfo(SCHEDULER_TZ)),
        id="sip_due_job",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    scheduler.start()
    _scheduler = scheduler


def stop_sip_scheduler() -> None:
    global _scheduler

    if _scheduler is None:
        return

    _scheduler.shutdown(wait=False)
    _scheduler = None


def get_sip_scheduler_status() -> dict:
    next_run_at = None
    running = bool(_scheduler and _scheduler.running)
    if _scheduler:
        job = _scheduler.get_job("sip_due_job")
        if job and job.next_run_time:
            next_run_at = job.next_run_time.isoformat()

    return {
        "enabled": _scheduler_enabled(),
        "running": running,
        "timezone": SCHEDULER_TZ,
        "hour": SCHEDULER_HOUR,
        "minute": SCHEDULER_MINUTE,
        "next_run_at": next_run_at,
    }
