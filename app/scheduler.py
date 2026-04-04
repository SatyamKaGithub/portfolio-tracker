import os
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.db import SessionLocal
from app.models import RecurringSip
from app.services import run_price_alert_check_job, run_sip_job

SCHEDULER_TZ = "Asia/Kolkata"
SCHEDULER_HOUR = int(os.getenv("SIP_SCHEDULER_HOUR", "9"))
SCHEDULER_MINUTE = int(os.getenv("SIP_SCHEDULER_MINUTE", "5"))
ALERT_CHECK_INTERVAL_MINUTES = int(os.getenv("PRICE_ALERT_CHECK_INTERVAL_MINUTES", "10"))

_scheduler: BackgroundScheduler | None = None


def _scheduler_enabled() -> bool:
    value = str(os.getenv("ENABLE_SIP_SCHEDULER", "1")).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _run_sip_job_task() -> None:
    db = SessionLocal()
    try:
        user_ids = [
            row[0]
            for row in db.query(RecurringSip.user_id).distinct().all()
            if row[0] is not None
        ]
        if not user_ids:
            return
        for user_id in user_ids:
            run_sip_job(db, user_id=user_id, trigger="SCHEDULED", force=False)
    finally:
        db.close()


def _run_price_alert_job_task() -> None:
    db = SessionLocal()
    try:
        run_price_alert_check_job(db)
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
    scheduler.add_job(
        _run_price_alert_job_task,
        trigger="interval",
        minutes=max(1, ALERT_CHECK_INTERVAL_MINUTES),
        id="price_alert_job",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
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
        alert_job = _scheduler.get_job("price_alert_job")
        alert_next_run_at = alert_job.next_run_time.isoformat() if alert_job and alert_job.next_run_time else None
    else:
        alert_next_run_at = None

    return {
        "enabled": _scheduler_enabled(),
        "running": running,
        "timezone": SCHEDULER_TZ,
        "hour": SCHEDULER_HOUR,
        "minute": SCHEDULER_MINUTE,
        "price_alert_interval_minutes": max(1, ALERT_CHECK_INTERVAL_MINUTES),
        "price_alert_next_run_at": alert_next_run_at,
        "next_run_at": next_run_at,
    }
