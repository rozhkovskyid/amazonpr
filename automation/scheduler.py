import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from automation.seed_categories import SEED_CATEGORIES
from automation.engine import run_category_scan

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
_category_index = 0
_last_run: dict = None
_recent_runs: list = []
_is_scanning = False


def _next_category() -> str:
    global _category_index
    cat = SEED_CATEGORIES[_category_index % len(SEED_CATEGORIES)]
    _category_index += 1
    return cat


async def _scheduled_scan():
    global _last_run, _recent_runs, _is_scanning
    if _is_scanning:
        logger.info("[Automation] Scan already in progress, skipping")
        return

    _is_scanning = True
    category = _next_category()
    try:
        result = await run_category_scan(category)
        _last_run = result
        _recent_runs.append(result)
        if len(_recent_runs) > 100:
            _recent_runs.pop(0)
    finally:
        _is_scanning = False


async def trigger_scan_now(category: str = None):
    """Manually trigger a scan, optionally for a specific category."""
    global _last_run, _recent_runs, _is_scanning
    if _is_scanning:
        return {"error": "Scan already in progress"}
    _is_scanning = True
    cat = category or _next_category()
    try:
        result = await run_category_scan(cat)
        _last_run = result
        _recent_runs.append(result)
        if len(_recent_runs) > 100:
            _recent_runs.pop(0)
        return result
    finally:
        _is_scanning = False


def start_scheduler(interval_minutes: int = 30):
    if scheduler.running:
        return
    scheduler.add_job(
        _scheduled_scan,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="category_scan",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"[Automation] Scheduler started — scanning every {interval_minutes} minutes")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("[Automation] Scheduler stopped")


def get_status() -> dict:
    job = scheduler.get_job("category_scan")
    next_run = None
    if job and job.next_run_time:
        next_run = job.next_run_time.isoformat()
    return {
        "running": scheduler.running,
        "is_scanning": _is_scanning,
        "next_run": next_run,
        "last_run": _last_run,
        "total_runs": len(_recent_runs),
        "recent_runs": list(reversed(_recent_runs[-10:])),
        "categories_total": len(SEED_CATEGORIES),
        "category_index": _category_index,
        "next_category": SEED_CATEGORIES[_category_index % len(SEED_CATEGORIES)],
    }
