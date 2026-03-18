import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from automation.seed_categories import SEED_CATEGORIES
from automation.engine import run_category_scan

logger = logging.getLogger(__name__)

# How many categories to scan in parallel each tick
PARALLEL_CATEGORIES = 2

scheduler      = AsyncIOScheduler()
_category_index = 0
_last_run: dict = None
_recent_runs: list = []
_is_scanning   = False


def _next_categories(n: int) -> list[str]:
    global _category_index
    cats = []
    for _ in range(n):
        cats.append(SEED_CATEGORIES[_category_index % len(SEED_CATEGORIES)])
        _category_index += 1
    return cats


async def _scheduled_scan():
    global _last_run, _recent_runs, _is_scanning
    if _is_scanning:
        logger.info("[Scheduler] Scan in progress, skipping tick")
        return

    _is_scanning = True
    categories = _next_categories(PARALLEL_CATEGORIES)
    logger.info(f"[Scheduler] Parallel scan: {categories}")

    try:
        results = await asyncio.gather(
            *[run_category_scan(c) for c in categories],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, dict):
                _last_run = r
                _recent_runs.append(r)
        _recent_runs = _recent_runs[-100:]
    finally:
        _is_scanning = False


async def trigger_scan_now(category: str = None):
    global _last_run, _recent_runs, _is_scanning
    if _is_scanning:
        return {"error": "Scan already in progress"}

    _is_scanning = True
    if category:
        cats = [category]
    else:
        cats = _next_categories(PARALLEL_CATEGORIES)

    try:
        results = await asyncio.gather(
            *[run_category_scan(c) for c in cats],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, dict):
                _last_run = r
                _recent_runs.append(r)
        _recent_runs = _recent_runs[-100:]
        return _last_run
    finally:
        _is_scanning = False


def start_scheduler(interval_minutes: int = 5):
    if scheduler.running:
        return
    scheduler.add_job(
        _scheduled_scan,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="category_scan",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"[Scheduler] Started — {PARALLEL_CATEGORIES} categories every {interval_minutes} min")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("[Scheduler] Stopped")


def get_status() -> dict:
    job = scheduler.get_job("category_scan")
    next_run = job.next_run_time.isoformat() if (job and job.next_run_time) else None
    return {
        "running":          scheduler.running,
        "is_scanning":      _is_scanning,
        "next_run":         next_run,
        "last_run":         _last_run,
        "total_runs":       len(_recent_runs),
        "recent_runs":      list(reversed(_recent_runs[-10:])),
        "categories_total": len(SEED_CATEGORIES),
        "category_index":   _category_index,
        "next_category":    SEED_CATEGORIES[_category_index % len(SEED_CATEGORIES)],
        "parallel_categories": PARALLEL_CATEGORIES,
    }
