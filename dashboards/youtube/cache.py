"""APScheduler-backed YouTube data cache with SQLite persistence."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, func, delete
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from dashboards.config import settings
from dashboards.database import YoutubeShort, YoutubeViewsSnapshot, get_session
from dashboards.youtube.api import YouTubeAPI

_scheduler: Optional[AsyncIOScheduler] = None
_engine = None


async def start(engine):
    global _scheduler, _engine
    _engine = engine
    _scheduler = AsyncIOScheduler()
    interval = max(5, settings.youtube_refresh_interval_minutes)
    _scheduler.add_job(
        _refresh,
        "interval",
        minutes=interval,
        next_run_time=datetime.now(),  # fire immediately on startup
        id="yt_refresh",
    )
    _scheduler.start()


async def stop():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


async def refresh_now():
    await _refresh()


async def _refresh():
    if not _engine:
        return
    api = YouTubeAPI(settings.youtube_api_key)
    channel_id = settings.youtube_channel_id
    try:
        shorts = await api.get_channel_shorts(channel_id, max_results=50)
    except Exception as exc:
        print(f"[yt-cache] API error: {exc}")
        return

    async with get_session(_engine) as session:
        # Upsert each short
        for rank, short in enumerate(shorts):
            stmt = sqlite_insert(YoutubeShort).values(
                channel_id=channel_id,
                video_id=short.video_id,
                title=short.title,
                description=short.description,
                published_at=short.published_at,
                thumbnail_url=short.thumbnail_url,
                duration_seconds=short.duration_seconds,
                views=short.views,
                likes=short.likes,
                comments=short.comments,
                rank=rank,
                updated_at=datetime.utcnow(),
            ).on_conflict_do_update(
                index_elements=["video_id"],
                set_={
                    "title": short.title,
                    "views": short.views,
                    "likes": short.likes,
                    "comments": short.comments,
                    "rank": rank,
                    "updated_at": datetime.utcnow(),
                },
            )
            await session.execute(stmt)

        # Snapshot total views
        total_views = sum(s.views for s in shorts)
        session.add(YoutubeViewsSnapshot(
            channel_id=channel_id,
            timestamp=datetime.utcnow(),
            total_views=total_views,
        ))

        # Prune snapshots older than 48 hours
        cutoff = datetime.utcnow() - timedelta(hours=48)
        await session.execute(
            delete(YoutubeViewsSnapshot).where(YoutubeViewsSnapshot.timestamp < cutoff)
        )

        await session.commit()

    print(f"[yt-cache] Refreshed {len(shorts)} shorts, total_views={total_views:,}")


async def get_shorts(limit: int = 9) -> list[dict]:
    if not _engine:
        return []
    cutoff_30d = datetime.utcnow() - timedelta(days=30)
    async with get_session(_engine) as session:
        result = await session.execute(
            select(YoutubeShort)
            .where(
                YoutubeShort.channel_id == settings.youtube_channel_id,
                YoutubeShort.published_at >= cutoff_30d,
            )
            .order_by(YoutubeShort.rank)
            .limit(limit)
        )
        rows = result.scalars().all()
    return [
        {
            "video_id": r.video_id,
            "title": r.title,
            "thumbnail_url": r.thumbnail_url,
            "views": r.views,
            "likes": r.likes,
            "comments": r.comments,
            "published_at": r.published_at.isoformat() if r.published_at else None,
        }
        for r in rows
    ]


async def get_metrics_30d() -> dict:
    if not _engine:
        return {"views": 0, "likes": 0, "comments": 0}
    cutoff = datetime.utcnow() - timedelta(days=30)
    async with get_session(_engine) as session:
        result = await session.execute(
            select(
                func.sum(YoutubeShort.views),
                func.sum(YoutubeShort.likes),
                func.sum(YoutubeShort.comments),
            ).where(
                YoutubeShort.channel_id == settings.youtube_channel_id,
                YoutubeShort.published_at >= cutoff,
            )
        )
        row = result.one()
    return {
        "views": row[0] or 0,
        "likes": row[1] or 0,
        "comments": row[2] or 0,
    }


async def get_chart_data(hours: int = 24) -> dict:
    """Returns labels + values for a bar chart of view deltas per snapshot interval."""
    if not _engine:
        return {"labels": [], "values": []}
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    async with get_session(_engine) as session:
        result = await session.execute(
            select(YoutubeViewsSnapshot)
            .where(
                YoutubeViewsSnapshot.channel_id == settings.youtube_channel_id,
                YoutubeViewsSnapshot.timestamp >= cutoff,
            )
            .order_by(YoutubeViewsSnapshot.timestamp)
        )
        snaps = result.scalars().all()

    if len(snaps) < 2:
        return {"labels": [], "values": []}

    labels = []
    values = []
    for i in range(1, len(snaps)):
        delta = max(0, snaps[i].total_views - snaps[i - 1].total_views)
        ts = snaps[i].timestamp
        labels.append(f"{ts.hour:02d}:{ts.minute:02d}")
        values.append(delta)

    return {"labels": labels, "values": values}
