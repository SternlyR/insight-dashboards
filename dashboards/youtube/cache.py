from datetime import datetime, timedelta
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
    _scheduler.add_job(_refresh,"interval",minutes=interval,next_run_time=datetime.now(),id="yt_refresh")
    _scheduler.start()

async def stop():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)

async def refresh_now(): await _refresh()

async def _refresh():
    if not _engine: return
    api = YouTubeAPI(settings.youtube_api_key)
    cid = settings.youtube_channel_id
    try: shorts = await api.get_channel_shorts(cid, max_results=50)
    except Exception as e: print(f"[yt-cache] error: {e}"); return
    async with get_session(_engine) as session:
        for rank, s in enumerate(shorts):
            stmt = sqlite_insert(YoutubeShort).values(
                channel_id=cid, video_id=s.video_id, title=s.title,
                description=s.description, published_at=s.published_at,
                thumbnail_url=s.thumbnail_url, duration_seconds=s.duration_seconds,
                views=s.views, likes=s.likes, comments=s.comments,
                rank=rank, updated_at=datetime.utcnow()
            ).on_conflict_do_update(index_elements=["video_id"],
                set_={"title":s.title,"views":s.views,"likes":s.likes,
                      "comments":s.comments,"rank":rank,"updated_at":datetime.utcnow()})
            await session.execute(stmt)
        total = sum(s.views for s in shorts)
        session.add(YoutubeViewsSnapshot(channel_id=cid,timestamp=datetime.utcnow(),total_views=total))
        cutoff = datetime.utcnow() - timedelta(hours=48)
        await session.execute(delete(YoutubeViewsSnapshot).where(YoutubeViewsSnapshot.timestamp < cutoff))
        await session.commit()
    print(f"[yt-cache] {len(shorts)} shorts cached, total={total:,}")

async def get_shorts(limit=9):
    if not _engine: return []
    async with get_session(_engine) as session:
        res = await session.execute(select(YoutubeShort)
            .where(YoutubeShort.channel_id==settings.youtube_channel_id)
            .order_by(YoutubeShort.rank).limit(limit))
        rows = res.scalars().all()
    return [{"video_id":r.video_id,"title":r.title,"thumbnail_url":r.thumbnail_url,
             "views":r.views,"likes":r.likes,"comments":r.comments,
             "published_at":r.published_at.isoformat() if r.published_at else None} for r in rows]

async def get_metrics_7d():
    if not _engine: return {"views":0,"likes":0,"comments":0}
    cutoff = datetime.utcnow() - timedelta(days=7)
    async with get_session(_engine) as session:
        res = await session.execute(select(func.sum(YoutubeShort.views),func.sum(YoutubeShort.likes),func.sum(YoutubeShort.comments))
            .where(YoutubeShort.channel_id==settings.youtube_channel_id, YoutubeShort.published_at>=cutoff))
        row = res.one()
    return {"views":row[0] or 0,"likes":row[1] or 0,"comments":row[2] or 0}

async def get_chart_data(hours=24):
    if not _engine: return {"labels":[],"values":[]}
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    async with get_session(_engine) as session:
        res = await session.execute(select(YoutubeViewsSnapshot)
            .where(YoutubeViewsSnapshot.channel_id==settings.youtube_channel_id, YoutubeViewsSnapshot.timestamp>=cutoff)
            .order_by(YoutubeViewsSnapshot.timestamp))
        snaps = res.scalars().all()
    if len(snaps) < 2: return {"labels":[],"values":[]}
    labels, values = [], []
    for i in range(1,len(snaps)):
        delta = max(0, snaps[i].total_views - snaps[i-1].total_views)
        ts = snaps[i].timestamp
        labels.append(f"{ts.hour:02d}:{ts.minute:02d}")
        values.append(delta)
    return {"labels":labels,"values":values}
