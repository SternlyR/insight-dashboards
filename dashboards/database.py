from contextlib import asynccontextmanager
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text, BigInteger
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from dashboards.config import settings

class Base(DeclarativeBase):
    pass

class YoutubeShort(Base):
    __tablename__ = "youtube_shorts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(String(100), nullable=False, index=True)
    video_id = Column(String(20), unique=True, nullable=False)
    title = Column(String(500))
    description = Column(Text)
    published_at = Column(DateTime)
    thumbnail_url = Column(String(1000))
    duration_seconds = Column(Integer)
    views = Column(BigInteger, default=0)
    likes = Column(BigInteger, default=0)
    comments = Column(BigInteger, default=0)
    rank = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class YoutubeViewsSnapshot(Base):
    __tablename__ = "youtube_views_snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(String(100), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    total_views = Column(BigInteger, default=0)

async def init_db():
    engine = create_async_engine(settings.database_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine

@asynccontextmanager
async def get_session(engine):
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
