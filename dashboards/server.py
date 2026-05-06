"""FastAPI application — YouTube analytics dashboards."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse

from dashboards.database import init_db
import dashboards.youtube.cache as yt

FRONTEND = Path(__file__).parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = await init_db()
    await yt.start(engine)
    yield
    await yt.stop()


app = FastAPI(title="Insight Dashboards", lifespan=lifespan)


# ── API routes ────────────────────────────────────────────────────────────────

@app.get("/api/youtube/videos")
async def api_videos():
    return await yt.get_shorts(limit=9)


@app.get("/api/youtube/metrics")
async def api_metrics():
    return await yt.get_metrics_30d()


@app.get("/api/youtube/chart")
async def api_chart():
    return await yt.get_chart_data(hours=24)



@app.post("/api/youtube/refresh")
async def api_refresh():
    await yt.refresh_now()
    return {"status": "ok"}


# ── Frontend routes ───────────────────────────────────────────────────────────

@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse(url="/youtube-shorts")


@app.get("/youtube-shorts", response_class=HTMLResponse)
async def shorts_dashboard():
    html = (FRONTEND / "youtube-shorts" / "index.html").read_text()
    return HTMLResponse(content=html)
