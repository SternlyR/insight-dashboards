import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import httpx

@dataclass
class ShortVideo:
    video_id: str
    title: str
    description: str
    published_at: Optional[datetime]
    thumbnail_url: str
    duration_seconds: int
    views: int
    likes: int
    comments: int

class YouTubeAPI:
    BASE = "https://www.googleapis.com/youtube/v3"
    def __init__(self, api_key: str):
        self.api_key = api_key
    async def get_channel_shorts(self, channel_id: str, max_results: int = 50):
        ids = await self._search_shorts(channel_id, max_results)
        if not ids: return []
        videos = await self._fetch_details(ids)
        shorts = [v for v in videos if v.duration_seconds <= 60]
        shorts.sort(key=lambda v: v.views, reverse=True)
        return shorts
    async def _search_shorts(self, channel_id, max_results):
        p = {"part":"id","channelId":channel_id,"type":"video","videoDuration":"short",
             "order":"viewCount","maxResults":min(max_results,50),"key":self.api_key}
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(f"{self.BASE}/search", params=p); r.raise_for_status()
        return [i["id"]["videoId"] for i in r.json().get("items",[])]
    async def _fetch_details(self, ids):
        results = []
        for i in range(0,len(ids),50):
            chunk = ids[i:i+50]
            p = {"part":"snippet,contentDetails,statistics","id":",".join(chunk),"key":self.api_key}
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.get(f"{self.BASE}/videos", params=p); r.raise_for_status()
            for item in r.json().get("items",[]):
                v = self._parse(item)
                if v: results.append(v)
        return results
    def _parse(self, item):
        try:
            sn = item.get("snippet",{}); st = item.get("statistics",{})
            dt = item.get("contentDetails",{})
            th = (sn.get("thumbnails",{}).get("maxres") or sn.get("thumbnails",{}).get("high") or
                  sn.get("thumbnails",{}).get("medium") or sn.get("thumbnails",{}).get("default") or {})
            pub = None
            if raw := sn.get("publishedAt"):
                try: pub = datetime.fromisoformat(raw.replace("Z","+00:00"))
                except ValueError: pass
            return ShortVideo(video_id=item["id"], title=sn.get("title",""),
                description=sn.get("description",""), published_at=pub,
                thumbnail_url=th.get("url",""),
                duration_seconds=self._iso_to_secs(dt.get("duration","PT0S")),
                views=int(st.get("viewCount",0)), likes=int(st.get("likeCount",0)),
                comments=int(st.get("commentCount",0)))
        except (KeyError, ValueError): return None
    @staticmethod
    def _iso_to_secs(d):
        m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", d)
        if not m: return 0
        return int(m.group(1) or 0)*3600 + int(m.group(2) or 0)*60 + int(m.group(3) or 0)
