"""YouTube Analytics API v2 — daily view totals used for chart backfill."""

from datetime import datetime, timedelta, timezone
import httpx


class YouTubeAnalyticsAPI:
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    REPORTS_URL = "https://youtubeanalytics.googleapis.com/v2/reports"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

    async def _access_token(self) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(self.TOKEN_URL, data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            })
            r.raise_for_status()
            return r.json()["access_token"]

    async def get_daily_views(self, days: int = 8) -> dict[str, int]:
        """Return {YYYY-MM-DD: total_views} for the last `days` days (all channel content)."""
        token = await self._access_token()
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(self.REPORTS_URL, params={
                "ids": "channel==MINE",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views",
                "dimensions": "day",
                "sort": "day",
            }, headers={"Authorization": f"Bearer {token}"})
            r.raise_for_status()
            data = r.json()

        return {row[0]: int(row[1]) for row in data.get("rows", [])}
