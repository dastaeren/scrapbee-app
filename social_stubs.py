from core.models import ColumnSchema, Settings, ScrapeResult


class SocialPlatformStub:
    def __init__(self, platform: str):
        self.platform = platform

    def scrape_one(self, url: str, schema: ColumnSchema, settings: Settings) -> ScrapeResult:
        return ScrapeResult(
            url=url,
            platform=self.platform,
            data={c: "N/A" for c in schema.columns},
            status="warning",
            message=f"{self.platform} scraper is a stub. Use official API/authorized Apify actor with proper permissions."
        )
