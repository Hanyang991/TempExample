from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseModel):
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")
    slack_channel_daily: str = os.getenv("SLACK_CHANNEL_DAILY", "#kb-trends-daily")
    slack_channel_alert: str = os.getenv("SLACK_CHANNEL_ALERT", "#kb-trends-alert")

    postgres_dsn: str = os.getenv("POSTGRES_DSN", "")

    trends_mode: str = os.getenv("TRENDS_MODE", "pytrends")

    pytrends_hl: str = os.getenv("PYTRENDS_HL", "en-US")
    pytrends_tz: int = int(os.getenv("PYTRENDS_TZ", "0"))

    google_trends_api_key: str = os.getenv("GOOGLE_TRENDS_API_KEY", "")

settings = Settings()
