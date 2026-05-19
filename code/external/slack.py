import os
import json
import logging
import requests
import yaml
from loggers import logger
from datetime import datetime, timedelta


SLACK_CONN_ID = "dte-slack-alert"

DEFAULT_CHANNEL_ID = "C0AA30YR10R"

# Read from /var/secrets/data.yaml (GSM-mounted) once at import time.
# Env vars take precedence so local testing can override.
_YAML_SECRETS = {}
try:
    with open("/var/secrets/data.yaml", "r") as _f:
        _YAML_SECRETS = (yaml.safe_load(_f) or {}).get("secrets", {}) or {}
except FileNotFoundError:
    logger.warning("/var/secrets/data.yaml not found; falling back to env vars for Slack creds.")
except Exception as _e:
    logger.warning(f"Failed reading /var/secrets/data.yaml for Slack creds: {_e}")

def _resolve(key: str, default: str | None = None) -> str | None:
    return os.getenv(key) or _YAML_SECRETS.get(key) or default

SLACK_WEBHOOK_URL = _resolve("SLACK_WEBHOOK_URL")
SLACK_BOT_TOKEN = _resolve("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = _resolve("SLACK_CHANNEL_ID", DEFAULT_CHANNEL_ID)


def notify_slack(
    text: str,
    *,
    channel_id: str | None = None,
) -> None:
    """
    Slack notifier for NON-Airflow services.

    Preferred: Incoming webhook (SLACK_WEBHOOK_URL)
    Alternative: Bot token (SLACK_BOT_TOKEN) + channel_id
    """
    channel_id = channel_id or SLACK_CHANNEL_ID

    try:
        if SLACK_WEBHOOK_URL:
            resp = requests.post(
                SLACK_WEBHOOK_URL,
                data=json.dumps({"text": text}),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code >= 300:
                logger.error(f"Slack webhook failed: {resp.status_code} {resp.text}")
            return

        if SLACK_BOT_TOKEN:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={
                    "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                json={"channel": channel_id, "text": text},
                timeout=10,
            )
            if resp.status_code >= 300:
                logger.error(f"Slack API failed: {resp.status_code} {resp.text}")
            return

        logger.warning(
            "Slack not configured. Set SLACK_WEBHOOK_URL or SLACK_BOT_TOKEN (and optionally SLACK_CHANNEL_ID)."
        )

    except Exception as e:
        logger.error(f"Slack notify exception: {e}")


class SlackPusher:
    def __init__(self, interval: int = 300):
        self.interval = timedelta(seconds=interval)
        self.last_push = datetime(year=2020, month=1, day=1)
    
    def limiter_decorator(func):
        def inner(self, *args, **kwargs):
            dt = datetime.now()
            if dt > self.last_push + self.interval:
                self.last_push = dt
                res = func(self, *args, **kwargs)
                return res
        return inner
    
    @limiter_decorator
    def notify_slack(self, text, *args, channel_id):
        return notify_slack(text=text, *args, channel_id=channel_id)

slack_pusher = SlackPusher()