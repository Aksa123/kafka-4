import os
import json
import logging
import requests

logger = logging.getLogger(__name__)

SLACK_CONN_ID = "dte-slack-alert"

DEFAULT_CHANNEL_ID = "C0AA30YR10R"

# Service configuration (set these via Kubernetes env vars / secrets)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")         
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")             
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", DEFAULT_CHANNEL_ID)

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