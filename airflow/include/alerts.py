"""Called by Airflow when a task fails after its final retry."""
import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def notify_failure(context):
    ti = context["ti"]
    message = {
        "text": f"EdgePulse failure: {ti.dag_id}.{ti.task_id}, run {ti.run_id}, try {ti.try_number}",
        "error": str(context.get("exception")),
    }
    log.error("ALERT %s", message)
    url = os.getenv("ALERT_WEBHOOK_URL")
    if url:
        request = urllib.request.Request(
            url, data=json.dumps(message).encode(), headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(request, timeout=10)