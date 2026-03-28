"""
utils/alerter.py
================
Sends failure alert emails asynchronously in a background daemon thread.
The pipeline NEVER blocks waiting for email delivery.
Alerts are ONLY sent on failure — never on success.

Usage:
    from src.utils.alerter import send_alert_async
    send_alert_async(
        subject="Schema validation failed",
        error_message="Missing columns: ['order_id', 'customer_id']",
        context={"file": "orders.json", "table": "orders", "run_date": "2026-02-22"}
    )
"""

import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _build_email_body(subject: str, error_message: str, context: dict) -> str:
    """Build a plain-text email body with structured context."""
    lines = [
        "FastFeast Pipeline — Failure Alert",
        "=" * 50,
        f"Time     : {datetime.now(tz=timezone.utc).isoformat()}",
        f"Subject  : {subject}",
        "",
        "Error:",
        "-" * 30,
        error_message,
        "",
        "Context:",
        "-" * 30,
    ]
    for key, value in context.items():
        lines.append(f"  {key}: {value}")

    lines += [
        "",
        "=" * 50,
        "This is an automated alert from the FastFeast pipeline.",
        "Do NOT reply to this email.",
    ]
    return "\n".join(lines)


def _send_email(subject: str, error_message: str, context: dict) -> None:
    """
    Internal function run inside a daemon thread.
    Any exception here is logged but never propagated — we must never
    let a failed alert crash or block the pipeline.
    """
    try:
        from config.settings import settings

        if not getattr(settings.alerts, "enabled", True):
            logger.info("Alerts disabled in config — skipping email")
            return

        smtp_host = settings.alerts.smtp_host
        smtp_port = int(settings.alerts.smtp_port)
        use_tls = getattr(settings.alerts, "use_tls", True)
        sender = settings.alerts.sender_email
        password = settings.alerts.sender_password
        recipients = settings.alerts.recipients

        if not isinstance(recipients, list):
            recipients = [recipients]

        # Build message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[FastFeast Pipeline ALERT] {subject}"
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)

        body = _build_email_body(subject, error_message, context)
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # Send
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            if use_tls:
                server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())

        logger.info(
            "Alert email sent",
            extra={"subject": subject, "recipients": recipients},
        )

    except Exception as exc:
        # Log the failure — do NOT raise, do NOT crash the pipeline
        logger.error(
            "Failed to send alert email",
            extra={"subject": subject, "error": str(exc)},
        )


def send_alert_async(
    subject: str,
    error_message: str,
    context: Optional[dict] = None,
) -> None:
    """
    Dispatch an alert email in a background daemon thread.
    Returns immediately — the pipeline continues without waiting.

    Args:
        subject:       Short description of the failure (used as email subject).
        error_message: Full error text / traceback.
        context:       Dict of key-value pairs providing extra context
                       (e.g. file path, table name, run date).
    """
    if context is None:
        context = {}

    thread = threading.Thread(
        target=_send_email,
        args=(subject, error_message, context),
        daemon=True,   # Dies automatically when main process exits
        name=f"alert-{subject[:30]}",
    )
    thread.start()
    logger.info(
        "Alert dispatched (async)",
        extra={"subject": subject, "context": context},
    )
