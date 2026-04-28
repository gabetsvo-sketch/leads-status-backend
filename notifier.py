"""APNs push notifications for LeadsStatus iOS app.

Two flavors:
  - Regular alert push: banner + sound + badge for new leads.
  - Live Activity push: starts/updates/ends a lock-screen activity.

Token storage is a flat JSON file `devices.json`:
  {
    "devices": [
      {
        "device_token": "<hex>",
        "live_activity_token": "<hex|null>",
        "platform": "ios",
        "app_version": "1.0.3",
        "registered_at": "<iso>",
        "updated_at": "<iso>"
      }
    ]
  }
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("notifier")

DEVICES_FILE = Path(os.environ.get("DEVICES_FILE", "devices.json"))

APNS_KEY_ID = os.environ.get("APNS_KEY_ID", "").strip()
APNS_TEAM_ID = os.environ.get("APNS_TEAM_ID", "").strip()
APNS_BUNDLE_ID = os.environ.get("APNS_BUNDLE_ID", "com.gabetsvo.LeadsStatus").strip()
APNS_KEY_P8_B64 = os.environ.get("APNS_KEY_P8_B64", "").strip()
# "production" for TestFlight/AppStore, "sandbox" only for Xcode debug builds
APNS_ENVIRONMENT = os.environ.get("APNS_ENVIRONMENT", "production").strip()

_apns_client = None
_apns_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Token storage
# ---------------------------------------------------------------------------


def load_devices() -> list[dict]:
    if not DEVICES_FILE.exists():
        return []
    try:
        data = json.loads(DEVICES_FILE.read_text(encoding="utf-8"))
        return data.get("devices", []) if isinstance(data, dict) else []
    except Exception as e:
        log.error(f"load_devices failed: {e}")
        return []


def save_devices(devices: list[dict]) -> None:
    try:
        DEVICES_FILE.write_text(
            json.dumps({"devices": devices}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        log.error(f"save_devices failed: {e}")


def upsert_device(
    device_token: str,
    *,
    live_activity_token: Optional[str] = None,
    app_version: Optional[str] = None,
) -> dict:
    """Insert or update device by device_token. Returns the stored entry."""
    devices = load_devices()
    now = datetime.now(timezone.utc).isoformat()
    for d in devices:
        if d.get("device_token") == device_token:
            if live_activity_token is not None:
                d["live_activity_token"] = live_activity_token or None
            if app_version is not None:
                d["app_version"] = app_version
            d["updated_at"] = now
            save_devices(devices)
            return d
    entry = {
        "device_token": device_token,
        "live_activity_token": live_activity_token or None,
        "platform": "ios",
        "app_version": app_version or "",
        "registered_at": now,
        "updated_at": now,
    }
    devices.append(entry)
    save_devices(devices)
    return entry


def remove_device(device_token: str) -> bool:
    devices = load_devices()
    new = [d for d in devices if d.get("device_token") != device_token]
    if len(new) != len(devices):
        save_devices(new)
        return True
    return False


# ---------------------------------------------------------------------------
# APNs client
# ---------------------------------------------------------------------------


async def _get_apns_client():
    """Lazy-init APNs client. Returns None if APNs creds aren't set."""
    global _apns_client
    if _apns_client is not None:
        return _apns_client

    if not (APNS_KEY_ID and APNS_TEAM_ID and APNS_KEY_P8_B64):
        log.warning("APNs not configured (missing key/team/p8)")
        return None

    async with _apns_lock:
        if _apns_client is not None:
            return _apns_client

        try:
            from aioapns import APNs

            key_pem = base64.b64decode(APNS_KEY_P8_B64).decode("utf-8")
            use_sandbox = APNS_ENVIRONMENT == "sandbox"

            _apns_client = APNs(
                key=key_pem,
                key_id=APNS_KEY_ID,
                team_id=APNS_TEAM_ID,
                topic=APNS_BUNDLE_ID,
                use_sandbox=use_sandbox,
            )
            log.info(
                f"APNs initialized: key_id={APNS_KEY_ID} team={APNS_TEAM_ID} "
                f"topic={APNS_BUNDLE_ID} sandbox={use_sandbox}"
            )
            return _apns_client
        except Exception as e:
            log.error(f"APNs init failed: {e}")
            return None


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------


async def send_alert_push(
    title: str,
    body: str,
    *,
    badge: Optional[int] = None,
    custom: Optional[dict] = None,
    sound: str = "default",
) -> dict:
    """Send a regular alert push to every registered device.

    Returns {"sent": N, "failed": [tokens], "removed": [tokens]} where tokens
    in `removed` were invalid and have been pruned from devices.json.
    """
    apns = await _get_apns_client()
    if apns is None:
        return {"sent": 0, "failed": [], "removed": [], "skipped_reason": "apns_not_configured"}

    devices = load_devices()
    if not devices:
        return {"sent": 0, "failed": [], "removed": []}

    from aioapns import NotificationRequest, PushType

    aps = {
        "alert": {"title": title, "body": body},
        "sound": sound,
    }
    if badge is not None:
        aps["badge"] = badge
    payload = {"aps": aps}
    if custom:
        payload.update(custom)

    sent = 0
    failed: list[str] = []
    removed: list[str] = []

    async def _one(token: str):
        nonlocal sent
        try:
            req = NotificationRequest(
                device_token=token,
                message=payload,
                push_type=PushType.ALERT,
            )
            res = await apns.send_notification(req)
            if res.is_successful:
                sent += 1
                return
            reason = (res.description or "").lower()
            if "badtoken" in reason or "unregistered" in reason or res.status == "410":
                removed.append(token)
            else:
                failed.append(token)
                log.warning(f"APNs send failed token={token[:8]}…: {res.description}")
        except Exception as e:
            failed.append(token)
            log.warning(f"APNs send exception token={token[:8]}…: {e}")

    await asyncio.gather(*[_one(d["device_token"]) for d in devices])

    if removed:
        keep = [d for d in devices if d["device_token"] not in removed]
        save_devices(keep)
        log.info(f"pruned {len(removed)} invalid device token(s)")

    return {"sent": sent, "failed": failed, "removed": removed}


async def send_lead_push(lead: dict) -> dict:
    """High-level: notify about a new lead."""
    name = (lead.get("name") or "").strip() or "Без имени"
    phone = (lead.get("phone") or "").strip()
    source = (lead.get("source") or "").strip()
    body_parts = []
    if phone:
        body_parts.append(phone)
    if source:
        body_parts.append(source)
    body = " · ".join(body_parts) or "Новая заявка"
    return await send_alert_push(
        title=f"Новая заявка: {name}",
        body=body,
        custom={"lead_id": lead.get("lead_id"), "kind": "new_lead"},
    )
