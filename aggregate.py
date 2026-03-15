#!/usr/bin/env python3
"""
Space Events Aggregator
Sources: Solar Flares + CME (NASA DONKI) · Gravitational Waves (LIGO GraceDB)
"""

import json
import os
import sys
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Optional

# CONFIG
RETENTION_DAYS: int = 14  # Events older than this are pruned
FETCH_TIMEOUT: int = 20  # HTTP timeout per request (seconds)
MAX_RESULTS: int = 100  # Max results per API call

OUT_PATH: str = os.path.join(os.path.dirname(__file__), "docs", "events.json")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("aggregator")


# Time helpers
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fmt_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        s = s.strip().replace(" ", "T")
        if not s.endswith("Z") and "+" not in s[10:] and s.count("-") < 3:
            s += "Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def event_dt(e: dict) -> datetime:
    return parse_iso(e.get("time", "")) or _EPOCH


# HTTP
def fetch_json(url: str) -> Optional[dict | list]:
    try:
        with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        log.warning("HTTP %s - %s", e.code, url)
    except Exception as e:
        log.warning("Fetch error %s: %s", url, e)
    return None


# remove duplicates
def dedup(events: list[dict]) -> list[dict]:
    seen, out = set(), []
    for e in events:
        if e["id"] not in seen:
            seen.add(e["id"])
            out.append(e)
    return out


# 1. Solar Flares — NASA DONKI
def fetch_solar_flares() -> list[dict]:
    start = utcnow() - timedelta(days=RETENTION_DAYS)
    url = (
        "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/FLR"
        f"?startDate={fmt_date(start)}&endDate={fmt_date(utcnow())}"
    )
    data = fetch_json(url)
    if not isinstance(data, list):
        log.warning("Solar flare fetch failed")
        return []

    events = []
    for item in data:
        begin = item.get("beginTime") or item.get("peakTime") or ""
        cls = item.get("classType", "")
        events.append(
            {
                "id": f"flr-{item.get('flrID', begin)}",
                "type": "solar_flare",
                "source": "NASA DONKI",
                "time": begin,
                "title": f"Solar Flare {cls or 'Unknown'}",
                "description": (
                    f"Class {cls or '?'} solar flare. "
                    f"Peak: {item.get('peakTime', 'N/A')}. "
                    f"Location: {item.get('sourceLocation', 'N/A')}."
                ),
                "severity": _flare_sev(cls),
                "metadata": {
                    "class": cls,
                    "peak_time": item.get("peakTime"),
                    "end_time": item.get("endTime"),
                    "source_location": item.get("sourceLocation"),
                    "active_region": item.get("activeRegionNum"),
                },
                "url": item.get("link"),
            }
        )

    log.info("Solar flares: %d", len(events))
    return events


def _flare_sev(cls: str) -> str:
    return {
        "X": "extreme",
        "M": "high",
        "C": "moderate",
        "B": "low",
        "A": "minimal",
    }.get((cls or " ")[0].upper(), "unknown")


# 2. Coronal Mass Ejections — NASA DONKI
def fetch_cme() -> list[dict]:
    start = utcnow() - timedelta(days=RETENTION_DAYS)
    url = (
        "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/CMEAnalysis"
        f"?startDate={fmt_date(start)}&endDate={fmt_date(utcnow())}&mostAccurateOnly=true"
    )
    data = fetch_json(url)
    if not isinstance(data, list):
        log.warning("CME fetch failed")
        return []

    events = []
    for item in data:
        time_str = item.get("time21_5") or ""
        speed = item.get("speed")
        cme_id = item.get("associatedCMEID", time_str)
        events.append(
            {
                "id": f"cme-{cme_id}",
                "type": "coronal_mass_ejection",
                "source": "NASA DONKI",
                "time": time_str,
                "title": "Coronal Mass Ejection",
                "description": (
                    f"CME at 21.5 solar radii. Speed: {speed or 'N/A'} km/s. "
                    f"Type: {item.get('type', 'N/A')}."
                ),
                "severity": _cme_sev(speed),
                "metadata": {
                    "speed_km_s": speed,
                    "type": item.get("type"),
                    "half_angle": item.get("halfAngle"),
                    "latitude": item.get("latitude"),
                    "longitude": item.get("longitude"),
                },
                "url": item.get("link"),
            }
        )

    log.info("CME: %d", len(events))
    return events


def _cme_sev(speed) -> str:
    try:
        s = float(speed)
        if s >= 2000:
            return "extreme"
        if s >= 1000:
            return "high"
        if s >= 500:
            return "moderate"
        return "low"
    except (TypeError, ValueError):
        return "unknown"


# 3. Gravitational Waves — LIGO/Virgo GraceDB
def fetch_gravitational_waves() -> list[dict]:
    url = "https://gracedb.ligo.org/api/superevents/?format=json&limit=50&orderby=-created"
    data = fetch_json(url)
    if not isinstance(data, dict):
        log.warning("GraceDB fetch failed")
        return []

    cutoff = utcnow() - timedelta(days=RETENTION_DAYS)
    events = []
    for item in data.get("superevents", []):
        created_str = item.get("created", "")
        dt = parse_iso(created_str)
        if not dt or dt < cutoff:
            continue
        sid = item.get("superevent_id", "")
        far = item.get("far")
        far_str = f"{float(far):.2e} Hz" if far else "N/A"
        events.append(
            {
                "id": f"gw-{sid}",
                "type": "gravitational_wave",
                "source": "LIGO/Virgo GraceDB",
                "time": created_str,
                "title": f"Gravitational Wave {sid}",
                "description": (
                    f"GW superevent {sid} ({item.get('category', '?')}). "
                    f"False alarm rate: {far_str}."
                ),
                "severity": "high",
                "metadata": {
                    "superevent_id": sid,
                    "category": item.get("category"),
                    "false_alarm_rate": far,
                    "labels": item.get("labels", []),
                    "preferred_event": item.get("preferred_event"),
                },
                "url": f"https://gracedb.ligo.org/superevents/{sid}/",
            }
        )

    log.info("Gravitational waves: %d", len(events))
    return events


# Output
def build_output(events: list[dict]) -> dict:
    now = utcnow()
    cutoff = now - timedelta(days=RETENTION_DAYS)
    counts: dict[str, int] = {}
    for e in events:
        counts[e["type"]] = counts.get(e["type"], 0) + 1
    return {
        "meta": {
            "generated_at": fmt_iso(now),
            "window_start": fmt_iso(cutoff),
            "window_end": fmt_iso(now),
            "retention_days": RETENTION_DAYS,
            "total_events": len(events),
            "event_counts": counts,
            "schema_version": "2.0",
            "sources": [
                "NASA DONKI (Solar Flares, CME)",
                "LIGO/Virgo GraceDB (Gravitational Waves)",
            ],
        },
        "events": events,
    }


# Main
def main() -> int:
    events: list[dict] = []
    events.extend(fetch_solar_flares())
    events.extend(fetch_cme())
    events.extend(fetch_gravitational_waves())

    cutoff = utcnow() - timedelta(days=RETENTION_DAYS)
    events = dedup(events)
    events = [e for e in events if event_dt(e) >= cutoff]
    events.sort(key=event_dt, reverse=True)

    output = build_output(events)
    abs_path = os.path.abspath(OUT_PATH)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)

    with open(abs_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    log.info("Wrote %d events → %s", len(events), abs_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
