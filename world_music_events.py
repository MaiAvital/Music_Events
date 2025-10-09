import argparse
import os
import csv
import datetime as dt
from pathlib import Path
import json
from urllib import request, parse

API_KEY = os.getenv("TICKETMASTER_API_KEY")
if not API_KEY:
    raise ValueError("Missing TICKETMASTER_API_KEY environment variable")

BASE_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
DEEP_PAGING_LIMIT = 1000

# Default countries
DEFAULT_ALLOWED = ["Israel", "United States Of America", "Great Britain", "Canada"]

# Optional normalization if you ever pass different spellings in CLI
COUNTRY_NORMALIZE = {
    "uk": "Great Britain",
    "united kingdom": "Great Britain",
    "great britain": "Great Britain",
    "usa": "United States Of America",
    "united states": "United States Of America",
    "united states of america": "United States Of America",
    "israel": "Israel",
    "ca": "Canada",
    "canada": "Canada",

}


def to_iso_range(start_date: dt.date, end_date: dt.date) -> tuple[str, str]:
    start_iso = f"{start_date.strftime('%Y-%m-%d')}T00:00:00Z"
    end_iso = f"{end_date.strftime('%Y-%m-%d')}T23:59:59Z"
    return start_iso, end_iso


def extract_artists(e: dict) -> list[str]:
    atts = ((e.get("_embedded") or {}).get("attractions")) or []
    names = [a.get("name") for a in atts if isinstance(a, dict) and a.get("name")]
    seen, out = set(), []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def normalize_row(e: dict) -> dict:
    emb = e.get("_embedded") or {}
    venues = emb.get("venues") or [{}]
    v = venues[0] if venues else {}

    cls_list = e.get("classifications") or [{}]
    cls = cls_list[0] if cls_list else {}
    genre_name = (cls.get("genre") or {}).get("name")
    subgenre_name = (cls.get("subGenre") or {}).get("name")

    start = (e.get("dates") or {}).get("start") or {}
    local_date = start.get("localDate")
    local_time = start.get("localTime")

    artists = extract_artists(e)
    artist_primary = artists[0] if artists else None
    artists_joined = "; ".join(artists) if artists else None

    return {
        "event_id": e.get("id"),
        "event_name": e.get("name"),
        "local_date": local_date,
        "local_time": local_time,
        "city": (v.get("city") or {}).get("name"),
        "country": (v.get("country") or {}).get("name"),
        "venue_name": v.get("name"),
        "genre_name": genre_name,
        "subgenre_name": subgenre_name,
        "artist_primary": artist_primary,
        "artists": artists_joined,
        "url": e.get("url"),
    }


def fetch_events_page(
    segment_name: str,
    start_iso: str,
    end_iso: str,
    page: int = 0,
    size: int = 200,
    country_code: str | None = None,
) -> tuple[list[dict], bool]:
    params = {
        "segmentName": segment_name,
        "startDateTime": start_iso,
        "endDateTime": end_iso,
        "size": str(size),
        "page": str(page),
        "apikey": API_KEY,
    }
    if country_code:
        params["countryCode"] = country_code

    url = BASE_URL + "?" + parse.urlencode(params)
    with request.urlopen(url, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    events = (data.get("_embedded") or {}).get("events") or []
    rows = [normalize_row(e) for e in events]
    has_next = bool((data.get("_links") or {}).get("next"))
    return rows, has_next


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="World + IL Music Events (slim CSV) — Ticketmaster")
    p.add_argument("--start-date", type=str, help="YYYY-MM-DD (default: 2025-10-01)", default=None)
    p.add_argument("--end-date", type=str, help="YYYY-MM-DD (default: 2026-07-01)", default=None)  # extended to ~9 months
    p.add_argument("--out", type=str, default="world_music_events.csv", help="Output CSV path (slim)")
    p.add_argument("--segment", type=str, default="music", help="Segment name (default: 'music')")
    p.add_argument("--no-il", action="store_true", help="Do NOT fetch IL explicitly")
    p.add_argument("--allowed-countries", type=str, help="Comma-separated list of countries to KEEP.")
    p.add_argument("--blocked-countries", type=str, help="Comma-separated list of countries to EXCLUDE.")
    return p.parse_args()


def ensure_dates(args: argparse.Namespace) -> tuple[dt.date, dt.date]:
    default_start = dt.date(2025, 10, 1)
    default_end = dt.date(2026, 7, 1)  # extended default window

    start = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else default_start
    end = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else default_end

    if end < start:
        raise ValueError("end-date must be on/after start-date")
    return start, end


def paginate(segment: str, start_iso: str, end_iso: str, country_code: str | None = None) -> list[dict]:
    rows_all: list[dict] = []
    page = 0
    while True:
        page_rows, has_next = fetch_events_page(segment, start_iso, end_iso, page=page, size=200, country_code=country_code)
        if not page_rows:
            break
        rows_all.extend(page_rows)
        if not has_next or (page + 1) * 200 >= DEEP_PAGING_LIMIT:
            break
        page += 1
    return rows_all


def _normalize_country_label(name: str) -> str:
    key = (name or "").strip().lower()
    return COUNTRY_NORMALIZE.get(key, name)


def parse_list(arg_val: str | None, default_list=None) -> list[str]:
    if arg_val is None:
        return list(default_list or [])
    items = [x.strip() for x in arg_val.split(",") if x.strip()]
    # Normalize common synonyms to the exact Ticketmaster labels we expect
    return [_normalize_country_label(it) for it in items]


def filter_by_countries(rows: list[dict], allowed=None, blocked=None) -> list[dict]:
    allowed_set = set((a or "").lower() for a in (allowed or []))
    blocked_set = set((b or "").lower() for b in (blocked or []))

    out = []
    for r in rows:
        c = (r.get("country") or "").strip()
        cl = c.lower()
        if allowed_set and cl not in allowed_set:
            continue
        if blocked_set and cl in blocked_set:
            continue
        out.append(r)
    return out


def main() -> None:
    if not API_KEY:
        raise ValueError("Missing API_KEY")

    args = parse_args()
    start, end = ensure_dates(args)
    start_iso, end_iso = to_iso_range(start, end)

    allowed = parse_list(args.allowed_countries, default_list=DEFAULT_ALLOWED)
    blocked = parse_list(args.blocked_countries, default_list=[])

    # Global pass + explicit IL (improves IL coverage)
    world_rows = paginate(args.segment, start_iso, end_iso, country_code=None)
    il_rows = [] if args.no_il else paginate(args.segment, start_iso, end_iso, country_code="IL")

    # Deduplicate by event_id
    combined: dict[str, dict] = {}
    for r in world_rows + il_rows:
        combined[r["event_id"]] = r
    all_rows = list(combined.values())

    # Country filter
    filtered_rows = filter_by_countries(all_rows, allowed=allowed, blocked=blocked)

    # Write slim CSV ready for Power BI
    headers = [
        "event_id",
        "event_name",
        "local_date",
        "local_time",
        "city",
        "country",
        "venue_name",
        "genre_name",
        "subgenre_name",
        "artist_primary",
        "artists",
        "url",
    ]
    out_path = Path(args.out).resolve()
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in filtered_rows:
            w.writerow(r)


if __name__ == "__main__":
    main()
