import argparse
import base64
import json
import logging
import re
import time
from io import BytesIO
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 fallback
    ZoneInfo = None

import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont


BASE_URL = "https://www.margonem.pl"
STATS_URL = f"{BASE_URL}/stats"
GUILD_URL_TEMPLATE = BASE_URL + "/guilds/view,{world},{guild_id}"
DEFAULT_WEBHOOK_USERNAME = "Cwl monitor"
DEFAULT_WEBHOOK_AVATAR_URL = "https://bivis.pl/wp-content/uploads/2025/09/Kamizelki.jpg"
DISCORD_EMBED_MAX_FIELDS = 25
DISCORD_EMBED_MAX_CHARS = 6000
CHART_HISTORY_SECONDS = 43200
CHART_MAX_POINTS = 720
CHART_IMAGE_WIDTH = 2000
CHART_IMAGE_HEIGHT = 2000
CHART_MARGIN_LEFT = 110
CHART_MARGIN_RIGHT = 50
CHART_MARGIN_TOP = 90
CHART_MARGIN_BOTTOM = 250
CHART_X_LABEL_EVERY_MINUTES = 30
CHART_X_TICK_EVERY_MINUTES = 5
CHART_VERTICAL_GRID_EVERY_MINUTES = 30
CHART_SMOOTHING_ITERATIONS = 1
CHART_SERIES_COLORS = [
    (255, 107, 107),
    (255, 159, 67),
    (254, 202, 87),
    (29, 209, 161),
    (72, 219, 251),
    (84, 160, 255),
    (95, 39, 205),
    (224, 86, 253),
    (16, 172, 132),
    (0, 210, 211),
    (87, 101, 116),
    (238, 82, 83),
]


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip().casefold()


def _last_sunday(year: int, month: int) -> date:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() + 1) % 7)


def poland_now() -> datetime:
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Europe/Warsaw"))
        except Exception:
            pass

    now_utc = datetime.now(timezone.utc)
    year = now_utc.year
    dst_start = datetime.combine(_last_sunday(year, 3), dt_time(1, 0), tzinfo=timezone.utc)
    dst_end = datetime.combine(_last_sunday(year, 10), dt_time(1, 0), tzinfo=timezone.utc)
    offset = timedelta(hours=2) if dst_start <= now_utc < dst_end else timedelta(hours=1)
    return now_utc + offset


def poland_datetime_from_utc(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)

    if ZoneInfo is not None:
        try:
            return dt_utc.astimezone(ZoneInfo("Europe/Warsaw"))
        except Exception:
            pass

    year = dt_utc.year
    dst_start = datetime.combine(_last_sunday(year, 3), dt_time(1, 0), tzinfo=timezone.utc)
    dst_end = datetime.combine(_last_sunday(year, 10), dt_time(1, 0), tzinfo=timezone.utc)
    offset = timedelta(hours=2) if dst_start <= dt_utc < dst_end else timedelta(hours=1)
    return (dt_utc + offset).replace(tzinfo=timezone(offset))


def utc_now() -> str:
    return poland_now().strftime("%Y-%m-%d %H:%M:%S")


def load_config(config_path: Path) -> Dict:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Brak pliku konfiguracyjnego: {config_path}. "
            "Skopiuj config.example.json do config.json i uzupelnij wartosci."
        )

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required = ["world", "guild_ids"]
    for key in required:
        if key not in config:
            raise ValueError(f"Brak wymaganego pola w configu: {key}")

    if not isinstance(config["guild_ids"], list) or not config["guild_ids"]:
        raise ValueError("guild_ids musi byc niepusta lista ID klanow")

    config.setdefault("poll_seconds", 60)
    config.setdefault("guild_refresh_seconds", 3600)
    config.setdefault("request_timeout", 20)
    config.setdefault("state_file", "state.json")
    config.setdefault("notify_on_startup", True)
    config.setdefault("output_mode", "terminal")
    config.setdefault("webhook_url", "")
    config.setdefault("webhook_username", DEFAULT_WEBHOOK_USERNAME)
    config.setdefault("webhook_avatar_url", DEFAULT_WEBHOOK_AVATAR_URL)
    config.setdefault("guild_chart_groups", [])
    config.setdefault(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MargonemGuildMonitor/1.0",
    )

    output_mode = str(config["output_mode"]).strip().lower()
    if output_mode not in {"terminal", "discord", "both"}:
        raise ValueError("output_mode musi byc jednym z: terminal, discord, both")

    if output_mode in {"discord", "both"} and not str(config["webhook_url"]).strip():
        raise ValueError("Dla output_mode=discord/both wymagane jest webhook_url")

    if int(config["poll_seconds"]) < 30:
        config["poll_seconds"] = 30

    if int(config["guild_refresh_seconds"]) <= 0:
        raise ValueError("guild_refresh_seconds musi byc > 0")

    raw_groups = config.get("guild_chart_groups", [])
    if raw_groups is None:
        raw_groups = []
    if not isinstance(raw_groups, list):
        raise ValueError("guild_chart_groups musi byc lista grup")

    configured_guild_ids = {int(gid) for gid in config["guild_ids"]}
    validated_groups: List[Dict[str, object]] = []
    for index, group in enumerate(raw_groups, start=1):
        if not isinstance(group, dict):
            raise ValueError(f"guild_chart_groups[{index}] musi byc obiektem")

        group_name = str(group.get("name") or "").strip()
        if not group_name:
            raise ValueError(f"guild_chart_groups[{index}].name nie moze byc puste")

        group_ids_raw = group.get("guild_ids")
        if not isinstance(group_ids_raw, list) or not group_ids_raw:
            raise ValueError(f"guild_chart_groups[{index}].guild_ids musi byc niepusta lista")

        seen: Set[int] = set()
        group_ids: List[int] = []
        for gid_raw in group_ids_raw:
            gid = int(gid_raw)
            if gid not in configured_guild_ids:
                raise ValueError(
                    f"guild_chart_groups[{index}] zawiera nieznane guild_id={gid}. "
                    "Dodaj je tez do guild_ids."
                )
            if gid in seen:
                continue
            seen.add(gid)
            group_ids.append(gid)

        validated_groups.append(
            {
                "name": group_name,
                "guild_ids": group_ids,
            }
        )

    config["guild_chart_groups"] = validated_groups

    return config


def parse_retry_after_seconds(response: requests.Response, fallback_seconds: float) -> float:
    retry_after_header = response.headers.get("Retry-After")
    if retry_after_header:
        try:
            return max(float(retry_after_header), 0.5)
        except ValueError:
            pass

    try:
        data = response.json()
        if isinstance(data, dict) and "retry_after" in data:
            return max(float(data["retry_after"]), 0.5)
    except Exception:
        pass

    return max(fallback_seconds, 0.5)


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    timeout: int,
    payload: Optional[Dict] = None,
    data: Optional[Dict[str, str]] = None,
    files: Optional[Dict[str, object]] = None,
    max_attempts: int = 4,
) -> requests.Response:
    last_response: Optional[requests.Response] = None

    for attempt in range(1, max_attempts + 1):
        response = session.request(
            method=method,
            url=url,
            json=payload,
            data=data,
            files=files,
            timeout=timeout,
        )
        last_response = response

        if response.status_code != 429:
            return response

        if attempt >= max_attempts:
            return response

        wait_seconds = parse_retry_after_seconds(
            response,
            fallback_seconds=min(2 ** attempt, 30),
        )
        logging.warning(
            "HTTP 429 dla %s %s, proba %s/%s. Czekam %.1fs i ponawiam.",
            method,
            url,
            attempt,
            max_attempts,
            wait_seconds,
        )
        time.sleep(wait_seconds)

    if last_response is None:
        raise RuntimeError("Brak odpowiedzi HTTP")
    return last_response


def fetch_html(session: requests.Session, url: str, timeout: int) -> str:
    response = request_with_retry(
        session=session,
        method="GET",
        url=url,
        timeout=timeout,
        payload=None,
        max_attempts=4,
    )
    response.raise_for_status()
    if not response.encoding:
        response.encoding = "utf-8"
    return response.text


def parse_guild_members(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    members: Set[str] = set()

    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        name = cells[1].get_text(" ", strip=True)
        if not name:
            continue
        members.add(re.sub(r"\s+", " ", name).strip())

    return members


def parse_guild_name(html: str, guild_id: int) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title:
        raw_title = soup.title.get_text(" ", strip=True)
        if raw_title:
            return raw_title.split(" - ", 1)[0].strip()
    return f"Klan {guild_id}"


def parse_online_names_for_world(html: str, world: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    popup_selector = f".news-container.no-footer.{world.lower()}-popup"
    popup = soup.select_one(popup_selector)
    if popup is None:
        raise ValueError(
            f"Nie znaleziono popupa dla swiata '{world}'. "
            "Sprawdz nazwe swiata w configu."
        )

    names: Set[str] = set()
    for anchor in popup.select(".news-body a"):
        name = anchor.get_text(" ", strip=True)
        if not name:
            continue
        names.add(re.sub(r"\s+", " ", name).strip())

    return names


def load_state(state_path: Path) -> Dict[str, object]:
    if not state_path.exists():
        return {
            "online_norm": set(),
            "webhook_stats_message_id": None,
            "webhook_group_message_ids": {},
            "webhook_nicks_message_id": None,
            "webhook_url": None,
            "webhook_username": None,
            "webhook_avatar_url": None,
            "history": [],
        }

    try:
        with state_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {
            "online_norm": set(),
            "webhook_stats_message_id": None,
            "webhook_group_message_ids": {},
            "webhook_nicks_message_id": None,
            "webhook_url": None,
            "webhook_username": None,
            "webhook_avatar_url": None,
            "history": [],
        }

    raw_group_message_ids = raw.get("webhook_group_message_ids")
    if not isinstance(raw_group_message_ids, dict):
        raw_group_message_ids = raw.get("webhook_guild_message_ids", {})
    if not isinstance(raw_group_message_ids, dict):
        raw_group_message_ids = {}

    return {
        "online_norm": set(raw.get("online_norm", [])),
        "webhook_stats_message_id": raw.get("webhook_stats_message_id") or raw.get("webhook_message_id"),
        "webhook_group_message_ids": {
            str(group_key): str(message_id)
            for group_key, message_id in raw_group_message_ids.items()
            if message_id
        },
        "webhook_nicks_message_id": raw.get("webhook_nicks_message_id"),
        "webhook_url": raw.get("webhook_url"),
        "webhook_username": raw.get("webhook_username"),
        "webhook_avatar_url": raw.get("webhook_avatar_url"),
        "history": raw.get("history", []),
    }


def save_state(
    state_path: Path,
    online_norm: Set[str],
    webhook_stats_message_id: Optional[str],
    webhook_group_message_ids: Dict[str, str],
    webhook_nicks_message_id: Optional[str],
    webhook_url: Optional[str],
    webhook_username: Optional[str],
    webhook_avatar_url: Optional[str],
    history: List[Dict[str, object]],
) -> None:
    payload = {
        "updated_at": utc_now(),
        "online_norm": sorted(online_norm),
        "webhook_stats_message_id": webhook_stats_message_id,
        "webhook_group_message_ids": webhook_group_message_ids,
        "webhook_nicks_message_id": webhook_nicks_message_id,
        "webhook_url": webhook_url,
        "webhook_username": webhook_username,
        "webhook_avatar_url": webhook_avatar_url,
        "history": history,
    }
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def to_norm_map(names: Iterable[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name in names:
        out[normalize_name(name)] = name
    return out


def format_name_list(names: List[str], limit: int = 25) -> str:
    if not names:
        return "-"

    if len(names) <= limit:
        return ", ".join(names)

    shown = ", ".join(names[:limit])
    return f"{shown} ... (+{len(names) - limit} wiecej)"


def build_chart_window_label(points: int) -> str:
    if points <= 1:
        return "ostatnia probka"
    if points % 60 == 0:
        hours = points // 60
        return f"ostatnie {hours}h"
    return f"ostatnie {points} minut"


def format_chart_timestamp(ts: int) -> str:
    return poland_datetime_from_utc(datetime.fromtimestamp(ts, tz=timezone.utc)).strftime("%H:%M")


def load_chart_font(size: int) -> ImageFont.ImageFont:
    for font_name in ["DejaVuSans.ttf", "arial.ttf"]:
        try:
            return ImageFont.truetype(font_name, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def load_chart_bold_font(size: int) -> ImageFont.ImageFont:
    for font_name in ["DejaVuSans-Bold.ttf", "arialbd.ttf"]:
        try:
            return ImageFont.truetype(font_name, size=size)
        except OSError:
            continue
    return load_chart_font(size)


def draw_rotated_label(
    image: Image.Image,
    text: str,
    x: int,
    y: int,
    font: ImageFont.ImageFont,
    fill: Tuple[int, int, int],
) -> None:
    probe = Image.new("RGBA", (1, 1), (0, 0, 0, 0))
    probe_draw = ImageDraw.Draw(probe)
    bbox = probe_draw.textbbox((0, 0), text, font=font)
    width = max(1, bbox[2] - bbox[0])
    height = max(1, bbox[3] - bbox[1])

    label = Image.new("RGBA", (width + 2, height + 2), (0, 0, 0, 0))
    label_draw = ImageDraw.Draw(label)
    label_draw.text((1, 1), text, fill=fill, font=font)
    rotated = label.rotate(90, expand=True)
    image.alpha_composite(rotated, (x, y))


def extract_guild_points(history: List[Dict[str, object]], guild_id: int) -> List[Tuple[int, int]]:
    points: List[Tuple[int, int]] = []
    guild_key = str(guild_id)
    for item in history:
        ts_raw = item.get("ts")
        if not isinstance(ts_raw, int):
            continue
        guild_counts = item.get("guild_counts")
        if not isinstance(guild_counts, dict):
            continue
        if guild_key not in guild_counts:
            continue
        try:
            points.append((ts_raw, int(guild_counts[guild_key])))
        except (TypeError, ValueError):
            continue
    return points


def extract_group_points(history: List[Dict[str, object]], guild_ids: List[int]) -> List[Tuple[int, int]]:
    points: List[Tuple[int, int]] = []
    guild_keys = [str(guild_id) for guild_id in guild_ids]

    for item in history:
        ts_raw = item.get("ts")
        if not isinstance(ts_raw, int):
            continue
        guild_counts = item.get("guild_counts")
        if not isinstance(guild_counts, dict):
            continue

        total = 0
        has_any = False
        for guild_key in guild_keys:
            if guild_key not in guild_counts:
                continue
            has_any = True
            try:
                total += int(guild_counts[guild_key])
            except (TypeError, ValueError):
                continue

        if has_any:
            points.append((ts_raw, total))

    return points


def build_group_series(
    history: List[Dict[str, object]],
    group_guilds: List[Dict[str, object]],
    timestamps: List[int],
) -> List[Dict[str, object]]:
    if not timestamps:
        return []

    series: List[Dict[str, object]] = []
    for guild in group_guilds:
        guild_id = int(guild["id"])
        guild_points_map = {
            ts: value
            for ts, value in extract_guild_points(history, guild_id)
        }
        points = [(ts, int(guild_points_map.get(ts, 0))) for ts in timestamps]
        series.append(
            {
                "id": guild_id,
                "name": str(guild["name"]),
                "current_online": int(guild.get("online_count", 0)),
                "points": points,
            }
        )

    return series


def build_chart_groups(guild_ids: List[int], group_config: List[Dict[str, object]]) -> List[Dict[str, object]]:
    groups: List[Dict[str, object]] = []

    if group_config:
        for index, group in enumerate(group_config, start=1):
            groups.append(
                {
                    "key": f"group:{index}",
                    "name": str(group["name"]),
                    "guild_ids": [int(gid) for gid in group["guild_ids"]],
                }
            )
        return groups

    for guild_id in guild_ids:
        groups.append(
            {
                "key": f"guild:{guild_id}",
                "name": None,
                "guild_ids": [int(guild_id)],
            }
        )

    return groups


def build_group_row(cycle_data: Dict, group: Dict[str, object]) -> Dict[str, object]:
    guild_ids = [int(gid) for gid in group["guild_ids"]]
    guild_lookup = {
        int(guild["id"]): guild
        for guild in cycle_data["guild_breakdown"]
    }

    present_guilds = [guild_lookup[gid] for gid in guild_ids if gid in guild_lookup]
    group_name = str(group.get("name") or "").strip()
    if not group_name:
        if len(present_guilds) == 1:
            group_name = str(present_guilds[0]["name"])
        elif len(guild_ids) == 1:
            group_name = f"Klan {guild_ids[0]}"
        else:
            group_name = f"Grupa ({len(guild_ids)} klanow)"

    return {
        "key": str(group["key"]),
        "name": group_name,
        "guild_ids": guild_ids,
        "guild_names": [str(guild["name"]) for guild in present_guilds],
        "guild_rows": present_guilds,
        "members_count": sum(int(guild.get("members_count", 0)) for guild in present_guilds),
        "online_count": sum(int(guild.get("online_count", 0)) for guild in present_guilds),
        "delta_10m": sum(int(guild.get("delta_10m", 0)) for guild in present_guilds),
    }


def smooth_polyline(points: List[Tuple[float, float]], iterations: int = 2) -> List[Tuple[float, float]]:
    if len(points) < 3:
        return points

    smoothed = points[:]
    for _ in range(max(0, iterations)):
        if len(smoothed) < 3:
            break

        next_points: List[Tuple[float, float]] = [smoothed[0]]
        for i in range(len(smoothed) - 1):
            x1, y1 = smoothed[i]
            x2, y2 = smoothed[i + 1]
            q = (0.75 * x1 + 0.25 * x2, 0.75 * y1 + 0.25 * y2)
            r = (0.25 * x1 + 0.75 * x2, 0.25 * y1 + 0.75 * y2)
            next_points.extend([q, r])
        next_points.append(smoothed[-1])
        smoothed = next_points

    return smoothed


def compute_dynamic_chart_max(members_count: int, observed_peak: int) -> Tuple[int, int]:
    members = max(0, int(members_count))
    peak = max(0, int(observed_peak))

    # More dynamic scaling: lower baseline and let the current peak drive the scale more strongly.
    baseline_floor = max(3, (members * 8 + 99) // 100)
    dynamic_headroom = max(2, (members * 2 + 99) // 100, (peak * 12 + 99) // 100)
    raw_max = max(1, baseline_floor, peak + dynamic_headroom)

    if raw_max <= 20:
        bucket = 1
    elif raw_max <= 50:
        bucket = 2
    elif raw_max <= 120:
        bucket = 3
    elif raw_max <= 240:
        bucket = 5
    elif raw_max <= 300:
        bucket = 10
    else:
        bucket = 20

    rounded_max = ((raw_max + bucket - 1) // bucket) * bucket

    if members > 0:
        rounded_max = min(rounded_max, members)

    return max(1, rounded_max), baseline_floor


def render_guild_chart_png(
    guild_name: str,
    points: List[Tuple[int, int]],
    current_online: int,
    members_count: int,
    guild_series: Optional[List[Dict[str, object]]] = None,
) -> bytes:
    width = CHART_IMAGE_WIDTH
    height = CHART_IMAGE_HEIGHT
    background = (15, 18, 24)
    chart_panel = (22, 27, 36)
    grid = (58, 67, 82)
    axis = (158, 171, 189)
    total_line_color = (72, 202, 228)
    total_line_glow = (72, 202, 228, 80)
    current_dot_color = (245, 251, 255)
    text_color = (245, 248, 252)
    muted_text = (197, 206, 219)
    timestamp_text = (255, 255, 255)

    image = Image.new("RGBA", (width, height), background)
    draw = ImageDraw.Draw(image, "RGBA")
    font = load_chart_font(18)
    small_font = load_chart_font(16)
    title_font = load_chart_font(24)
    timestamp_font = load_chart_bold_font(12)

    clamped_points = [
        (int(ts), max(0, int(value)))
        for ts, value in points[-CHART_MAX_POINTS:]
    ]
    observed_peak = max([current_online] + [value for _, value in clamped_points]) if clamped_points else max(0, current_online)
    max_value, baseline_floor = compute_dynamic_chart_max(
        members_count=members_count,
        observed_peak=observed_peak,
    )

    clamped_points = [
        (ts, min(value, max_value))
        for ts, value in clamped_points
    ]

    clamped_series: List[Dict[str, object]] = []
    for series_index, series in enumerate(guild_series or []):
        raw_points = series.get("points", [])
        if not isinstance(raw_points, list):
            continue
        series_points = [
            (int(ts), min(max(0, int(value)), max_value))
            for ts, value in raw_points[-len(clamped_points):]
        ]
        if not series_points:
            continue
        clamped_series.append(
            {
                "name": str(series.get("name") or f"Klan {series_index + 1}"),
                "current_online": int(series.get("current_online", 0)),
                "color": CHART_SERIES_COLORS[series_index % len(CHART_SERIES_COLORS)],
                "points": series_points,
            }
        )

    left = CHART_MARGIN_LEFT
    right = width - CHART_MARGIN_RIGHT
    top = CHART_MARGIN_TOP
    bottom = height - CHART_MARGIN_BOTTOM
    chart_width = right - left
    chart_height = bottom - top

    draw.rounded_rectangle((12, 12, width - 12, height - 12), radius=24, outline=(44, 51, 64), width=2)
    draw.rounded_rectangle((left, top, right, bottom), radius=14, fill=chart_panel, outline=(40, 48, 60), width=1)
    draw.text((left, 22), f"{guild_name} - live online", fill=text_color, font=title_font)
    draw.text(
        (left, 44),
        f"Teraz: {current_online}/{members_count}  |  Okno: {build_chart_window_label(len(clamped_points))}",
        fill=muted_text,
        font=small_font,
    )

    if not clamped_points:
        draw.text((left, top + chart_height // 2), "Brak danych do wykresu", fill=muted_text, font=font)
        buffer = BytesIO()
        image.convert("RGB").save(buffer, format="PNG")
        return buffer.getvalue()

    series = [value for _, value in clamped_points]
    tick_values = list(range(0, max_value + 1, 2))
    if tick_values[-1] != max_value:
        tick_values.append(max_value)

    for tick in tick_values:
        ratio = tick / max_value if max_value else 0
        y = bottom - ratio * chart_height
        draw.line((left, y, right, y), fill=grid, width=1)
        label = str(tick)
        label_width = draw.textlength(label, font=small_font)
        draw.text((left - label_width - 12, y - 8), label, fill=muted_text, font=small_font)

    draw.line((left, top, left, bottom), fill=axis, width=2)
    draw.line((left, bottom, right, bottom), fill=axis, width=2)

    x_step = chart_width / max(len(series) - 1, 1)

    for index in range(len(series)):
        if index % CHART_VERTICAL_GRID_EVERY_MINUTES != 0:
            continue
        x = left + index * x_step
        draw.line((x, top, x, bottom), fill=(46, 54, 68), width=1)

    plot_points: List[Tuple[float, float]] = []
    for index, value in enumerate(series):
        x = left + index * x_step
        ratio = value / max_value if max_value else 0
        y = bottom - ratio * chart_height
        plot_points.append((x, y))

    for series in clamped_series:
        series_values = [value for _, value in series["points"]]
        series_plot_points: List[Tuple[float, float]] = []
        for index, value in enumerate(series_values):
            x = left + index * x_step
            ratio = value / max_value if max_value else 0
            y = bottom - ratio * chart_height
            series_plot_points.append((x, y))

        if len(series_plot_points) < 2:
            continue

        int_points = [(int(x), int(y)) for x, y in series_plot_points]
        color = series["color"]
        draw.line(int_points, fill=(color[0], color[1], color[2], 95), width=3, joint="curve")
        draw.line(int_points, fill=color, width=2, joint="curve")

    if len(plot_points) >= 2:
        int_points = [(int(x), int(y)) for x, y in plot_points]
        draw.line(int_points, fill=total_line_glow, width=6, joint="curve")
        draw.line(int_points, fill=total_line_color, width=3, joint="curve")

    if plot_points:
        last_x, last_y = plot_points[-1]
        draw.ellipse((last_x - 5, last_y - 5, last_x + 5, last_y + 5), fill=current_dot_color, outline=total_line_color, width=2)

    if len(clamped_points) >= 2:
        peak_label_indices: List[int] = []
        window_size = max(1, CHART_X_LABEL_EVERY_MINUTES)

        for window_start in range(0, len(clamped_points), window_size):
            window_end = min(window_start + window_size, len(clamped_points))
            window_values = [value for _, value in clamped_points[window_start:window_end]]
            if not window_values:
                continue

            local_peak_value = max(window_values)
            local_peak_offset = window_values.index(local_peak_value)
            peak_label_indices.append(window_start + local_peak_offset)

        for index in peak_label_indices:
            ts, _ = clamped_points[index]
            x = left + index * x_step
            y = plot_points[index][1]
            timestamp_label = format_chart_timestamp(ts)
            label_width = draw.textlength(timestamp_label, font=timestamp_font)
            label_height = 16

            label_x = int(max(left, min(right - label_width - 10, x - (label_width / 2) - 6)))
            place_above = y > top + (chart_height * 0.32)
            label_y = int(max(top + 6, y - 24)) if place_above else int(min(bottom - 22, y + 8))

            background_box = (
                label_x,
                label_y,
                int(label_x + label_width + 12),
                int(label_y + label_height + 4),
            )
            draw.rounded_rectangle(background_box, radius=4, fill=(11, 14, 20, 220), outline=(70, 79, 93), width=1)
            draw.text((label_x + 6, label_y + 1), timestamp_label, fill=timestamp_text, font=timestamp_font)

    top_label = f"Skala max: {max_value} (bazowy zakres: {baseline_floor})"
    bottom_label = "0"
    draw.text((left + 10, top - 22), top_label, fill=muted_text, font=small_font)
    draw.text((left + 10, bottom + 10), bottom_label, fill=muted_text, font=small_font)

    online_ratio = (current_online / members_count * 100) if members_count else 0
    status_text = f"Aktualnie: {current_online}/{members_count} ({online_ratio:.0f}%)"
    status_width = draw.textlength(status_text, font=small_font)
    draw.text((right - status_width, top - 22), status_text, fill=muted_text, font=small_font)

    legend_items: List[Tuple[str, Tuple[int, int, int]]] = [("Suma grupy", total_line_color)]
    for series in clamped_series:
        legend_items.append(
            (
                f"{series['name']} ({series['current_online']})",
                series["color"],
            )
        )

    legend_x = left + 10
    legend_y = top + 8
    legend_col_width = max(280, int(chart_width * 0.28))
    legend_row_height = 20
    max_legend_rows = max(1, (chart_height // 3) // legend_row_height)
    for index, (legend_name, legend_color) in enumerate(legend_items):
        col = index // max_legend_rows
        row = index % max_legend_rows
        x = legend_x + col * legend_col_width
        y = legend_y + row * legend_row_height

        if x + 240 > right:
            break

        draw.line((x, y + 7, x + 20, y + 7), fill=legend_color, width=3)
        draw.text(
            (x + 28, y),
            truncate_text(legend_name, 26),
            fill=muted_text,
            font=timestamp_font,
        )

    if len(clamped_points) >= 2:
        x_label_base = bottom + 22
        draw.rectangle((left, bottom + 12, right, height - 24), fill=(24, 29, 38, 195))
        for index, (ts, _) in enumerate(clamped_points):
            is_label_tick = index % CHART_X_LABEL_EVERY_MINUTES == 0
            is_minor_tick = index % CHART_X_TICK_EVERY_MINUTES == 0
            is_last = index == len(clamped_points) - 1

            if not is_minor_tick and not is_label_tick and not is_last:
                continue

            x = left + index * x_step
            tick_height = 12 if (is_label_tick or is_last) else 6
            draw.line((x, bottom, x, bottom + tick_height), fill=axis, width=1)

            if is_label_tick or is_last:
                timestamp_label = format_chart_timestamp(ts)
                label_width = draw.textlength(timestamp_label, font=small_font)
                label_y = x_label_base
                if index == 0:
                    label_x = left
                elif is_last:
                    label_x = right - label_width
                else:
                    label_x = x - (label_width / 2)

                draw_rotated_label(
                    image=image,
                    text=timestamp_label,
                    x=int(label_x),
                    y=int(label_y),
                    font=timestamp_font,
                    fill=timestamp_text,
                )

    buffer = BytesIO()
    image.convert("RGB").save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def request_with_rate_limit(
    session: requests.Session,
    method: str,
    url: str,
    payload: Dict,
    data: Optional[Dict[str, str]] = None,
    files: Optional[Dict[str, object]] = None,
    timeout: int = 20,
) -> requests.Response:
    return request_with_retry(
        session=session,
        method=method,
        url=url,
        timeout=timeout,
        payload=payload,
        data=data,
        files=files,
        max_attempts=4,
    )


def truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def split_names_for_discord_spoiler(names: List[str], max_chunk_len: int = 980) -> List[str]:
    """Split full nick list into chunks that fit Discord field limits."""
    if not names:
        return ["brak"]

    chunks: List[str] = []
    current: List[str] = []
    current_len = 0

    for name in names:
        part_len = len(name) if not current else len(name) + 2

        if part_len > max_chunk_len:
            if current:
                chunks.append(", ".join(current))
                current = []
                current_len = 0
            chunks.append(name[:max_chunk_len])
            continue

        if current_len + part_len > max_chunk_len:
            chunks.append(", ".join(current))
            current = [name]
            current_len = len(name)
            continue

        current.append(name)
        current_len += part_len

    if current:
        chunks.append(", ".join(current))

    return chunks


def embed_char_count(title: str, description: str, fields: List[Dict[str, object]]) -> int:
    total = len(title) + len(description)
    for field in fields:
        total += len(str(field.get("name", "")))
        total += len(str(field.get("value", "")))
    return total


def apply_discord_embed_limits(
    title: str,
    description: str,
    fields: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    kept_fields: List[Dict[str, object]] = []
    omitted = 0

    for field in fields:
        if len(kept_fields) >= DISCORD_EMBED_MAX_FIELDS:
            omitted += 1
            continue

        trial = kept_fields + [field]
        if embed_char_count(title, description, trial) > DISCORD_EMBED_MAX_CHARS:
            omitted += 1
            continue

        kept_fields = trial

    if omitted > 0:
        note_field: Dict[str, object] = {
            "name": "Dodatkowe dane",
            "value": f"Pominieto {omitted} pol ze wzgledu na limity Discord (max 25 pol / 6000 znakow).",
            "inline": False,
        }

        # Reserve one slot for the note if needed.
        while len(kept_fields) >= DISCORD_EMBED_MAX_FIELDS:
            kept_fields.pop()

        # Ensure note fits both field count and total char budget.
        while kept_fields and embed_char_count(title, description, kept_fields + [note_field]) > DISCORD_EMBED_MAX_CHARS:
            kept_fields.pop()

        if embed_char_count(title, description, kept_fields + [note_field]) <= DISCORD_EMBED_MAX_CHARS:
            kept_fields.append(note_field)

    return kept_fields


def prune_history(history: List[Dict[str, object]], now_ts: int, window_seconds: int) -> List[Dict[str, object]]:
    cutoff = now_ts - window_seconds
    pruned: List[Dict[str, object]] = []
    for item in history:
        ts_raw = item.get("ts")
        if not isinstance(ts_raw, int):
            continue
        if ts_raw >= cutoff:
            pruned.append(item)
    return pruned


def get_guild_online_map(cycle_data: Dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for guild in cycle_data["guild_breakdown"]:
        out[str(guild["id"])] = int(guild["online_count"])
    return out


def enrich_with_10m_delta(
    cycle_data: Dict,
    history: List[Dict[str, object]],
    now_ts: int,
) -> Tuple[Dict, List[Dict[str, object]]]:
    history = prune_history(history, now_ts=now_ts, window_seconds=CHART_HISTORY_SECONDS)
    recent_history = prune_history(history, now_ts=now_ts, window_seconds=600)
    baseline = recent_history[0] if recent_history else None

    baseline_total = int(baseline.get("online_count", cycle_data["online_count"])) if baseline else int(cycle_data["online_count"])
    cycle_data["delta_10m"] = int(cycle_data["online_count"]) - baseline_total

    baseline_guild_map = baseline.get("guild_counts", {}) if isinstance(baseline, dict) else {}
    if not isinstance(baseline_guild_map, dict):
        baseline_guild_map = {}

    for guild in cycle_data["guild_breakdown"]:
        gid = str(guild["id"])
        baseline_count = int(baseline_guild_map.get(gid, guild["online_count"]))
        guild["delta_10m"] = int(guild["online_count"]) - baseline_count

    history.append(
        {
            "ts": now_ts,
            "online_count": int(cycle_data["online_count"]),
            "guild_counts": get_guild_online_map(cycle_data),
        }
    )
    history = prune_history(history, now_ts=now_ts, window_seconds=CHART_HISTORY_SECONDS)
    return cycle_data, history


def trend_style(delta_10m: int) -> Tuple[int, str, str]:
    if delta_10m >= 20:
        return 0xE74C3C, "Zagrozenie", "🔴"
    if delta_10m >= 3:
        return 0xF39C12, "Wzrost", "🟠"
    if delta_10m > 0:
        return 0xF1C40F, "Lekki wzrost", "🟡"
    if delta_10m < 0:
        return 0x2ECC71, "Spadek", "🟢"
    return 0x3498DB, "Stabilnie", "🔵"


def guild_delta_marker(delta_10m: int) -> str:
    if delta_10m >= 6:
        return "🔴"
    if delta_10m > 0:
        return "🟠"
    if delta_10m < 0:
        return "🟢"
    return "🔵"


def build_discord_stats_payload(world: str, cycle_data: Dict, avatar_url: str) -> Dict:
    now_iso = poland_now().replace(microsecond=0).isoformat()
    title = f"Cwel Patrol | statystyki"
    delta_10m = int(cycle_data.get("delta_10m", 0))
    color, trend_label, trend_icon = trend_style(delta_10m)
    delta_sign = "+" if delta_10m > 0 else ""
    description = f"Zaktualizowano: {utc_now()}"

    embed: Dict[str, object] = {
        "title": truncate_text(title, 256),
        "description": truncate_text(description, 4096),
        "color": color,
        "timestamp": now_iso,
        "fields": [],
    }

    fields: List[Dict[str, object]] = [
        {
            "name": "Sledzonych",
            "value": str(cycle_data["tracked_members_count"]),
            "inline": True,
        },
        {
            "name": "Online teraz",
            "value": str(cycle_data["online_count"]),
            "inline": True,
        },
        {
            "name": "Zmiana 10m",
            "value": f"{trend_icon} {delta_sign}{delta_10m} ({trend_label})",
            "inline": True,
        },
    ]

    guild_rows = cycle_data["guild_breakdown"]

    if not guild_rows:
        fields.append(
            {
                "name": "Brak danych klanow",
                "value": "Sprawdz konfiguracje guild_ids.",
                "inline": False,
            }
        )
    else:
        guild_inline_fields: List[Dict[str, object]] = []
        for guild in guild_rows:
            guild_delta = int(guild.get("delta_10m", 0))
            guild_sign = "+" if guild_delta > 0 else ""
            marker = guild_delta_marker(guild_delta)
            guild_inline_fields.append(
                {
                    "name": truncate_text(f"{marker} {guild['name']}", 256),
                    "value": truncate_text(
                        (
                            f"Online: **{guild['online_count']}/{guild['members_count']}**\n"
                            f"Zmiana 10m: **{guild_sign}{guild_delta}**"
                        ),
                        1024,
                    ),
                    "inline": True,
                }
            )

        # Keep a stable 3-column grid in Discord by padding the last row.
        while len(guild_inline_fields) % 3 != 0:
            guild_inline_fields.append(
                {
                    "name": "\u200b",
                    "value": "\u200b",
                    "inline": True,
                }
            )

        fields.extend(guild_inline_fields)

    embed["fields"] = apply_discord_embed_limits(
        title=str(embed["title"]),
        description=str(embed["description"]),
        fields=fields,
    )

    return {
        "content": "",
        "embeds": [embed],
        "allowed_mentions": {"parse": []},
    }


def build_discord_group_chart_payload(cycle_data: Dict, history: List[Dict[str, object]], group: Dict[str, object], avatar_url: str) -> Dict:
    now_iso = poland_now().replace(microsecond=0).isoformat()
    group_row = build_group_row(cycle_data, group)
    points = extract_group_points(history, group_row["guild_ids"])
    if len(points) > CHART_MAX_POINTS:
        points = points[-CHART_MAX_POINTS:]
    timestamps = [ts for ts, _ in points]
    guild_series = build_group_series(
        history=history,
        group_guilds=list(group_row.get("guild_rows", [])),
        timestamps=timestamps,
    )

    title = f"Cwel Patrol | {group_row['name']} | live"
    description = f"Zaktualizowano: {utc_now()}"
    current_online = int(group_row.get("online_count", 0))
    members_count = int(group_row.get("members_count", 0))
    delta_10m = int(group_row.get("delta_10m", 0))
    delta_sign = "+" if delta_10m > 0 else ""
    chart_window = build_chart_window_label(len(points))
    chart_image = render_guild_chart_png(
        guild_name=str(group_row["name"]),
        points=points,
        current_online=current_online,
        members_count=members_count,
        guild_series=guild_series,
    )
    group_key = str(group_row["key"])
    safe_group_key = re.sub(r"[^a-zA-Z0-9_-]+", "-", group_key)
    image_filename = f"group-{safe_group_key}-chart.png"

    embed: Dict[str, object] = {
        "title": truncate_text(title, 256),
        "description": truncate_text(description, 4096),
        "color": 0x5865F2,
        "timestamp": now_iso,
        "fields": [],
    }

    fields: List[Dict[str, object]] = [
        {
            "name": "Online teraz",
            "value": f"**{current_online}/{members_count}**",
            "inline": True,
        },
        {
            "name": "Zmiana 10m",
            "value": f"**{delta_sign}{delta_10m}**",
            "inline": True,
        },
    ]

    fields.append(
        {
            "name": "Okno wykresu",
            "value": chart_window,
            "inline": True,
        }
    )

    embed["image"] = {"url": f"attachment://{image_filename}"}

    embed["fields"] = apply_discord_embed_limits(
        title=str(embed["title"]),
        description=str(embed["description"]),
        fields=fields,
    )

    return {
        "content": "",
        "embeds": [embed],
        "allowed_mentions": {"parse": []},
        "attachment_filename": image_filename,
        "attachment_bytes": chart_image,
    }


def delete_webhook_messages(
    session: requests.Session,
    webhook_url: str,
    message_ids: Iterable[str],
) -> None:
    for message_id in message_ids:
        if not message_id:
            continue
        try:
            delete_webhook_message(session=session, webhook_url=webhook_url, message_id=str(message_id))
        except Exception as exc:
            logging.warning("Nie udalo sie usunac starej wiadomosci webhook: %s", exc)


def upsert_webhook_message(
    session: requests.Session,
    webhook_url: str,
    payload: Dict,
    message_id: Optional[str],
    webhook_username: str,
    webhook_avatar_url: str,
) -> Optional[str]:
    patch_payload = {
        "content": payload.get("content", "")[:1900],
        "embeds": payload.get("embeds", []),
        "allowed_mentions": payload.get("allowed_mentions", {"parse": []}),
    }

    attachment_filename = payload.get("attachment_filename")
    attachment_bytes = payload.get("attachment_bytes")
    files = None
    data = None

    if attachment_filename and attachment_bytes:
        patch_payload["attachments"] = [{"id": 0, "filename": attachment_filename}]
        data = {"payload_json": json.dumps(patch_payload, ensure_ascii=False)}
        files = {
            "files[0]": (attachment_filename, BytesIO(attachment_bytes), "image/png"),
        }
    else:
        data = None
        files = None

    if message_id:
        patch_url = webhook_url.rstrip("/") + f"/messages/{message_id}"
        response = request_with_rate_limit(
            session=session,
            method="PATCH",
            url=patch_url,
            payload=None if files else patch_payload,
            data=data,
            files=files,
        )
        if response.status_code != 404:
            response.raise_for_status()
            return message_id
        logging.warning(
            "Nie znaleziono poprzedniej wiadomosci webhook (%s), tworze nowa.",
            message_id,
        )

    wait_url = webhook_url + ("&" if "?" in webhook_url else "?") + "wait=true"
    post_payload = dict(patch_payload)
    if webhook_username:
        post_payload["username"] = webhook_username

    if files:
        post_payload["attachments"] = [{"id": 0, "filename": attachment_filename}]
        response = request_with_rate_limit(
            session=session,
            method="POST",
            url=wait_url,
            payload=None,
            data={"payload_json": json.dumps(post_payload, ensure_ascii=False)},
            files={
                "files[0]": (attachment_filename, BytesIO(attachment_bytes), "image/png"),
            },
        )
    else:
        response = request_with_rate_limit(
            session=session,
            method="POST",
            url=wait_url,
            payload=post_payload,
        )
    response.raise_for_status()

    try:
        return str(response.json().get("id"))
    except Exception:
        return None


def delete_webhook_message(
    session: requests.Session,
    webhook_url: str,
    message_id: str,
) -> None:
    delete_url = webhook_url.rstrip("/") + f"/messages/{message_id}"
    response = session.request(method="DELETE", url=delete_url, timeout=20)
    if response.status_code == 429:
        logging.warning("Rate limit przy usuwaniu wiadomosci webhook (%s). Pomijam usuniecie.", message_id)
        return
    if response.status_code not in {204, 404}:
        response.raise_for_status()


def sync_webhook_profile(
    session: requests.Session,
    webhook_url: str,
    webhook_username: str,
    webhook_avatar_url: str,
    timeout: int,
) -> None:
    payload: Dict[str, object] = {}

    if webhook_username:
        payload["name"] = webhook_username

    if webhook_avatar_url:
        avatar_response = request_with_retry(
            session=session,
            method="GET",
            url=webhook_avatar_url,
            timeout=timeout,
            payload=None,
            max_attempts=4,
        )
        avatar_response.raise_for_status()
        content_type = avatar_response.headers.get("content-type", "image/jpeg")
        avatar_b64 = base64.b64encode(avatar_response.content).decode("ascii")
        payload["avatar"] = f"data:{content_type};base64,{avatar_b64}"

    if not payload:
        return

    response = request_with_rate_limit(
        session=session,
        method="PATCH",
        url=webhook_url,
        payload=payload,
        timeout=timeout,
    )
    response.raise_for_status()


def publish_terminal(output_mode: str, content: str) -> None:
    if output_mode in {"terminal", "both"}:
        print("\n" + content + "\n", flush=True)


def collect_tracked_members(
    session: requests.Session, world: str, guild_ids: List[int], timeout: int
) -> Tuple[Set[str], Dict[int, Dict[str, object]]]:
    tracked: Set[str] = set()
    guild_data: Dict[int, Dict[str, object]] = {}

    for guild_id in guild_ids:
        url = GUILD_URL_TEMPLATE.format(world=world.lower(), guild_id=guild_id)
        try:
            html = fetch_html(session, url, timeout=timeout)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logging.warning(
                "Pomijam klan %s (HTTP %s): %s",
                guild_id,
                status,
                url,
            )
            continue
        members = parse_guild_members(html)
        guild_name = parse_guild_name(html, guild_id)
        guild_data[guild_id] = {
            "name": guild_name,
            "members": members,
            "size": len(members),
        }
        tracked.update(members)

    return tracked, guild_data


def describe_guild_member_count_changes(
    previous_guild_data: Dict[int, Dict[str, object]],
    current_guild_data: Dict[int, Dict[str, object]],
) -> List[str]:
    lines: List[str] = []

    all_ids = sorted(set(previous_guild_data.keys()) | set(current_guild_data.keys()))
    for guild_id in all_ids:
        prev = previous_guild_data.get(guild_id)
        curr = current_guild_data.get(guild_id)

        if prev is None and curr is not None:
            lines.append(f"- {curr['name']}: nowy klan ({curr['size']} osob)")
            continue

        if prev is not None and curr is None:
            lines.append(f"- {prev['name']}: klan niedostepny")
            continue

        if prev is None or curr is None:
            continue

        prev_size = int(prev["size"])
        curr_size = int(curr["size"])
        diff = curr_size - prev_size
        if diff == 0:
            continue

        sign = "+" if diff > 0 else ""
        lines.append(
            f"- {curr['name']}: {prev_size} -> {curr_size} ({sign}{diff})"
        )

    return lines


def run_cycle(
    session: requests.Session,
    world: str,
    timeout: int,
    previous_online_norm: Set[str],
    tracked_members: Set[str],
    guild_sizes: Dict[int, Dict[str, object]],
) -> Dict:
    tracked_map = to_norm_map(tracked_members)

    stats_html = fetch_html(session, STATS_URL, timeout=timeout)
    online_world_members = parse_online_names_for_world(stats_html, world)
    online_map = to_norm_map(online_world_members)

    current_online_norm = set(tracked_map.keys()) & set(online_map.keys())

    went_online_norm = sorted(current_online_norm - previous_online_norm)
    went_offline_norm = sorted(previous_online_norm - current_online_norm)

    went_online_names = sorted(online_map[n] for n in went_online_norm if n in online_map)
    went_offline_names = sorted(tracked_map[n] for n in went_offline_norm if n in tracked_map)

    tracked_online_names = sorted(online_map[n] for n in current_online_norm if n in online_map)

    guild_breakdown: List[Dict[str, object]] = []
    for guild_id in sorted(guild_sizes.keys()):
        guild_info = guild_sizes[guild_id]
        guild_members = guild_info["members"]
        guild_norm = {normalize_name(name) for name in guild_members}
        guild_online_norm = sorted(guild_norm & set(online_map.keys()))
        guild_online_names = sorted(online_map[n] for n in guild_online_norm if n in online_map)
        guild_breakdown.append(
            {
                "id": guild_id,
                "name": str(guild_info["name"]),
                "members_count": int(guild_info["size"]),
                "online_count": len(guild_online_names),
                "online_names": guild_online_names,
            }
        )

    return {
        "tracked_members_count": len(tracked_members),
        "guild_breakdown": guild_breakdown,
        "online_count": len(tracked_online_names),
        "online_names": tracked_online_names,
        "went_online": went_online_names,
        "went_offline": went_offline_names,
        "current_online_norm": current_online_norm,
    }


def build_startup_message(world: str, cycle_data: Dict) -> str:
    guild_breakdown = ", ".join(
        f"{g['name']}: {g['members_count']}"
        for g in cycle_data["guild_breakdown"]
    )
    return (
        f"[Cwel Patrol] Start {utc_now()}\n"
        f"Swiat: {world}\n"
        f"Klanow: {len(cycle_data['guild_breakdown'])} ({guild_breakdown})\n"
        f"Sledzonych postaci: {cycle_data['tracked_members_count']}\n"
        f"Online teraz: {cycle_data['online_count']}\n"
        f"Wykresy grupowe: aktywne"
    )


def build_delta_message(world: str, cycle_data: Dict) -> str:
    return (
        f"[Cwel Patrol] Zmiana {utc_now()} | swiat: {world}\n"
        f"Online teraz: {cycle_data['online_count']}\n"
        f"Weszli online ({len(cycle_data['went_online'])}): "
        f"{format_name_list(cycle_data['went_online'])}\n"
        f"Zeszli offline ({len(cycle_data['went_offline'])}): "
        f"{format_name_list(cycle_data['went_offline'])}"
    )


def build_status_message(world: str, cycle_data: Dict) -> str:
    lines = [
        f"[Cwel Patrol] Stan {utc_now()} | swiat: {world}",
        f"Sledzonych postaci: {cycle_data['tracked_members_count']}",
        f"Online teraz: {cycle_data['online_count']}",
        "",
        "Podzial na klany:",
    ]

    for guild in cycle_data["guild_breakdown"]:
        lines.append(
            f"- {guild['name']}: {guild['online_count']}/{guild['members_count']} online"
        )

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor aktywnosci postaci Margonem dla wybranych klanow"
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Sciezka do pliku konfiguracyjnego (domyslnie: config.json)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Wykonaj jeden cykl i zakoncz (do testow).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    config_path = Path(args.config)
    config = load_config(config_path)

    world = str(config["world"]).lower()
    guild_ids = [int(gid) for gid in config["guild_ids"]]
    guild_chart_groups = build_chart_groups(
        guild_ids=guild_ids,
        group_config=config.get("guild_chart_groups", []),
    )
    output_mode = str(config["output_mode"]).lower()
    webhook_url = str(config["webhook_url"]).strip()
    webhook_username = str(config.get("webhook_username", DEFAULT_WEBHOOK_USERNAME)).strip()
    webhook_avatar_url = str(config.get("webhook_avatar_url", DEFAULT_WEBHOOK_AVATAR_URL)).strip()
    poll_seconds = int(config["poll_seconds"])
    guild_refresh_seconds = int(config["guild_refresh_seconds"])
    request_timeout = int(config["request_timeout"])
    state_path = Path(config["state_file"])
    notify_on_startup = bool(config["notify_on_startup"])

    session = requests.Session()
    session.headers.update({"User-Agent": config["user_agent"]})

    state = load_state(state_path)
    previous_online_norm = set(state["online_norm"])
    webhook_stats_message_id = state.get("webhook_stats_message_id")
    webhook_group_message_ids = state.get("webhook_group_message_ids", {})
    if not isinstance(webhook_group_message_ids, dict):
        webhook_group_message_ids = {}
    webhook_nicks_message_id = state.get("webhook_nicks_message_id")
    previous_webhook_url = str(state.get("webhook_url") or "").strip()
    previous_webhook_username = str(state.get("webhook_username") or "").strip()
    previous_webhook_avatar_url = str(state.get("webhook_avatar_url") or "").strip()
    history_raw = state.get("history", [])
    history: List[Dict[str, object]] = history_raw if isinstance(history_raw, list) else []
    configured_group_keys = {str(group["key"]) for group in guild_chart_groups}
    webhook_url_changed = previous_webhook_url != webhook_url

    if webhook_url_changed:
        logging.info("Wykryto zmiane webhooka. Resetuje zapisane ID wiadomosci bez kasowania starych.")
        webhook_stats_message_id = None
        webhook_group_message_ids = {}
        webhook_nicks_message_id = None

    tracked_members, guild_sizes = collect_tracked_members(
        session=session,
        world=world,
        guild_ids=guild_ids,
        timeout=request_timeout,
    )
    next_guild_refresh_ts = int(time.time()) + guild_refresh_seconds

    profile_changed = output_mode in {"discord", "both"} and (
        previous_webhook_username != webhook_username
        or previous_webhook_avatar_url != webhook_avatar_url
    )
    if not webhook_url_changed and output_mode in {"discord", "both"} and webhook_nicks_message_id:
        try:
            delete_webhook_message(
                session=session,
                webhook_url=webhook_url,
                message_id=str(webhook_nicks_message_id),
            )
        except Exception as exc:
            logging.warning("Nie udalo sie usunac starej wiadomosci nickow webhook: %s", exc)
        webhook_nicks_message_id = None

    if not webhook_url_changed and profile_changed and (
        webhook_stats_message_id or webhook_group_message_ids
    ):
        logging.info(
            "Wykryto zmiane profilu webhooka (nazwa/avatar). Odtwarzam wiadomosci statusu."
        )
        ids_to_delete = [webhook_stats_message_id]
        ids_to_delete.extend(webhook_group_message_ids.values())
        delete_webhook_messages(
            session=session,
            webhook_url=webhook_url,
            message_ids=ids_to_delete,
        )
        webhook_stats_message_id = None
        webhook_group_message_ids = {}

    if output_mode in {"discord", "both"}:
        try:
            sync_webhook_profile(
                session=session,
                webhook_url=webhook_url,
                webhook_username=webhook_username,
                webhook_avatar_url=webhook_avatar_url,
                timeout=request_timeout,
            )
        except Exception as exc:
            logging.warning("Nie udalo sie ustawic profilu webhooka: %s", exc)

    first_cycle = True

    while True:
        try:
            now_ts = int(time.time())
            if now_ts >= next_guild_refresh_ts:
                try:
                    refreshed_tracked_members, refreshed_guild_sizes = collect_tracked_members(
                        session=session,
                        world=world,
                        guild_ids=guild_ids,
                        timeout=request_timeout,
                    )
                    guild_count_changes = describe_guild_member_count_changes(
                        previous_guild_data=guild_sizes,
                        current_guild_data=refreshed_guild_sizes,
                    )
                    if guild_count_changes:
                        msg = (
                            f"[Margonem monitor] Aktualizacja liczebnosci klanow {utc_now()} | swiat: {world}\n"
                            + "\n".join(guild_count_changes)
                        )
                        publish_terminal(output_mode, msg)
                        logging.info(
                            "Wykryto zmiane liczebnosci klanow: %s",
                            " | ".join(guild_count_changes),
                        )

                    tracked_members = refreshed_tracked_members
                    guild_sizes = refreshed_guild_sizes
                    next_guild_refresh_ts = now_ts + guild_refresh_seconds
                except Exception as exc:
                    logging.warning(
                        "Nie udalo sie odswiezyc listy czlonkow klanow: %s", exc
                    )
                    next_guild_refresh_ts = now_ts + min(60, guild_refresh_seconds)

            cycle_data = run_cycle(
                session=session,
                world=world,
                timeout=request_timeout,
                previous_online_norm=previous_online_norm,
                tracked_members=tracked_members,
                guild_sizes=guild_sizes,
            )
            cycle_data, history = enrich_with_10m_delta(cycle_data, history, now_ts)

            logging.info(
                "Sledzonych: %s | online teraz: %s | +%s / -%s",
                cycle_data["tracked_members_count"],
                cycle_data["online_count"],
                len(cycle_data["went_online"]),
                len(cycle_data["went_offline"]),
            )

            should_send_delta = bool(cycle_data["went_online"] or cycle_data["went_offline"])

            if first_cycle and notify_on_startup:
                publish_terminal(output_mode, build_startup_message(world, cycle_data))
            elif should_send_delta:
                publish_terminal(output_mode, build_delta_message(world, cycle_data))

            if output_mode in {"discord", "both"}:
                discord_stats_payload = build_discord_stats_payload(world, cycle_data, webhook_avatar_url)

                webhook_stats_message_id = upsert_webhook_message(
                    session=session,
                    webhook_url=webhook_url,
                    payload=discord_stats_payload,
                    message_id=str(webhook_stats_message_id) if webhook_stats_message_id else None,
                    webhook_username=webhook_username,
                    webhook_avatar_url=webhook_avatar_url,
                )

                current_group_message_ids: Dict[str, str] = {}
                for group in guild_chart_groups:
                    group_key = str(group["key"])
                    chart_payload = build_discord_group_chart_payload(
                        cycle_data=cycle_data,
                        history=history,
                        group=group,
                        avatar_url=webhook_avatar_url,
                    )
                    message_id = webhook_group_message_ids.get(group_key)
                    updated_message_id = upsert_webhook_message(
                        session=session,
                        webhook_url=webhook_url,
                        payload=chart_payload,
                        message_id=str(message_id) if message_id else None,
                        webhook_username=webhook_username,
                        webhook_avatar_url=webhook_avatar_url,
                    )
                    if updated_message_id:
                        current_group_message_ids[group_key] = str(updated_message_id)

                stale_group_keys = [
                    group_key
                    for group_key in webhook_group_message_ids.keys()
                    if group_key not in configured_group_keys
                ]
                stale_message_ids = [
                    webhook_group_message_ids[group_key]
                    for group_key in stale_group_keys
                    if webhook_group_message_ids.get(group_key)
                ]
                if stale_message_ids:
                    delete_webhook_messages(
                        session=session,
                        webhook_url=webhook_url,
                        message_ids=stale_message_ids,
                    )
                webhook_group_message_ids = {
                    **{
                        group_key: message_id
                        for group_key, message_id in webhook_group_message_ids.items()
                        if group_key in configured_group_keys and group_key not in current_group_message_ids
                    },
                    **current_group_message_ids,
                }

            save_state(
                state_path,
                cycle_data["current_online_norm"],
                str(webhook_stats_message_id) if webhook_stats_message_id else None,
                webhook_group_message_ids,
                str(webhook_nicks_message_id) if webhook_nicks_message_id else None,
                webhook_url,
                webhook_username if output_mode in {"discord", "both"} else None,
                webhook_avatar_url if output_mode in {"discord", "both"} else None,
                history,
            )

            previous_online_norm = set(cycle_data["current_online_norm"])
            first_cycle = False

            if args.once:
                break

        except Exception as exc:
            logging.exception("Blad w cyklu monitora: %s", exc)
            if args.once:
                raise

        if args.once:
            break

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
