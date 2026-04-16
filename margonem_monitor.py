import argparse
import base64
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.margonem.pl"
STATS_URL = f"{BASE_URL}/stats"
GUILD_URL_TEMPLATE = BASE_URL + "/guilds/view,{world},{guild_id}"
DEFAULT_WEBHOOK_USERNAME = "Cwl monitor"
DEFAULT_WEBHOOK_AVATAR_URL = "https://bivis.pl/wp-content/uploads/2025/09/Kamizelki.jpg"
DISCORD_EMBED_MAX_FIELDS = 25
DISCORD_EMBED_MAX_CHARS = 6000


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip().casefold()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


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
    config.setdefault(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MargonemGuildMonitor/1.0",
    )

    output_mode = str(config["output_mode"]).strip().lower()
    if output_mode not in {"terminal", "discord", "both"}:
        raise ValueError("output_mode musi byc jednym z: terminal, discord, both")

    if output_mode in {"discord", "both"} and not str(config["webhook_url"]).strip():
        raise ValueError("Dla output_mode=discord/both wymagane jest webhook_url")

    if int(config["guild_refresh_seconds"]) <= 0:
        raise ValueError("guild_refresh_seconds musi byc > 0")

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
    max_attempts: int = 4,
) -> requests.Response:
    last_response: Optional[requests.Response] = None

    for attempt in range(1, max_attempts + 1):
        response = session.request(method=method, url=url, json=payload, timeout=timeout)
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
            "webhook_nicks_message_id": None,
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
            "webhook_nicks_message_id": None,
            "webhook_username": None,
            "webhook_avatar_url": None,
            "history": [],
        }

    return {
        "online_norm": set(raw.get("online_norm", [])),
        "webhook_stats_message_id": raw.get("webhook_stats_message_id") or raw.get("webhook_message_id"),
        "webhook_nicks_message_id": raw.get("webhook_nicks_message_id"),
        "webhook_username": raw.get("webhook_username"),
        "webhook_avatar_url": raw.get("webhook_avatar_url"),
        "history": raw.get("history", []),
    }


def save_state(
    state_path: Path,
    online_norm: Set[str],
    webhook_stats_message_id: Optional[str],
    webhook_nicks_message_id: Optional[str],
    webhook_username: Optional[str],
    webhook_avatar_url: Optional[str],
    history: List[Dict[str, object]],
) -> None:
    payload = {
        "updated_at": utc_now(),
        "online_norm": sorted(online_norm),
        "webhook_stats_message_id": webhook_stats_message_id,
        "webhook_nicks_message_id": webhook_nicks_message_id,
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


def request_with_rate_limit(
    session: requests.Session,
    method: str,
    url: str,
    payload: Dict,
    timeout: int = 20,
) -> requests.Response:
    return request_with_retry(
        session=session,
        method=method,
        url=url,
        timeout=timeout,
        payload=payload,
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
    history = prune_history(history, now_ts=now_ts, window_seconds=600)
    baseline = history[0] if history else None

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
    history = prune_history(history, now_ts=now_ts, window_seconds=600)
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
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    title = f"Margonem monitor | {world} | statystyki"
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

    if avatar_url:
        embed["thumbnail"] = {"url": avatar_url}

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
        "content": f"",
        "embeds": [embed],
        "allowed_mentions": {"parse": []},
    }


def build_discord_nicks_payload(world: str, cycle_data: Dict, avatar_url: str) -> Dict:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    title = f"Margonem monitor | {world} | nicki online"
    description = f"Zaktualizowano: {utc_now()}"

    embed: Dict[str, object] = {
        "title": truncate_text(title, 256),
        "description": truncate_text(description, 4096),
        "color": 0x5865F2,
        "timestamp": now_iso,
        "fields": [],
    }

    if avatar_url:
        embed["thumbnail"] = {"url": avatar_url}

    fields: List[Dict[str, object]] = []
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
        for guild in guild_rows:
            name_chunks = split_names_for_discord_spoiler(guild["online_names"])
            total_chunks = len(name_chunks)
            for idx, chunk in enumerate(name_chunks, start=1):
                suffix = "" if total_chunks == 1 else f" ({idx}/{total_chunks})"
                fields.append(
                    {
                        "name": truncate_text(
                            f"Nicki - {guild['name']}{suffix}",
                            256,
                        ),
                        "value": truncate_text(f"||{chunk}||", 1024),
                        "inline": False,
                    }
                )

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

    if message_id:
        patch_url = webhook_url.rstrip("/") + f"/messages/{message_id}"
        response = request_with_rate_limit(
            session=session,
            method="PATCH",
            url=patch_url,
            payload=patch_payload,
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
    if webhook_avatar_url:
        post_payload["avatar_url"] = webhook_avatar_url

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
    response = request_with_rate_limit(
        session=session,
        method="DELETE",
        url=delete_url,
        payload={},
    )
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
        f"[Margonem monitor] Start {utc_now()}\n"
        f"Swiat: {world}\n"
        f"Klanow: {len(cycle_data['guild_breakdown'])} ({guild_breakdown})\n"
        f"Sledzonych postaci: {cycle_data['tracked_members_count']}\n"
        f"Online teraz: {cycle_data['online_count']}\n"
        f"Online (fragment): {format_name_list(cycle_data['online_names'])}"
    )


def build_delta_message(world: str, cycle_data: Dict) -> str:
    return (
        f"[Margonem monitor] Zmiana {utc_now()} | swiat: {world}\n"
        f"Online teraz: {cycle_data['online_count']}\n"
        f"Weszli online ({len(cycle_data['went_online'])}): "
        f"{format_name_list(cycle_data['went_online'])}\n"
        f"Zeszli offline ({len(cycle_data['went_offline'])}): "
        f"{format_name_list(cycle_data['went_offline'])}"
    )


def build_status_message(world: str, cycle_data: Dict) -> str:
    lines = [
        f"[Margonem monitor] Stan {utc_now()} | swiat: {world}",
        f"Sledzonych postaci: {cycle_data['tracked_members_count']}",
        f"Online teraz: {cycle_data['online_count']}",
        "",
        "Podzial na klany:",
    ]

    for guild in cycle_data["guild_breakdown"]:
        lines.append(
            f"- {guild['name']}: {guild['online_count']}/{guild['members_count']} online"
        )
        lines.append(f"  Online: {format_name_list(guild['online_names'])}")

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
    webhook_nicks_message_id = state.get("webhook_nicks_message_id")
    previous_webhook_username = str(state.get("webhook_username") or "").strip()
    previous_webhook_avatar_url = str(state.get("webhook_avatar_url") or "").strip()
    history_raw = state.get("history", [])
    history: List[Dict[str, object]] = history_raw if isinstance(history_raw, list) else []

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
    if profile_changed and (webhook_stats_message_id or webhook_nicks_message_id):
        logging.info(
            "Wykryto zmiane profilu webhooka (nazwa/avatar). Odtwarzam wiadomosci statusu."
        )
        for msg_id in [webhook_stats_message_id, webhook_nicks_message_id]:
            if not msg_id:
                continue
            try:
                delete_webhook_message(
                    session=session,
                    webhook_url=webhook_url,
                    message_id=str(msg_id),
                )
            except Exception as exc:
                logging.warning("Nie udalo sie usunac starej wiadomosci webhook: %s", exc)
        webhook_stats_message_id = None
        webhook_nicks_message_id = None

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
                discord_nicks_payload = build_discord_nicks_payload(world, cycle_data, webhook_avatar_url)

                webhook_stats_message_id = upsert_webhook_message(
                    session=session,
                    webhook_url=webhook_url,
                    payload=discord_stats_payload,
                    message_id=str(webhook_stats_message_id) if webhook_stats_message_id else None,
                    webhook_username=webhook_username,
                    webhook_avatar_url=webhook_avatar_url,
                )
                webhook_nicks_message_id = upsert_webhook_message(
                    session=session,
                    webhook_url=webhook_url,
                    payload=discord_nicks_payload,
                    message_id=str(webhook_nicks_message_id) if webhook_nicks_message_id else None,
                    webhook_username=webhook_username,
                    webhook_avatar_url=webhook_avatar_url,
                )

            save_state(
                state_path,
                cycle_data["current_online_norm"],
                str(webhook_stats_message_id) if webhook_stats_message_id else None,
                str(webhook_nicks_message_id) if webhook_nicks_message_id else None,
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
