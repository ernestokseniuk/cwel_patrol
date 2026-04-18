import math
import time
from pathlib import Path

from margonem_monitor import render_guild_chart_png


def build_simulated_day_points(max_members: int = 180):
    now_ts = int(time.time())
    start_ts = now_ts - (1440 - 1) * 60
    points = []

    for minute in range(1440):
        ts = start_ts + minute * 60

        # Base daily rhythm (night low, evening high).
        daily = 0.45 + 0.35 * math.sin((minute / 1440.0) * 2 * math.pi - math.pi / 2)

        # Mid-frequency activity waves.
        wave = 0.1 * math.sin((minute / 180.0) * 2 * math.pi)

        # Add deterministic spikes to mimic raids/events.
        spike = 0.0
        if 450 <= minute <= 520:
            spike += 0.12
        if 1080 <= minute <= 1160:
            spike += 0.18

        ratio = max(0.03, min(0.95, daily + wave + spike))
        online = int(round(max_members * ratio))
        points.append((ts, online))

    return points


def main():
    max_members = 180
    points = build_simulated_day_points(max_members=max_members)
    current_online = points[-1][1]

    image_bytes = render_guild_chart_png(
        guild_name="Symulacja Klanu",
        points=points,
        current_online=current_online,
        members_count=max_members,
    )

    out_path = Path("preview_chart_day.png")
    out_path.write_bytes(image_bytes)
    print(f"Saved: {out_path.resolve()}")


if __name__ == "__main__":
    main()
