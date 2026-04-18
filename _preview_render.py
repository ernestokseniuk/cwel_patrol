import math
import time
from pathlib import Path

from margonem_monitor import render_guild_chart_png


def build_simulated_group_points(hours: int = 12):
    now_ts = int(time.time())
    minutes_total = hours * 60
    start_ts = now_ts - (minutes_total - 1) * 60

    clan_specs = [
        {
            "name": "Klan Alfa",
            "members": 64,
            "base": 0.16,
            "amp": 0.08,
            "phase_shift": 0.15,
            "spikes": [(95, 130, 0.10), (300, 360, 0.12)],
        },
        {
            "name": "Klan Beta",
            "members": 52,
            "base": 0.12,
            "amp": 0.06,
            "phase_shift": 0.55,
            "spikes": [(180, 235, 0.08), (430, 500, 0.15)],
        },
        {
            "name": "Klan Gamma",
            "members": 38,
            "base": 0.09,
            "amp": 0.05,
            "phase_shift": 0.92,
            "spikes": [(40, 85, 0.07), (250, 290, 0.10), (560, 620, 0.08)],
        },
        {
            "name": "Klan Delta",
            "members": 26,
            "base": 0.07,
            "amp": 0.04,
            "phase_shift": 1.28,
            "spikes": [(150, 200, 0.08), (380, 430, 0.12)],
        },
    ]

    series_points = []

    for spec_index, spec in enumerate(clan_specs):
        points = []

        for minute in range(minutes_total):
            ts = start_ts + minute * 60

            daily = spec["base"] + spec["amp"] * math.sin(
                (minute / minutes_total) * 2 * math.pi - math.pi / 2 + spec["phase_shift"]
            )
            wave = 0.03 * math.sin((minute / 36.0) * 2 * math.pi + spec["phase_shift"])
            spike = 0.0
            for spike_start, spike_end, spike_value in spec["spikes"]:
                if spike_start <= minute <= spike_end:
                    spike += spike_value

            ratio = max(0.03, min(0.95, daily + wave + spike))
            online = int(round(spec["members"] * ratio))
            points.append((ts, online))

        series_points.append(
            {
                "name": spec["name"],
                "current_online": points[-1][1],
                "points": points,
                "members": spec["members"],
            }
        )

    totals = []
    for minute in range(minutes_total):
        ts = start_ts + minute * 60
        total_online = sum(series["points"][minute][1] for series in series_points)
        totals.append((ts, total_online))

    return totals, series_points


def main():
    points, guild_series = build_simulated_group_points(hours=12)
    max_members = sum(series["members"] for series in guild_series)
    current_online = points[-1][1]

    image_bytes = render_guild_chart_png(
        guild_name="Symulacja grupy klanow",
        points=points,
        current_online=current_online,
        members_count=max_members,
        guild_series=guild_series,
    )

    out_path = Path("preview_chart_12h_multi_guild.png")
    out_path.write_bytes(image_bytes)
    print(f"Saved: {out_path.resolve()}")


if __name__ == "__main__":
    main()
