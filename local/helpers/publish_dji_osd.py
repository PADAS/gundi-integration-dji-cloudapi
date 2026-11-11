#!/usr/bin/env python
"""Publish DJI Cloud API OSD telemetry to an MQTT broker, as a dock or Pilot 2 RC would.

For demos and local testing of the connector against a broker you control, e.g. the
mosquitto service in local/docker-compose.yml. Needs only aiomqtt, which the connector
already depends on, so run it with the project's environment:

  # A synthetic 5-minute flight from a dock, 2 messages/s, circling a point in Nairobi:
  python local/helpers/publish_dji_osd.py synthetic --gateway 4TADKAQ000002J

  # The same aircraft flown from DJI Pilot 2 (no gateway field):
  python local/helpers/publish_dji_osd.py synthetic --center -21.4012,28.3403 --duration 0

  # Replay a capture recorded with mosquitto_sub (see app/actions/tests/dji_samples.py),
  # with timestamps rewritten to now and the messages paced at --rate:
  python local/helpers/publish_dji_osd.py replay app/actions/tests/files/pilot2_mavic3t_capture.ndjson --loop

DJI publishes OSD at QoS 0 (the default here). Messages published while the connector is
between runs only reach it if the broker queues QoS 0 for offline sessions; the bundled
mosquitto.conf does. Use --qos 1 to sidestep that when testing something else.
"""
import argparse
import asyncio
import itertools
import json
import math
import sys
import time
import uuid
from pathlib import Path
from typing import Iterator, Optional, Tuple

import aiomqtt

AIRCRAFT_SN = "1581F5BKD225D00BH5RF"
NAIROBI = (-1.2921, 36.8219)
EARTH_RADIUS_M = 6_371_000


def osd_topic(device_sn: str) -> str:
    return f"thing/product/{device_sn}/osd"


def envelope(data: dict, timestamp_ms: int, gateway: Optional[str]) -> dict:
    message = {
        "tid": str(uuid.uuid4()),
        "bid": str(uuid.uuid4()),
        "timestamp": timestamp_ms,
        "data": data,
    }
    if gateway:
        message["gateway"] = gateway
    return message


def offset(center: Tuple[float, float], east_m: float, north_m: float) -> Tuple[float, float]:
    lat, lon = center
    dlat = math.degrees(north_m / EARTH_RADIUS_M)
    dlon = math.degrees(east_m / (EARTH_RADIUS_M * math.cos(math.radians(lat))))
    return lat + dlat, lon + dlon


def synthetic_flight(
    center: Tuple[float, float],
    radius_m: float,
    speed_mps: float,
    ground_height_m: float,
    cruise_elevation_m: float,
    rate_hz: float,
    takeoff_s: float = 15.0,
) -> Iterator[dict]:
    """Yield OSD `data` dicts for an aircraft taking off, then circling `center` forever.

    `height` is DJI's ellipsoid altitude, `elevation` is height above takeoff.
    """
    period_s = 2 * math.pi * radius_m / speed_mps
    for i in itertools.count():
        t = i / rate_hz
        taking_off = t < takeoff_s
        elevation = cruise_elevation_m * min(t / takeoff_s, 1.0)
        angle = 2 * math.pi * max(t - takeoff_s, 0) / period_s
        east, north = radius_m * math.sin(angle), radius_m * math.cos(angle) - radius_m
        lat, lon = offset(center, east, north)
        heading = (math.degrees(angle) + 90) % 360
        yield {
            "latitude": round(lat, 7),
            "longitude": round(lon, 7),
            "height": round(ground_height_m + elevation, 2),
            "elevation": round(elevation, 2),
            "attitude_head": round(((heading + 180) % 360) - 180, 1),  # DJI: -180..180, 0 = north
            "attitude_pitch": 0.0,
            "attitude_roll": 0.0,
            "horizontal_speed": 0.0 if taking_off else speed_mps,
            "vertical_speed": round(cruise_elevation_m / takeoff_s, 2) if taking_off else 0.0,
            "mode_code": 4 if taking_off else 5,  # Automatic takeoff, then wayline flight
            "battery": {"capacity_percent": max(5, int(95 - t / 30))},
            "position_state": {"gps_number": 18, "is_fixed": 2, "rtk_number": 0},
        }


def dock_osd(center: Tuple[float, float], ground_height_m: float) -> dict:
    return {
        "latitude": center[0],
        "longitude": center[1],
        "height": ground_height_m,
        "mode_code": 0,
        "network_state": {"type": 2, "quality": 4},
    }


def replay_messages(path: Path, loop: bool) -> Iterator[Tuple[str, dict]]:
    """Yield (topic, envelope) from an NDJSON capture: {"topic": ..., "payload": {...}} per line."""
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    if not lines:
        sys.exit(f"{path} is empty")
    while True:
        for line in lines:
            record = json.loads(line)
            yield record["topic"], record["payload"]
        if not loop:
            return


def now_ms() -> int:
    return int(time.time() * 1000)


async def publish(args, messages: Iterator[Tuple[str, dict]]):
    host, _, port = args.broker.partition(":")
    sent = 0
    started = time.monotonic()
    async with aiomqtt.Client(
        hostname=host, port=int(port or 1883), username=args.username, password=args.password,
        identifier=f"dji-publisher-{uuid.uuid4().hex[:8]}",
    ) as client:
        print(f"Connected to {args.broker}; publishing at {args.rate}/s, QoS {args.qos}. Ctrl-C to stop.")
        for topic, message in messages:
            if args.duration and time.monotonic() - started >= args.duration:
                break
            await client.publish(topic, json.dumps(message).encode(), qos=args.qos)
            sent += 1
            if sent % max(1, int(args.rate * 10)) == 0:
                data = message.get("data", {})
                print(f"{sent:6d} sent  {topic}  {data.get('latitude')},{data.get('longitude')}")
            await asyncio.sleep(1 / args.rate)
    print(f"Done: {sent} message(s) published.")


def synthetic_stream(args) -> Iterator[Tuple[str, dict]]:
    center = tuple(float(v) for v in args.center.split(","))
    flight = synthetic_flight(center, args.radius, args.speed, args.ground_height, args.elevation, args.rate)
    dock_every = int(args.rate * 5)  # A dock reports its own OSD too, a few times a minute.
    for i, data in enumerate(flight):
        if args.gateway and i % dock_every == 0:
            yield osd_topic(args.gateway), envelope(dock_osd(center, args.ground_height), now_ms(), args.gateway)
        yield osd_topic(args.serial), envelope(data, now_ms(), args.gateway)


def replay_stream(args) -> Iterator[Tuple[str, dict]]:
    for topic, message in replay_messages(args.capture, args.loop):
        if not args.keep_timestamps:
            message = {**message, "timestamp": now_ms()}
        yield topic, message


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--broker", default="localhost:1883", help="host:port (default localhost:1883)")
    parser.add_argument("--username", default="dji")
    parser.add_argument("--password", default="dji")
    parser.add_argument("--rate", type=float, default=2.0, help="messages per second (DJI sends 2)")
    parser.add_argument("--qos", type=int, choices=[0, 1], default=0, help="DJI publishes at 0")
    parser.add_argument("--duration", type=float, default=300, help="seconds to run; 0 = until stopped")
    modes = parser.add_subparsers(dest="mode", required=True)

    synth = modes.add_parser("synthetic", help="generate a flight circling a point")
    synth.add_argument("--serial", default=AIRCRAFT_SN, help="aircraft serial number")
    synth.add_argument("--gateway", help="dock or RC serial; omit for Pilot 2, which sends none")
    synth.add_argument("--center", default=f"{NAIROBI[0]},{NAIROBI[1]}", help="lat,lon to circle")
    synth.add_argument("--radius", type=float, default=200, help="circle radius in metres")
    synth.add_argument("--speed", type=float, default=10, help="metres per second")
    synth.add_argument("--elevation", type=float, default=80, help="cruise height above takeoff, metres")
    synth.add_argument("--ground-height", type=float, default=1700, help="ellipsoid height at takeoff, metres")

    replay = modes.add_parser("replay", help="replay an NDJSON capture")
    replay.add_argument("capture", type=Path)
    replay.add_argument("--loop", action="store_true", help="start over when the capture ends")
    replay.add_argument("--keep-timestamps", action="store_true", help="send the recorded timestamps as is")

    args = parser.parse_args(argv)
    stream = synthetic_stream(args) if args.mode == "synthetic" else replay_stream(args)
    try:
        asyncio.run(publish(args, stream))
    except KeyboardInterrupt:
        pass
    except aiomqtt.MqttError as e:
        sys.exit(f"MQTT error: {e}")


if __name__ == "__main__":
    main()
