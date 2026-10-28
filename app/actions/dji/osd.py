import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .exceptions import InvalidOSDMessage

OSD_TOPIC_PATTERN = re.compile(r"^thing/product/(?P<sn>[^/]+)/osd$")

# DJI Cloud API aircraft flight-controller modes (OSD `mode_code`).
AIRCRAFT_MODES = {
    0: "Standby",
    1: "Takeoff preparation",
    2: "Takeoff preparation complete",
    3: "Manual flight",
    4: "Automatic takeoff",
    5: "Wayline flight",
    6: "Panoramic photography",
    7: "Intelligent tracking",
    8: "ADS-B avoidance",
    9: "Auto returning to home",
    10: "Automatic landing",
    11: "Forced landing",
    12: "Three-propeller landing",
    13: "Upgrading",
    14: "Not connected",
    15: "APAS",
    16: "Virtual stick state",
    17: "Live flight controls",
}


@dataclass(frozen=True)
class AircraftPosition:
    serial_number: str
    gateway_serial_number: Optional[str]
    recorded_at: datetime
    latitude: float
    longitude: float
    ellipsoid_height_m: Optional[float]
    relative_height_m: Optional[float]
    heading_deg: Optional[float]
    horizontal_speed_mps: Optional[float]
    vertical_speed_mps: Optional[float]
    battery_percent: Optional[int]
    mode_code: Optional[int]
    gps_satellites: Optional[int]

    @property
    def mode(self) -> Optional[str]:
        if self.mode_code is None:
            return None
        return AIRCRAFT_MODES.get(self.mode_code, f"Unknown ({self.mode_code})")


# Fields only an aircraft's OSD carries. Docks and RCs report position, battery
# and (docks) mode_code and position_state, but never attitude or speeds.
AIRCRAFT_ONLY_FIELDS = ("attitude_head", "attitude_pitch", "attitude_roll", "horizontal_speed", "vertical_speed")


def _is_aircraft(device_sn: str, envelope: dict, data: dict) -> bool:
    # Docks and RCs publish their own OSD on a topic named after themselves and
    # are their own gateway; an aircraft's OSD names its dock/RC as gateway.
    gateway = envelope.get("gateway")
    if gateway:
        return gateway != device_sn
    # OSD is incremental (only changed fields are sent), so check for any
    # aircraft-only field rather than one in particular.
    return any(field in data for field in AIRCRAFT_ONLY_FIELDS)


def parse_aircraft_position(topic: str, payload: bytes) -> Optional[AircraftPosition]:
    """Parse one OSD message into an aircraft position.

    Returns None when the message is valid but carries no aircraft fix: another
    topic, gateway (dock/RC) telemetry, or an aircraft without GPS lock, which
    DJI reports as 0,0 or with no satellites. Raises InvalidOSDMessage for a
    malformed payload.
    """
    match = OSD_TOPIC_PATTERN.match(topic)
    if not match:
        return None
    device_sn = match.group("sn")

    try:
        envelope = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidOSDMessage(f"OSD payload on {topic} is not JSON: {e}") from e
    if not isinstance(envelope, dict) or not isinstance(envelope.get("data"), dict):
        raise InvalidOSDMessage(f"OSD payload on {topic} has no data object")
    data = envelope["data"]

    if not _is_aircraft(device_sn, envelope, data):
        return None

    latitude, longitude = data.get("latitude"), data.get("longitude")
    if latitude is None or longitude is None or (latitude == 0 and longitude == 0):
        return None

    try:
        recorded_at = datetime.fromtimestamp(int(envelope["timestamp"]) / 1000, tz=timezone.utc)
    except (KeyError, TypeError, ValueError, OverflowError) as e:
        raise InvalidOSDMessage(f"OSD payload on {topic} has no valid timestamp") from e

    battery = data.get("battery") if isinstance(data.get("battery"), dict) else {}
    position_state = data.get("position_state") if isinstance(data.get("position_state"), dict) else {}
    # position_state is absent from messages where it didn't change, so only an
    # explicit zero satellite count rules a fix out.
    if position_state.get("gps_number") == 0:
        return None
    return AircraftPosition(
        serial_number=device_sn,
        gateway_serial_number=envelope.get("gateway"),
        recorded_at=recorded_at,
        latitude=float(latitude),
        longitude=float(longitude),
        ellipsoid_height_m=data.get("height"),
        relative_height_m=data.get("elevation"),
        heading_deg=data.get("attitude_head"),
        horizontal_speed_mps=data.get("horizontal_speed"),
        vertical_speed_mps=data.get("vertical_speed"),
        battery_percent=battery.get("capacity_percent"),
        mode_code=data.get("mode_code"),
        gps_satellites=position_state.get("gps_number"),
    )
