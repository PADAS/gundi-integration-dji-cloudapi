import json
from datetime import datetime, timezone

import pytest

from app.actions.dji.exceptions import InvalidOSDMessage
from app.actions.dji.osd import parse_aircraft_position
from app.actions.tests.dji_samples import AIRCRAFT_SN, DOCK_SN, aircraft_osd, dock_osd


def test_parses_aircraft_osd():
    position = parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", aircraft_osd())

    assert position.serial_number == AIRCRAFT_SN
    assert position.gateway_serial_number == DOCK_SN
    assert position.recorded_at == datetime(2025, 10, 27, 14, 0, tzinfo=timezone.utc)
    assert (position.latitude, position.longitude) == (-1.2921, 36.8219)
    assert position.ellipsoid_height_m == 1712.4
    assert position.battery_percent == 74
    assert position.gps_satellites == 21
    assert position.mode == "Wayline flight"


def test_ignores_gateway_telemetry():
    assert parse_aircraft_position(f"thing/product/{DOCK_SN}/osd", dock_osd()) is None


def test_ignores_aircraft_without_gps_fix():
    assert parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", aircraft_osd(lat=0, lon=0)) is None


def test_ignores_aircraft_with_no_satellites():
    position = aircraft_osd(position_state={"gps_number": 0})
    assert parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", position) is None


def test_missing_position_state_is_not_a_lost_fix():
    # OSD is incremental: position_state is omitted when it hasn't changed.
    payload = json.loads(aircraft_osd())
    del payload["data"]["position_state"]
    position = parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", json.dumps(payload).encode())
    assert position.gps_satellites is None


@pytest.mark.parametrize("field", ["attitude_head", "horizontal_speed", "vertical_speed"])
def test_aircraft_without_gateway_is_recognised_by_any_flight_field(field):
    payload = {"timestamp": 1761573600000, "data": {"latitude": 1.0, "longitude": 2.0, field: 0}}
    position = parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", json.dumps(payload).encode())
    assert position is not None


def test_rc_without_gateway_is_ignored():
    # An RC's own OSD carries position and battery but no attitude or speeds.
    payload = {"timestamp": 1761573600000, "data": {"latitude": 1.0, "longitude": 2.0, "capacity_percent": 80}}
    assert parse_aircraft_position(f"thing/product/{DOCK_SN}/osd", json.dumps(payload).encode()) is None


def test_ignores_other_topics():
    assert parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/state", aircraft_osd()) is None


def test_unknown_mode_is_labelled():
    position = parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", aircraft_osd(mode_code=99))
    assert position.mode == "Unknown (99)"


@pytest.mark.parametrize("payload", [b"not json", b"[]", b'{"timestamp": 1}', b'{"gateway": "X", "data": {"latitude": 1, "longitude": 2}}'])
def test_malformed_payload_raises(payload):
    with pytest.raises(InvalidOSDMessage):
        parse_aircraft_position(f"thing/product/{AIRCRAFT_SN}/osd", payload)
