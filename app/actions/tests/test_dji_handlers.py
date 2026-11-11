import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pydantic
import pytest

from gundi_core.events import LogLevel

from app.actions import handlers
from app.actions.configurations import PullFlightPositionsConfig
from app.actions.dji.client import DJIMessage
from app.actions.dji.exceptions import DJIAuthError, DJIConnectionError, DJIConnectionLost
from app.actions.tests.dji_samples import AIRCRAFT_SN, DOCK_SN, aircraft_osd, dock_osd, load_capture
from app.services.activity_logger import ephemeral_run
from app.services.errors import IntegrationAuthError, IntegrationConnectionError

T0 = 1761573600000  # 2025-10-27T14:00:00Z
OSD_TOPIC = f"thing/product/{AIRCRAFT_SN}/osd"


@pytest.fixture
def integration():
    auth = SimpleNamespace(
        action=SimpleNamespace(value="auth"),
        data={"broker_host": "mqtt.example.org", "username": "gundi", "password": "secret"},
    )
    return SimpleNamespace(id=uuid4(), name="DJI Dock", configurations=[auth])


@pytest.fixture
def state(mocker):
    store = {}

    async def get_state(integration_id, action_id):
        return store.get((integration_id, action_id), {})

    async def set_state(integration_id, action_id, value):
        store[(integration_id, action_id)] = value

    async def set_if_absent(integration_id, action_id, *, ttl_seconds, source_id="no-source"):
        if ephemeral_run.get() or (integration_id, action_id, source_id) in store:
            return False
        store[(integration_id, action_id, source_id)] = ttl_seconds
        return True

    async def delete_state(integration_id, action_id, source_id="no-source"):
        store.pop((integration_id, action_id, source_id), None)

    mocker.patch.object(handlers.state_manager, "get_state", side_effect=get_state)
    mocker.patch.object(handlers.state_manager, "set_state", side_effect=set_state)
    mocker.patch.object(handlers.state_manager, "set_if_absent", side_effect=set_if_absent)
    mocker.patch.object(handlers.state_manager, "delete_state", side_effect=delete_state)
    return store


@pytest.fixture
def sent(mocker):
    calls = []

    async def send(observations, integration_id):
        calls.extend(observations)
        return {}

    mocker.patch.object(handlers, "send_observations_to_gundi", side_effect=send)
    return calls


@pytest.fixture(autouse=True)
def quiet_activity_log(mocker):
    mocker.patch("app.services.activity_logger.publish_event", return_value=None)
    mocker.patch.object(handlers, "log_action_activity", return_value=None)


def receive(mocker, messages=None, side_effect=None):
    return mocker.patch.object(
        handlers.dji_client, "collect_messages", return_value=messages, side_effect=side_effect
    )


def osd(seconds, **kwargs):
    return DJIMessage(OSD_TOPIC, aircraft_osd(timestamp_ms=T0 + seconds * 1000, **kwargs))


@pytest.mark.asyncio
async def test_sends_aircraft_positions_thinned_by_interval(mocker, integration, state, sent):
    messages = [osd(s) for s in (0, 2, 4, 10, 12, 21)] + [DJIMessage(f"thing/product/{DOCK_SN}/osd", dock_osd())]
    receive(mocker, messages)

    result = await handlers.action_pull_flight_positions(
        integration, PullFlightPositionsConfig(min_interval_seconds=10)
    )

    assert [o["recorded_at"] for o in sent] == [
        "2025-10-27T14:00:00+00:00", "2025-10-27T14:00:10+00:00", "2025-10-27T14:00:21+00:00"
    ]
    assert sent[0] == {
        "source": AIRCRAFT_SN,
        "source_name": AIRCRAFT_SN,
        "type": "tracking-device",
        "subject_type": "drone",
        "recorded_at": "2025-10-27T14:00:00+00:00",
        "location": {"lat": -1.2921, "lon": 36.8219, "alt": 1712.4},
        "additional": {
            "gateway_serial_number": DOCK_SN,
            "relative_height_m": 85.2,
            "heading_deg": 92.1,
            "horizontal_speed_mps": 11.3,
            "vertical_speed_mps": -0.2,
            "battery_percent": 74,
            "flight_mode": "Wayline flight",
            "gps_satellites": 21,
        },
    }
    assert result == {"messages_received": 7, "aircraft": 1, "observations_extracted": 3}


@pytest.mark.asyncio
async def test_state_prevents_resending_across_runs(mocker, integration, state, sent):
    receive(mocker, [osd(0), osd(30)])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    # A redelivered message and one too close to the last sent position.
    receive(mocker, [osd(30), osd(35), osd(45)])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    assert [o["recorded_at"][-14:-6] for o in sent] == ["14:00:00", "14:00:30", "14:00:45"]


@pytest.mark.asyncio
async def test_filters_by_serial_number(mocker, integration, state, sent):
    other = DJIMessage("thing/product/OTHERSN/osd", aircraft_osd())
    receive(mocker, [osd(0), other])

    await handlers.action_pull_flight_positions(
        integration, PullFlightPositionsConfig(aircraft_serial_numbers=["OTHERSN"])
    )

    assert [o["source"] for o in sent] == ["OTHERSN"]


@pytest.mark.asyncio
async def test_malformed_messages_are_skipped_and_reported(mocker, integration, state, sent):
    receive(mocker, [DJIMessage(OSD_TOPIC, b"garbage"), osd(0)])

    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    assert len(sent) == 1
    handlers.log_action_activity.assert_called_once()


@pytest.mark.asyncio
async def test_connection_lost_sends_partial_data_and_warns_without_failing(mocker, integration, state, sent):
    # Raising would make PubSub redeliver the run, which then overlaps the next scheduled one.
    # Unacked messages stay queued on the broker for the next run, so a retry adds nothing.
    receive(mocker, side_effect=DJIConnectionLost("dropped", [osd(0)]))

    result = await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    assert len(sent) == 1
    assert result["observations_extracted"] == 1
    assert any(v == {} for v in state.values()) is False and state  # progress saved
    warning = handlers.log_action_activity.call_args
    assert warning.kwargs["level"] == LogLevel.WARNING
    assert "dropped" in warning.kwargs["title"]


@pytest.mark.asyncio
@pytest.mark.parametrize("error, expected", [
    (DJIAuthError("bad password"), IntegrationAuthError),
    (DJIConnectionError("unreachable"), IntegrationConnectionError),
])
async def test_client_errors_are_classified(mocker, integration, state, sent, error, expected):
    receive(mocker, side_effect=error)
    with pytest.raises(expected):
        await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())


@pytest.mark.asyncio
async def test_auth_action_reports_credentials(mocker, integration):
    check = mocker.patch.object(handlers.dji_client, "check_connection", return_value=None)
    config = handlers.get_auth_config(integration)

    assert await handlers.action_auth(integration, config) == {"valid_credentials": True}
    assert check.call_args.kwargs["password"] == "secret"

    check.side_effect = DJIAuthError("refused")
    assert (await handlers.action_auth(integration, config))["valid_credentials"] is False


@pytest.mark.asyncio
async def test_auth_checks_use_distinct_client_ids(mocker, integration):
    # Two concurrent credential checks must not take over each other's broker session.
    check = mocker.patch.object(handlers.dji_client, "check_connection", return_value=None)
    config = handlers.get_auth_config(integration)
    await handlers.action_auth(integration, config)
    await handlers.action_auth(integration, config)
    ids = [call.kwargs["client_id"] for call in check.call_args_list]
    assert len(set(ids)) == 2
    assert all(i.startswith(f"gundi-dji-{integration.id}-auth") for i in ids)
    assert all(i != handlers.mqtt_client_id(integration) for i in ids)


@pytest.mark.asyncio
async def test_replays_pilot2_capture(mocker, integration, state, sent):
    # Values from a real DJI Pilot 2 / Mavic 3T flight, published in
    # PADAS/gundi-integration-dji-pilot2-telemetry (Apache-2.0). The envelopes
    # were rebuilt around those values; Pilot 2 sends no gateway field here.
    receive(mocker, load_capture("pilot2_mavic3t_capture.ndjson"))

    result = await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    assert result["observations_extracted"] == 3
    first = sent[0]
    assert first["source"] == first["source_name"] == "MOCK5F1DJ34T000000A1"
    # On the ground in standby: height is the absolute altitude, elevation
    # (relative to takeoff) is 0.
    assert first["location"] == {"lat": -21.401153638547164, "lon": 28.340260079564025, "alt": 641.366943359375}
    assert first["additional"]["relative_height_m"] == 0
    assert first["additional"]["gps_satellites"] == 14
    assert "gateway_serial_number" not in first["additional"]


def test_thinning_keeps_every_position_with_zero_interval():
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    positions = [
        SimpleNamespace(serial_number="A", recorded_at=base + timedelta(seconds=s)) for s in (0, 1, 2)
    ]
    assert len(handlers.thin_positions(positions, {}, timedelta(0))) == 3


def test_listen_seconds_cannot_exceed_the_cron_period():
    PullFlightPositionsConfig(listen_seconds=50)
    with pytest.raises(pydantic.ValidationError):
        PullFlightPositionsConfig(listen_seconds=51)


@pytest.mark.asyncio
async def test_overlapping_run_is_skipped_while_previous_run_holds_the_lock(mocker, integration, state, sent):
    collect = receive(mocker, [osd(0)])
    state[(str(integration.id), handlers.PULL_ACTION_ID, handlers.RUN_LOCK_SOURCE_ID)] = 80

    result = await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    collect.assert_not_called()
    assert sent == []
    assert result["skipped"]


@pytest.mark.asyncio
async def test_run_lock_is_released_after_the_run(mocker, integration, state, sent):
    receive(mocker, [osd(0)])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig(listen_seconds=20))
    assert (str(integration.id), handlers.PULL_ACTION_ID, handlers.RUN_LOCK_SOURCE_ID) not in state
    ttl = handlers.state_manager.set_if_absent.call_args.kwargs["ttl_seconds"]
    assert ttl > 20  # covers the listen window plus connect and send time


@pytest.mark.asyncio
async def test_run_lock_is_released_when_the_run_fails(mocker, integration, state, sent):
    receive(mocker, side_effect=DJIConnectionError("unreachable"))
    with pytest.raises(IntegrationConnectionError):
        await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())
    assert (str(integration.id), handlers.PULL_ACTION_ID, handlers.RUN_LOCK_SOURCE_ID) not in state


@pytest.mark.asyncio
async def test_ephemeral_run_does_not_need_the_lock(mocker, integration, state, sent):
    # set_if_absent always answers False on the ephemeral path; the run must still happen.
    receive(mocker, [osd(0)])
    token = ephemeral_run.set(True)
    try:
        result = await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())
    finally:
        ephemeral_run.reset(token)
    assert result["observations_extracted"] == 1


@pytest.mark.asyncio
async def test_malformed_message_warnings_are_capped_per_run(mocker, integration, state, sent, caplog):
    receive(mocker, [DJIMessage(OSD_TOPIC, b"garbage")] * 10)
    with caplog.at_level("WARNING", logger=handlers.logger.name):
        await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())
    warnings = [r for r in caplog.records if r.name == handlers.logger.name and r.levelname == "WARNING"]
    assert len(warnings) == handlers.MAX_LOGGED_INVALID_MESSAGES
    assert handlers.log_action_activity.call_args.kwargs["title"].startswith("Skipped 10")


@pytest.mark.asyncio
async def test_aircraft_unseen_for_a_day_are_dropped_from_state(mocker, integration, state, sent):
    other = DJIMessage("thing/product/OTHERSN/osd", aircraft_osd(timestamp_ms=T0))
    receive(mocker, [osd(0), other])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    two_days = 2 * 24 * 3600
    receive(mocker, [osd(two_days)])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    saved = state[(str(integration.id), handlers.PULL_ACTION_ID)]
    assert set(saved["aircraft"]) == {AIRCRAFT_SN}


def pilot2_osd(seconds, **data):
    # Pilot 2 publishes no gateway field; an aircraft is recognised by its flight fields.
    payload = {"timestamp": T0 + seconds * 1000, "data": {"latitude": -1.2921, "longitude": 36.8219, **data}}
    return DJIMessage(OSD_TOPIC, json.dumps(payload).encode())


@pytest.mark.asyncio
async def test_pilot2_aircraft_stays_classified_across_runs(mocker, integration, state, sent):
    receive(mocker, [pilot2_osd(0, attitude_head=90.0, horizontal_speed=1.0)])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    # Incremental message: only the position changed, so no flight fields are present.
    receive(mocker, [pilot2_osd(30)])
    await handlers.action_pull_flight_positions(integration, PullFlightPositionsConfig())

    assert [o["recorded_at"][-14:-6] for o in sent] == ["14:00:00", "14:00:30"]
