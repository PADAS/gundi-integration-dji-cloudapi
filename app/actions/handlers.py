import logging
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Tuple

from gundi_core.events import LogLevel

from app.actions.configurations import AuthenticateConfig, PullFlightPositionsConfig
from app.actions.dji import client as dji_client
from app.actions.dji.exceptions import DJIAuthError, DJIConnectionError, DJIConnectionLost, InvalidOSDMessage
from app.actions.dji.osd import AircraftPosition, parse_aircraft_position
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.errors import ConfigurationNotFound, IntegrationAuthError, IntegrationConnectionError
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action, generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

PULL_ACTION_ID = "pull_flight_positions"
OBSERVATION_BATCH_SIZE = 200
MAX_MESSAGES_PER_RUN = 20000
SUBJECT_TYPE = "drone"


def get_auth_config(integration) -> AuthenticateConfig:
    auth_config = find_config_for_action(configurations=integration.configurations, action_id="auth")
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def mqtt_client_id(integration) -> str:
    # Stable per integration so the broker keeps one persistent session for it.
    return f"gundi-dji-{integration.id}"


def connection_kwargs(auth_config: AuthenticateConfig) -> dict:
    return dict(
        host=auth_config.broker_host,
        port=auth_config.broker_port,
        username=auth_config.username,
        password=auth_config.password.get_secret_value(),
        use_tls=auth_config.use_tls,
    )


def parse_positions(messages: Iterable[dji_client.DJIMessage]) -> Tuple[List[AircraftPosition], int]:
    positions, invalid = [], 0
    for message in messages:
        try:
            position = parse_aircraft_position(message.topic, message.payload)
        except InvalidOSDMessage as e:
            logger.warning(str(e))
            invalid += 1
            continue
        if position:
            positions.append(position)
    return positions, invalid


def thin_positions(
    positions: List[AircraftPosition], last_recorded_at: Dict[str, datetime], min_interval: timedelta
) -> List[AircraftPosition]:
    """Drop positions already sent or closer than `min_interval` to the previous one, per aircraft.

    Updates `last_recorded_at` in place with the latest kept position per aircraft.
    """
    kept = []
    for position in sorted(positions, key=lambda p: p.recorded_at):
        previous = last_recorded_at.get(position.serial_number)
        if previous and position.recorded_at <= previous:
            continue  # Redelivered or out of order
        if previous and position.recorded_at - previous < min_interval:
            continue
        kept.append(position)
        last_recorded_at[position.serial_number] = position.recorded_at
    return kept


def source_name(serial_number: str, prefix: str) -> str:
    return f"{prefix}{serial_number[-6:]}" if prefix else serial_number


def to_observation(position: AircraftPosition, subject_name_prefix: str) -> dict:
    location = {"lat": position.latitude, "lon": position.longitude}
    if position.ellipsoid_height_m is not None:
        location["alt"] = position.ellipsoid_height_m
    additional = {
        "gateway_serial_number": position.gateway_serial_number,
        "relative_height_m": position.relative_height_m,
        "heading_deg": position.heading_deg,
        "horizontal_speed_mps": position.horizontal_speed_mps,
        "vertical_speed_mps": position.vertical_speed_mps,
        "battery_percent": position.battery_percent,
        "flight_mode": position.mode,
        "gps_satellites": position.gps_satellites,
    }
    return {
        "source": position.serial_number,
        "source_name": source_name(position.serial_number, subject_name_prefix),
        "type": "tracking-device",
        "subject_type": SUBJECT_TYPE,
        "recorded_at": position.recorded_at.isoformat(),
        "location": location,
        "additional": {k: v for k, v in additional.items() if v is not None},
    }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Testing MQTT broker connection for integration {integration.id}...")
    try:
        await dji_client.check_connection(
            **connection_kwargs(action_config), client_id=f"{mqtt_client_id(integration)}-auth"
        )
    except DJIAuthError as e:
        logger.warning(f"Auth unsuccessful for integration {integration.id}: {e}")
        return {"valid_credentials": False, "message": str(e)}
    except DJIConnectionError as e:
        raise IntegrationConnectionError(str(e)) from e
    return {"valid_credentials": True}


@crontab_schedule("* * * * *")  # Every minute; each run listens for listen_seconds
@activity_logger()
async def action_pull_flight_positions(integration, action_config: PullFlightPositionsConfig):
    auth_config = get_auth_config(integration)
    connection_lost = None
    try:
        messages = await dji_client.collect_messages(
            **connection_kwargs(auth_config),
            client_id=mqtt_client_id(integration),
            topics=[dji_client.OSD_TOPIC],
            listen_seconds=action_config.listen_seconds,
            max_messages=MAX_MESSAGES_PER_RUN,
        )
    except DJIConnectionLost as e:
        # Process what was received before failing; the broker won't resend it.
        messages, connection_lost = e.messages, e
    except DJIAuthError as e:
        raise IntegrationAuthError(str(e)) from e
    except DJIConnectionError as e:
        raise IntegrationConnectionError(str(e)) from e

    positions, invalid = parse_positions(messages)
    if action_config.aircraft_serial_numbers:
        wanted = set(action_config.aircraft_serial_numbers)
        positions = [p for p in positions if p.serial_number in wanted]

    state = await state_manager.get_state(str(integration.id), PULL_ACTION_ID)
    last_recorded_at = {
        sn: datetime.fromisoformat(ts) for sn, ts in state.get("last_recorded_at", {}).items()
    }
    positions = thin_positions(
        positions, last_recorded_at, timedelta(seconds=action_config.min_interval_seconds)
    )

    observations = [to_observation(p, action_config.subject_name_prefix) for p in positions]
    for batch in generate_batches(observations, OBSERVATION_BATCH_SIZE):
        await send_observations_to_gundi(observations=batch, integration_id=str(integration.id))

    await state_manager.set_state(
        str(integration.id),
        PULL_ACTION_ID,
        {"last_recorded_at": {sn: ts.isoformat() for sn, ts in last_recorded_at.items()}},
    )

    if invalid:
        await log_action_activity(
            integration_id=str(integration.id),
            action_id=PULL_ACTION_ID,
            level=LogLevel.WARNING,
            title=f"Skipped {invalid} malformed DJI OSD message(s)",
            config_data=action_config.dict(),
        )
    if connection_lost:
        raise IntegrationConnectionError(
            f"{connection_lost} ({len(observations)} observation(s) were sent before the drop)"
        ) from connection_lost

    return {
        "messages_received": len(messages),
        "aircraft": len({p.serial_number for p in positions}),
        "observations_extracted": len(observations),
    }
