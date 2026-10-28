import asyncio
import logging
import ssl
from dataclasses import dataclass
from typing import List, Optional, Sequence

import aiomqtt
from aiomqtt.exceptions import MqttConnectError

from .exceptions import DJIAuthError, DJIConnectionError, DJIConnectionLost

logger = logging.getLogger(__name__)

# Aircraft and gateway telemetry, published by docks and pilot-to-cloud RCs as
# thing/product/{device_sn}/osd.
OSD_TOPIC = "thing/product/+/osd"

# CONNACK refusals meaning "your credentials are wrong": MQTT 3.1.1 return
# codes 4/5, and the MQTT 5 reason codes paho maps them to (134/135).
_AUTH_REFUSAL_CODES = {4, 5, 134, 135}


@dataclass(frozen=True)
class DJIMessage:
    topic: str
    payload: bytes


def _code(rc) -> Optional[int]:
    return getattr(rc, "value", rc)


def _translate(exc: aiomqtt.MqttError) -> Exception:
    if isinstance(exc, MqttConnectError) and _code(exc.rc) in _AUTH_REFUSAL_CODES:
        return DJIAuthError(f"Broker refused the credentials: {exc}")
    return DJIConnectionError(f"MQTT connection failed: {exc}")


def _as_bytes(payload) -> bytes:
    if payload is None:
        return b""
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    return str(payload).encode()


def _build_client(
    host: str, port: int, username: str, password: str, use_tls: bool, client_id: str, persistent_session: bool
) -> aiomqtt.Client:
    return aiomqtt.Client(
        hostname=host,
        port=port,
        username=username,
        password=password,
        identifier=client_id,
        # A persistent session lets the broker queue QoS 1 messages for this
        # client id between scheduled runs.
        clean_session=not persistent_session,
        tls_context=ssl.create_default_context() if use_tls else None,
        timeout=20,
    )


async def check_connection(
    *, host: str, port: int, username: str, password: str, use_tls: bool, client_id: str
) -> None:
    """Connect and disconnect, raising DJIAuthError or DJIConnectionError on failure."""
    try:
        async with _build_client(host, port, username, password, use_tls, client_id, persistent_session=False):
            pass
    except aiomqtt.MqttError as e:
        raise _translate(e) from e


async def collect_messages(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    use_tls: bool,
    client_id: str,
    topics: Sequence[str],
    listen_seconds: float,
    max_messages: int,
) -> List[DJIMessage]:
    """Subscribe to `topics` and return what arrives within `listen_seconds`.

    Messages the broker queued for `client_id` since its last session are
    delivered first. Stops early once `max_messages` have been received.
    """
    messages: List[DJIMessage] = []

    async def _receive(client: aiomqtt.Client):
        async for message in client.messages:
            messages.append(DJIMessage(topic=message.topic.value, payload=_as_bytes(message.payload)))
            if len(messages) >= max_messages:
                return

    try:
        async with _build_client(host, port, username, password, use_tls, client_id, persistent_session=True) as client:
            granted = await client.subscribe([(topic, 1) for topic in topics])
            denied = [topic for topic, rc in zip(topics, granted) if (_code(rc) or 0) >= 0x80]
            if denied:
                raise DJIAuthError(f"Broker denied subscription to {', '.join(denied)}")
            try:
                await asyncio.wait_for(_receive(client), timeout=listen_seconds)
            except asyncio.TimeoutError:
                pass
    except aiomqtt.MqttError as e:
        if messages:
            raise DJIConnectionLost(f"MQTT connection lost after {len(messages)} messages: {e}", messages) from e
        raise _translate(e) from e

    logger.debug("Received %d MQTT messages from %s", len(messages), host)
    return messages
