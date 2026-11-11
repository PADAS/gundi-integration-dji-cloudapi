"""MQTT client behaviour.

The broker tests run against a real broker when MQTT_TEST_BROKER is set, e.g.
  docker run --rm -d -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
  MQTT_TEST_BROKER=localhost:1883 pytest app/actions/tests/test_dji_client.py
"""
import asyncio
import os
import uuid

import aiomqtt
from aiomqtt.exceptions import MqttConnectError
import pytest

from app.actions.dji import client as dji_client
from app.actions.dji.exceptions import DJIAuthError, DJIConnectionError, DJIConnectionLost
from app.actions.tests.dji_samples import AIRCRAFT_SN, aircraft_osd

BROKER = os.environ.get("MQTT_TEST_BROKER")
requires_broker = pytest.mark.skipif(not BROKER, reason="MQTT_TEST_BROKER not set")


def broker_kwargs():
    host, port = BROKER.split(":")
    return dict(host=host, port=int(port), username="gundi", password="secret", use_tls=False)


class FakeClient:
    """Stands in for aiomqtt.Client: fails on enter, or yields messages then optionally fails."""

    def __init__(self, enter_error=None, messages=(), stream_error=None, granted=(1,)):
        self.enter_error, self.stream_error, self.granted = enter_error, stream_error, granted
        self._messages = messages

    async def __aenter__(self):
        if self.enter_error:
            raise self.enter_error
        return self

    async def __aexit__(self, *exc):
        return False

    async def subscribe(self, topics):
        return list(self.granted)

    @property
    def messages(self):
        async def gen():
            for topic, payload in self._messages:
                yield aiomqtt.Message(topic, payload, qos=1, retain=False, mid=1, properties=None)
            if self.stream_error:
                raise self.stream_error
            await asyncio.sleep(3600)
        return gen()


def collect(**overrides):
    kwargs = dict(
        host="broker", port=8883, username="u", password="p", use_tls=True, client_id="c",
        topics=[dji_client.OSD_TOPIC], listen_seconds=0.2, max_messages=100,
    )
    kwargs.update(overrides)
    return dji_client.collect_messages(**kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("rc", [4, 5])
async def test_bad_credentials_raise_auth_error(mocker, rc):
    mocker.patch.object(dji_client, "_build_client", return_value=FakeClient(enter_error=MqttConnectError(rc)))
    with pytest.raises(DJIAuthError):
        await collect()


@pytest.mark.asyncio
async def test_unreachable_broker_raises_connection_error(mocker):
    mocker.patch.object(dji_client, "_build_client", return_value=FakeClient(enter_error=aiomqtt.MqttError("timed out")))
    with pytest.raises(DJIConnectionError) as exc_info:
        await collect()
    assert not isinstance(exc_info.value, DJIAuthError)


@pytest.mark.asyncio
async def test_denied_subscription_raises_auth_error(mocker):
    mocker.patch.object(dji_client, "_build_client", return_value=FakeClient(granted=(0x80,)))
    with pytest.raises(DJIAuthError):
        await collect()


@pytest.mark.asyncio
async def test_returns_messages_received_within_window(mocker):
    topic = f"thing/product/{AIRCRAFT_SN}/osd"
    mocker.patch.object(dji_client, "_build_client", return_value=FakeClient(messages=[(topic, aircraft_osd())]))
    messages = await collect()
    assert messages == [dji_client.DJIMessage(topic=topic, payload=aircraft_osd())]


@pytest.mark.asyncio
async def test_stops_at_max_messages(mocker):
    topic = f"thing/product/{AIRCRAFT_SN}/osd"
    fake = FakeClient(messages=[(topic, aircraft_osd())] * 5)
    mocker.patch.object(dji_client, "_build_client", return_value=fake)
    assert len(await collect(max_messages=3, listen_seconds=10)) == 3


@pytest.mark.asyncio
async def test_connection_lost_carries_received_messages(mocker):
    topic = f"thing/product/{AIRCRAFT_SN}/osd"
    fake = FakeClient(messages=[(topic, aircraft_osd())], stream_error=aiomqtt.MqttError("Disconnected"))
    mocker.patch.object(dji_client, "_build_client", return_value=fake)
    with pytest.raises(DJIConnectionLost) as exc_info:
        await collect(listen_seconds=10)
    assert len(exc_info.value.messages) == 1


@requires_broker
@pytest.mark.asyncio
async def test_broker_delivers_live_and_queued_messages():
    client_id = f"gundi-dji-test-{uuid.uuid4()}"
    topic = f"thing/product/{AIRCRAFT_SN}/osd"
    host, port = BROKER.split(":")

    # First run establishes the persistent session.
    await dji_client.collect_messages(
        **broker_kwargs(), client_id=client_id, topics=[dji_client.OSD_TOPIC], listen_seconds=0.5, max_messages=10
    )
    # Published while the runner is disconnected: the broker must queue it.
    async with aiomqtt.Client(host, int(port)) as publisher:
        await publisher.publish(topic, aircraft_osd(timestamp_ms=1), qos=1)

    async def publish_live():
        await asyncio.sleep(0.5)
        async with aiomqtt.Client(host, int(port)) as publisher:
            await publisher.publish(topic, aircraft_osd(timestamp_ms=2), qos=1)

    publisher_task = asyncio.create_task(publish_live())
    messages = await dji_client.collect_messages(
        **broker_kwargs(), client_id=client_id, topics=[dji_client.OSD_TOPIC], listen_seconds=2, max_messages=10
    )
    await publisher_task
    assert [m.payload for m in messages] == [aircraft_osd(timestamp_ms=1), aircraft_osd(timestamp_ms=2)]


@requires_broker
@pytest.mark.asyncio
async def test_broker_connection_check():
    await dji_client.check_connection(**broker_kwargs(), client_id=f"gundi-dji-test-{uuid.uuid4()}")


@pytest.mark.asyncio
async def test_unreachable_real_port_raises_connection_error():
    with pytest.raises(DJIConnectionError):
        await dji_client.check_connection(
            host="127.0.0.1", port=1, username="u", password="p", use_tls=False, client_id="c"
        )
