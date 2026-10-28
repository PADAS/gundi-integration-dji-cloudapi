from typing import List

import pydantic

from .core import AuthActionConfiguration, ExecutableActionMixin, PullActionConfiguration


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    broker_host: str = pydantic.Field(
        ...,
        title="MQTT Broker Host",
        description="Hostname of the MQTT broker your DJI docks or remote controllers publish to "
                    "(the broker configured for DJI Cloud API), e.g. mqtt.example.org.",
    )
    broker_port: int = pydantic.Field(
        8883,
        title="MQTT Broker Port",
        description="Usually 8883 for TLS or 1883 without TLS.",
        ge=1,
        le=65535,
    )
    use_tls: bool = pydantic.Field(
        True,
        title="Use TLS",
        description="Encrypt the connection to the broker. Turn off only for brokers on a private network.",
    )
    username: str = pydantic.Field(
        ...,
        title="Username",
        description="MQTT username issued for Gundi. It needs read access to thing/product/+/osd.",
    )
    password: pydantic.SecretStr = pydantic.Field(
        ...,
        title="Password",
        description="MQTT password for the username above.",
        format="password",
    )


class PullFlightPositionsConfig(PullActionConfiguration):
    listen_seconds: int = pydantic.Field(
        50,
        title="Listen Duration (seconds)",
        description="How long each run listens for telemetry. Runs start every minute, so keep this under 60 "
                    "for near-continuous coverage.",
        ge=5,
        le=300,
    )
    min_interval_seconds: int = pydantic.Field(
        10,
        title="Minimum Interval Between Positions (seconds)",
        description="DJI aircraft report their position up to twice a second. Positions closer together than "
                    "this, per aircraft, are dropped. Set to 0 to keep every position.",
        ge=0,
    )
    aircraft_serial_numbers: List[str] = pydantic.Field(
        default_factory=list,
        title="Aircraft Serial Numbers",
        description="Only send positions for these aircraft. Leave empty to send every aircraft on the broker.",
    )
