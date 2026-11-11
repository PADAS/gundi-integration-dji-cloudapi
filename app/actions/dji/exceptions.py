from typing import List


class DJIClientError(Exception):
    """Base for failures talking to a DJI Cloud API MQTT broker."""


class DJIAuthError(DJIClientError):
    """The broker rejected the credentials, or denied a topic subscription."""


class DJIConnectionError(DJIClientError):
    """The broker could not be reached or the connection could not be set up."""


class DJIConnectionLost(DJIConnectionError):
    """The connection dropped after messages were already received.

    Messages acknowledged before the drop are gone from the broker's queue, so
    they travel with the exception for the caller to process before failing.
    """

    def __init__(self, message: str, messages: List["DJIMessage"]):  # noqa: F821
        super().__init__(message)
        self.messages = messages


class InvalidOSDMessage(ValueError):
    """An OSD message whose payload is not the JSON document DJI specifies."""
