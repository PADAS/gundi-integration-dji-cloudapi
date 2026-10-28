# gundi-integration-dji

Gundi connector for DJI aircraft telemetry published over MQTT by the
[DJI Cloud API](https://developer.dji.com/doc/cloud-api-tutorial/en/) — DJI Docks and
pilot-to-cloud remote controllers (DJI Pilot 2) connected to an operator's own MQTT broker.

Built from the [gundi-integration-action-runner](https://github.com/PADAS/gundi-integration-action-runner)
template; see that repo for the runner framework itself.

## How it works

DJI devices publish aircraft telemetry ("OSD") to `thing/product/{aircraft_sn}/osd` on the
broker. An action runner is a request-driven Cloud Run service and can't hold an MQTT subscription
open, so the connector **drains the broker on a schedule**:

1. `pull_flight_positions` runs every minute. It connects with a stable client id
   (`gundi-dji-<integration id>`), a persistent session (`clean_session=False`) and a QoS 1
   subscription to `thing/product/+/osd`.
2. The broker first delivers what it queued for that session since the last run, then live
   messages, for `listen_seconds` (default 50).
3. Aircraft OSD messages become Gundi observations (`type: tracking-device`,
   `subject_type: drone`, `source` = aircraft serial number). Dock and RC telemetry, and aircraft
   without a GPS fix (DJI reports `0,0`), are skipped.
4. Positions are thinned per aircraft to `min_interval_seconds` (default 10). The last sent
   timestamp per aircraft is kept in integration state, so redelivered messages aren't resent.

Observation fields: `location.alt` is DJI's ellipsoid `height`. `additional` carries
`relative_height_m` (above takeoff), `heading_deg`, `horizontal_speed_mps`,
`vertical_speed_mps`, `battery_percent`, `flight_mode`, `gps_satellites` and
`gateway_serial_number` (the dock or RC).

## Configuration (Gundi portal)

**Auth** — the broker your DJI devices are configured to use:

| Field | Notes |
|---|---|
| MQTT Broker Host / Port | Port 8883 with TLS (default), 1883 without |
| Use TLS | Verifies the broker certificate against system CAs |
| Username / Password | An MQTT account for Gundi with **subscribe** permission on `thing/product/+/osd` |

**Pull Flight Positions**: `listen_seconds`, `min_interval_seconds`, and an optional
`aircraft_serial_numbers` allow-list.

### Broker requirements

- **Queued delivery between runs** relies on the broker keeping offline messages for persistent
  sessions. DJI publishes OSD at QoS 0. The subscriber's effective QoS is the lower of the two, so
  the broker must be set to queue QoS 0 messages for offline sessions: EMQX does this by default
  (`mqtt.mqueue_store_qos0`). Mosquitto needs `queue_qos0_messages true`. Without it, the
  connector still captures everything that arrives while it is listening (about 50 of every 60
  seconds by default).
- Give the Gundi account read-only ACLs. It never publishes.
- Each integration uses one client id. Don't run two integrations of this type against the same
  broker with the same Gundi integration id.

## Development

```bash
uv venv -p 3.10 .venv && uv pip install -p .venv -r requirements.txt
.venv/bin/python -m pytest app/actions/tests

# Exercise the MQTT client against a real broker:
docker run --rm -d -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
MQTT_TEST_BROKER=localhost:1883 .venv/bin/python -m pytest app/actions/tests/test_dji_client.py
```

Code layout:

- `app/actions/dji/` — MQTT client and OSD parser (no Gundi dependencies)
- `app/actions/configurations.py` — portal config models
- `app/actions/handlers.py` — `auth` and `pull_flight_positions` actions

Local runner setup (Redis, PubSub emulator, registering with a Gundi env) is described in
`local/LOCAL_DEVELOPMENT.md`.
