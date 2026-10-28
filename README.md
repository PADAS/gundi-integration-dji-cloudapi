# gundi-integration-dji

Gundi connector for DJI aircraft telemetry published over MQTT by the
[DJI Cloud API](https://developer.dji.com/doc/cloud-api-tutorial/en/) — DJI Docks and
pilot-to-cloud remote controllers (DJI Pilot 2) connected to an operator's own MQTT broker.

Built from the [gundi-integration-action-runner](https://github.com/PADAS/gundi-integration-action-runner)
template; see that repo for the runner framework itself.

> [!IMPORTANT]
> **Check these two things before deploying.** Both come from the field notes in
> [PADAS/gundi-integration-dji-pilot2-telemetry](https://github.com/PADAS/gundi-integration-dji-pilot2-telemetry),
> which runs DJI Pilot 2 with a Mavic 3T in production. Neither has been tested against a real
> device with this connector.
>
> 1. **Something on the operator's side must keep the DJI connection alive. This connector can't.**
>    - Pilot 2 won't publish aircraft telemetry until a platform replies to its `update_topo`
>      message on `sys/product/{gateway_sn}/status` (reply on `.../status_reply`). This connector
>      only reads `thing/product/+/osd`, never publishes, and isn't connected when the RC
>      connects. Docks paired with DJI FlightHub 2 or an operator's own Cloud API platform are
>      already covered. **An RC-only setup on a bare broker gets no data** unless the operator
>      runs something that answers, such as the pilot2-telemetry repo's `osd_forwarder.py`.
>    - The Pilot 2 platform login page must call `platformSetWorkspaceId` and
>      `platformSetInformation` before `thingConnect`. Without them, Pilot 2 drops the cloud
>      connection when the pilot leaves the login page to fly. See the pilot2-telemetry repo's
>      `reserve-side/deploy/login.html`.
> 2. **The broker must queue QoS 0 messages, or about 10 seconds of every minute is lost.**
>    Positions arriving between runs reach the connector only if the broker queues them for its
>    persistent session. DJI publishes OSD at QoS 0, and brokers don't queue QoS 0 messages for
>    offline sessions by default. EMQX queues them (`mqtt.mqueue_store_qos0`, on by default).
>    Mosquitto needs `queue_qos0_messages true`, and `persistence true` so the queue survives a
>    broker restart. Without this, the connector only gets what arrives while it's listening:
>    about 50 of every 60 seconds by default. If that gap isn't acceptable, the alternative is
>    an always-on forwarder, as in the pilot2-telemetry repo.

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
   `subject_type: drone`, `source` and `source_name` = the full aircraft serial number, which
   Gundi uses as the EarthRanger subject name and source id).
   Dock and RC telemetry, and aircraft without a GPS fix (DJI reports `0,0` or zero satellites),
   are skipped. OSD messages only carry fields that changed, so a message without a position
   is skipped too, and `additional` holds whatever that message reported.
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

- Queue QoS 0 messages for offline sessions; see item 2 at the top of this README.
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

### Replaying real captures

Test against recorded traffic, not just hand-written samples. Record a capture from a live
broker, one message per line:

```bash
mosquitto_sub -h HOST -p 8883 -u USER -P PASS -t 'thing/product/+/osd' \
  -F '{"topic":"%t","payload":%p}' > app/actions/tests/files/my_capture.ndjson
```

Then load it in a test with `load_capture("my_capture.ndjson")` from
`app/actions/tests/dji_samples.py`. Remove or replace real serial numbers before committing.
`pilot2_mavic3t_capture.ndjson` holds position and telemetry values from a real Pilot 2 / Mavic
3T flight, taken from the pilot2-telemetry repo (Apache-2.0). The surrounding message fields were
rebuilt around those values, because that repo has only normalized position fixes, not raw MQTT
messages. A raw Pilot 2 capture and a raw Dock capture are both still wanted.

Code layout:

- `app/actions/dji/` — MQTT client and OSD parser (no Gundi dependencies)
- `app/actions/configurations.py` — portal config models
- `app/actions/handlers.py` — `auth` and `pull_flight_positions` actions

Local runner setup (Redis, PubSub emulator, registering with a Gundi env) is described in
`local/LOCAL_DEVELOPMENT.md`.
