# Running an Action Runner Locally

You can run this Action Runner locally using Docker Compose. This will allow you to develop the code and debug it locally while still taking advantage of Gundi Core services.

## Requirements

- Python 3.10
- Docker
- Docker Compose

## Steps

Inside the `local` directory, do these things:

**Set your Environment**

Create a copy of `.env.local.example` and name it `.env.local`

Edit the `.env.local` file and set the `GUNDI_OAUTH_CLIENT_SECRET` to a secret from the stage environment (Ask the Gundi Team)

**Build and Run**

In a Terminal, run `docker compose up --build`

> [!NOTE]
>
> The Be sure to compile a `requirements.txt` file before running the docker compose command, or you might miss some important dependencies.


Once these steps are complete you can visit http://localhost:8080/docs to see your Action Runner's browsable API.

## Simulating DJI devices

The stack includes a Mosquitto broker (`mosquitto` service, `mosquitto/mosquitto.conf`) that stands
in for the operator's DJI Cloud API broker. It takes any username and password, has no TLS, and
queues QoS 0 messages for offline sessions, which DJI's real brokers must do too (see the README).

1. Start it, on its own or with the rest of the stack: `docker compose up -d mosquitto`
2. Point the integration at it. From the runner inside Compose the broker is `mosquitto`, port
   `1883`; from a runner on the host it is `localhost`. Set **Use TLS** off. Username and
   password can be anything.
3. Publish telemetry with `helpers/publish_dji_osd.py`, from the repo root with the project's
   Python environment (it only needs `aiomqtt`, which the connector already depends on):

   ```bash
   # A synthetic flight from a dock: aircraft OSD at 2/s plus the dock's own OSD, 5 minutes
   python local/helpers/publish_dji_osd.py synthetic --gateway 4TADKAQ000002J

   # The same flight from DJI Pilot 2 (no gateway field), until stopped
   python local/helpers/publish_dji_osd.py synthetic --duration 0

   # Replay the Mavic 3T capture used by the tests, on a loop
   python local/helpers/publish_dji_osd.py replay app/actions/tests/files/pilot2_mavic3t_capture.ndjson --loop
   ```

   `--help` lists the rest: broker address, centre point, radius, speed, rate and QoS.

4. Trigger `pull_flight_positions` (from the API docs page, or wait for the schedule). Each run
   reports `messages_received`, `aircraft` and `observations_extracted`.

To watch what the broker is delivering: `docker exec mosquitto mosquitto_sub -t 'thing/product/+/osd' -v`.
The broker keeps its session queue in the `mosquitto_data` volume; `docker compose down -v`
clears it.

## Notes

- This example uses configuration from https://stage.gundiservice.org.




