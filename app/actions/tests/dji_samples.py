import json

AIRCRAFT_SN = "1581F5BKD225D00BH5RF"
DOCK_SN = "4TADKAQ000002J"


def aircraft_osd(timestamp_ms=1761573600000, lat=-1.2921, lon=36.8219, **data):
    return json.dumps({
        "tid": "6a7bfe89-c386-4043-b600-b518e10096cc",
        "bid": "42a19f36-5117-4520-bd13-fd61d818d52e",
        "timestamp": timestamp_ms,
        "gateway": DOCK_SN,
        "data": {
            "latitude": lat,
            "longitude": lon,
            "height": 1712.4,
            "elevation": 85.2,
            "attitude_head": 92.1,
            "horizontal_speed": 11.3,
            "vertical_speed": -0.2,
            "mode_code": 5,
            "battery": {"capacity_percent": 74},
            "position_state": {"gps_number": 21, "is_fixed": 2},
            **data,
        },
    }).encode()


def dock_osd(timestamp_ms=1761573600000):
    return json.dumps({
        "timestamp": timestamp_ms,
        "gateway": DOCK_SN,
        "data": {"latitude": -1.30, "longitude": 36.80, "height": 1630.0, "mode_code": 0},
    }).encode()
