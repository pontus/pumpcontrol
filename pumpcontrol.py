#!/usr/bin/env python3

import zeroconf
import time
import socket
import requests
import time
import json
import dbm
import random
import dateutil.parser
import datetime
import sys
import logging

MAX_WAIT = 150
# Do not run before local time hours
NOT_BEFORE = 8
# Do not run after local time hours
NOT_AFTER = 19
USE_SUNRISE = False
RUNTIME = 4
PUMPNAME = "Poolpump"
REGION = "SE3"
OVERRIDE_DATA = ""


logger = logging.getLogger()


def override_active():
    current_data = False

    if not OVERRIDE_DATA:
        return (False, False)
    logger.debug(f"Checking override data {OVERRIDE_DATA}\n")

    r = requests.get(OVERRIDE_DATA)
    if r.status_code != 200:
        raise SystemError("override URL set but failed to fetch")
    j = json.loads(r.text.strip('"').encode("ascii").decode("unicode_escape"))
    if isinstance(j, dict):
        j = [j]

    now = datetime.datetime.now()
    for p in j:
        try:
            start = dateutil.parser.parse(p["start"])
            end = dateutil.parser.parse(p["end"])
            if start <= now and now <= end:
                # Matches
                logger.debug(f"Matching override data {p}\n")

                state = False
                if p["state"] == True or p["state"] == "on" or p["state"] == "1":
                    state = True

                return True, state
            if (
                start.day == now.day
                and start.month == now.month
                and start.year == now.year
            ) or (
                end.day == now.day and end.month == now.month and end.year == now.year
            ):
                # Day matches but not within window - have it off
                current_data = True
        except:
            pass

    logger.debug(f"Returning form override check - override is {current_data}\n")

    # Override info but no info for now, leave off
    return (current_data, False)


def setup_logger(
    console_level=logging.DEBUG, file_level=logging.DEBUG, filename="pumpcontrol.log"
):
    h = logging.StreamHandler()
    h.setLevel(console_level)
    logger.addHandler(h)
    f = logging.FileHandler(filename)
    f.setFormatter(logging.Formatter("{asctime} - {levelname} - {message}", style="{"))
    f.setLevel(file_level)
    logger.addHandler(f)

    logger.setLevel(min(file_level, console_level))


class HueController(zeroconf.ServiceListener):
    # Only handle one bridge for now
    _url = None

    def update_service(self, zc: zeroconf.Zeroconf, type_: str, name: str) -> None:
        self.add_service(zc, type_, name)

    def remove_service(self, zc: zeroconf.Zeroconf, type_: str, name: str) -> None:
        self._url = None

    def add_service(self, zc: zeroconf.Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        host = socket.inet_ntoa(info.addresses[0])

        proto = "http"
        if info.port == 443:
            proto = "https"
        self._url = f"{proto}://{host}"
        logger.debug(f"Noticed Hue Controller at {self._url}")

    @property
    def url(self):
        return self._url


def price_apply(p):
    t = dateutil.parser.parse(p["timestamp"])
    offset = time.localtime().tm_gmtoff / 3600
    if (t.hour + offset >= NOT_BEFORE) and (t.hour + offset < NOT_AFTER):
        return True
    return False


def should_run(db):
    t = time.localtime().tm_hour
    if t < NOT_BEFORE or t >= NOT_AFTER:
        return False

    prices = get_prices(db)

    prices.sort(key=lambda x: float(x["value"]))
    logger.debug(f"Prices are {prices}\n")

    interesting_prices = list(filter(price_apply, prices))[:RUNTIME]
    logger.debug(f"After filtering, prices are {interesting_prices}\n")

    # Price timestamps are in UTC
    # We have already checked borders and only need to see i we're
    # in one of the cheap slots
    thishour = datetime.datetime.utcnow().hour

    for p in interesting_prices:
        t = dateutil.parser.parse(p["timestamp"])
        if t.hour == thishour:
            return True
    return False


def get_prices(db):
    key = f"prices{time.strftime('%Y%m%d')}"
    if key in db:
        return json.loads(db[key])

    logger.debug("Fetching spot prices")
    r = requests.get(f"https://spot.utilitarian.io/electricity/SE3/latest")
    if r.status_code != 200:
        raise SystemError("could not fetch electricity info")

    db[key] = r.text
    return json.loads(r.text)


def find_hue():
    "Find a Hue locally through zeroconf"
    zc = zeroconf.Zeroconf()
    listener = HueController()
    _browser = zeroconf.ServiceBrowser(zc, "_hue._tcp.local.", listener)

    count = 0
    while count < MAX_WAIT and not listener.url:
        time.sleep(1)
    zc.close()

    url = listener.url
    if not url:
        raise SystemExit("Did not found Hue bridge")
    return url


def auth_hue(db, url):
    if not "hue_id" in db:
        data = {"devicetype": "Pump controller"}
        r = requests.post(f"{url}/api", json=data, verify=False)
        if r.status_code == 200:
            for p in r.json():
                if "success" in p:
                    db["hue_id"] = p["success"]["username"]

    if "hue_id" not in db:
        raise SystemError("No user in hue")
    hue_id = db["hue_id"]
    if type(hue_id) == type(b""):
        hue_id = hue_id.decode()
    logger.debug(f"Found hue id {hue_id}")
    return hue_id


def find_pump(hue_id, url):
    r = requests.get(f"{url}/api/{hue_id}", verify=False)
    if r.status_code != 200:
        raise SystemError("Getting Hue status failed")
    hue = r.json()
    for p in hue["lights"]:
        if hue["lights"][p]["name"] == PUMPNAME:
            logger.debug(f"Found pump {PUMPNAME}")
            return p
    raise SystemError(f"{PUMPNAME} not found in list of controlled units")


def is_running(hue_id, url, pump):
    r = requests.get(f"{url}/api/{hue_id}/lights/{pump}", verify=False)
    if r.status_code != 200:
        raise SystemError("Getting Hue pumpstatus failed")
    hue = r.json()
    return hue["state"]["on"]


def set_running(hue_id, url, pump, state):
    newstate = {"on": state}
    logger.info(f"Setting state of pump to f{newstate['on']}")
    r = requests.put(
        f"{url}/api/{hue_id}/lights/{pump}/state", json=newstate, verify=False
    )
    if r.status_code != 200:
        raise SystemError("Setting Hue {PUMPNAME} to running: {state} failed")


if __name__ == "__main__":
    setup_logger()

    url = find_hue()
    db = dbm.open("pumpcontrol.db", "c")

    hue_id = auth_hue(db, url)
    pumpid = find_pump(hue_id, url)

    (apply, correct_state) = override_active()
    if not apply:
        correct_state = should_run(db)
    current_state = is_running(hue_id, url, pumpid)

    logger.debug(f"Currently running for {PUMPNAME} is {current_state}\n")
    logger.debug(f"Should be running for {PUMPNAME} is {correct_state}\n")

    if current_state != correct_state:
        logger.debug(f"Need to change state of {PUMPNAME} running to {correct_state}\n")

        set_running(hue_id, url, pumpid, correct_state)
