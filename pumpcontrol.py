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

MAX_WAIT = 150
# Do not run before local time hours
NOT_BEFORE = 8
# Do not run after local time hours
NOT_AFTER = 19
USE_SUNRISE = False
RUNTIME = 4
PUMPNAME = "Poolpump"
REGION = "SE3"


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
    sys.stderr(f"Prices are {prices}\n")

    interesting_prices = list(filter(price_apply, prices))[:RUNTIME]
    sys.stderr(f"After filtering, prices are {interesting_prices}\n")

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

    r = requests.get(f"https://spot.utilitarian.io/electricity/SE3/latest")
    if r.status_code != 200:
        raise SystemError("could not fetch electricity info")

    db[key] = r.text
    return json.loads(r.text)


def find_hue():
    "Find a Hue locally through zeroconf"
    zc = zeroconf.Zeroconf()
    listener = HueController()
    browser = zeroconf.ServiceBrowser(zc, "_hue._tcp.local.", listener)

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
    return hue_id


def find_pump(hue_id, url):
    r = requests.get(f"{url}/api/{hue_id}", verify=False)
    if r.status_code != 200:
        raise SystemError("Getting Hue status failed")
    hue = r.json()
    for p in hue["lights"]:
        if hue["lights"][p]["name"] == PUMPNAME:
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
    r = requests.put(f"{url}/api/{hue_id}/lights/{pump}/state", json=newstate, verify=False)
    if r.status_code != 200:
        raise SystemError("Setting Hue {PUMPNAME} to running: {state} failed")

if __name__ == "__main__":
    url = find_hue()
    db = dbm.open("pumpcontrol.db", "c")

    hue_id = auth_hue(db, url)
    pumpid = find_pump(hue_id, url)

    correct_state = should_run(db)
    current_state = is_running(hue_id, url, pumpid)

    sys.stderr.write(f"Currently running for {PUMPNAME} is {current_state}\n")
    sys.stderr.write(f"Should be running for {PUMPNAME} is {correct_state}\n")
    
    if current_state != correct_state:
        sys.stderr.write(f"Setting {PUMPNAME} running to {correct_state}\n")

        set_running(hue_id, url, pumpid, correct_state)

    