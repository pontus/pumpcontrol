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
import typing
import logging

MAX_WAIT = 150
# Do not run before local time hours
DEFAULT_NOT_BEFORE = 8
# Do not run after local time hours
DEFAULT_NOT_AFTER = 19
USE_SUNRISE = False
DEFAULT_RUNTIME = 4
DEFAULT_OTHERSADD = 4

PUMPNAME = "Poolpump"
REGION = "SE3"
CONTROL_BASE = (
    "https://poolpumpcontrol-d4382-default-rtdb.europe-west1.firebasedatabase.app"
)

defaults = {
    "notafter": DEFAULT_NOT_AFTER,
    "notbefore": DEFAULT_NOT_BEFORE,
    "runtime": DEFAULT_RUNTIME,
    "othersadd": DEFAULT_OTHERSADD,
}


class Price(typing.TypedDict):
    value: float
    timestamp: datetime.datetime


class Override(typing.TypedDict):
    start: str
    end: str
    state: bool


class Config(typing.TypedDict):
    notafter: float
    notbefore: float
    runtime: float
    othersadd: float


OverrideConfig: typing.TypeAlias = "typing.List[Override]"
Database: typing.TypeAlias = "dbm._Database"


class AllConfig(typing.TypedDict):
    config: Config
    override: list[Override]


logger = logging.getLogger()


def get_config() -> AllConfig:
    if not CONTROL_BASE:
        return typing.cast(AllConfig, {"config": defaults, "override": []})

    logger.debug(f"Checking control data {CONTROL_BASE}/.json\n")

    r = requests.get(f"{CONTROL_BASE}/.json")
    if r.status_code != 200:
        raise SystemError("override URL set but failed to fetch")
    j = json.loads(r.text.strip('"').encode("ascii").decode("unicode_escape"))

    if not "config" in j:
        j["config"] = defaults

    for p in ("notafter", "notbefore", "runtime", "othersadd"):
        if not p in j["config"]:
            j["config"][p] = defaults[p]

    return j


def override_active(config: OverrideConfig) -> typing.Tuple[bool, bool]:
    current_data = False

    
    if not len (config):
        return (False, False)

    now = datetime.datetime.now()
    for p in config:
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

    logger.debug(f"Returning from override check - override is {current_data}\n")

    # Override info but no info for now, leave off
    return (current_data, False)


def setup_logger(
    console_level: int = logging.DEBUG,
    file_level: int = logging.DEBUG,
    filename: str = "pumpcontrol.log",
) -> None:
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
        info = typing.cast(zeroconf.ServiceInfo, zc.get_service_info(type_, name))
        host = socket.inet_ntoa(info.addresses[0])

        proto = "http"
        if info.port == 443:
            proto = "https"
        self._url = f"{proto}://{host}"
        logger.debug(f"Noticed Hue Controller at {self._url}")

    @property
    def url(self) -> str:
        return typing.cast(str, self._url)


def price_modif(p: Price, config: Config) -> Price:
    t = p["timestamp"]
    if (t.hour >= config["notbefore"]) and (t.hour < config["notafter"]):
        return p

    ## Allow other hours but add some extra charge
    p["value"] = float(p["value"]) + config["othersadd"]
    return p


def price_apply(p: Price) -> bool:
    d = time.localtime().tm_mday
    if p["timestamp"].day != d:
        return False
    return False

def should_run(db: Database, config: Config) -> bool:
    t = time.localtime().tm_hour

    prices = get_prices(db)
    prices = list(filter(price_apply, map(lambda x: price_modif(x, config), prices)))

    prices.sort(key=lambda x: float(x["value"]))
    logger.debug(f"Prices are {prices}\n")

    interesting_prices = prices[: int(config["runtime"])]
    logger.debug(f"After filtering, prices are {interesting_prices}\n")

    # We have already checked borders and only need to see i we're
    # in one of the cheap slots

    for p in interesting_prices:
        if p["timestamp"].hour == t:
            return True
    return False


def get_prices(db: Database) -> list[Price]:
    key = f"prices{time.strftime('%Y%m%d')}"
    if key in db:
        data = db[key]
    else:
        logger.debug("Fetching spot prices")
        r = requests.get(f"https://spot.utilitarian.io/electricity/SE3/latest")
        if r.status_code != 200:
            raise SystemError("could not fetch electricity info")

        db[key] = r.text
        data = r.text.encode("ascii")

    def fix_entry(x: typing.Dict[str, str]) -> Price:
        r = Price(
            value=float(x["value"]),
            timestamp=dateutil.parser.parse(x["timestamp"]).astimezone(),
        )
        return r

    fixed = list(map(fix_entry, json.loads(data)))

    return fixed


def find_hue() -> str:
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


def auth_hue(db: Database, url: str) -> str:
    if not "hue_id" in db:
        data = {"devicetype": "Pump controller"}
        r = requests.post(f"{url}/api", json=data, verify=False)
        if r.status_code == 200:
            for p in r.json():
                if "success" in p:
                    db["hue_id"] = bytes(p["success"]["username"], "ascii")

    if "hue_id" not in db:
        raise SystemError("No user in hue")

    id = db["hue_id"]
    hue_id = id.decode()

    logger.debug(f"Found hue id {hue_id}")
    return hue_id


def find_pump(hue_id: str, url: str) -> str:
    r = requests.get(f"{url}/api/{hue_id}", verify=False)
    if r.status_code != 200:
        raise SystemError("Getting Hue status failed")
    hue = r.json()
    for p in hue["lights"]:
        if hue["lights"][p]["name"] == PUMPNAME:
            logger.debug(f"Found pump {PUMPNAME}")
            return p
    raise SystemError(f"{PUMPNAME} not found in list of controlled units")


def is_running(hue_id: str, url: str, pump: str) -> bool:
    r = requests.get(f"{url}/api/{hue_id}/lights/{pump}", verify=False)
    if r.status_code != 200:
        raise SystemError("Getting Hue pumpstatus failed")
    hue = r.json()
    return hue["state"]["on"]


def set_running(hue_id: str, url: str, pump: str, state: bool) -> None:
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

    allconfig = get_config()
    (apply, correct_state) = override_active(allconfig["override"])
    if not apply:
        correct_state = should_run(db, allconfig["config"])
    current_state = is_running(hue_id, url, pumpid)

    logger.debug(f"Currently running for {PUMPNAME} is {current_state}\n")
    logger.debug(f"Should be running for {PUMPNAME} is {correct_state}\n")

    if current_state != correct_state:
        logger.debug(f"Need to change state of {PUMPNAME} running to {correct_state}\n")

        set_running(hue_id, url, pumpid, correct_state)
