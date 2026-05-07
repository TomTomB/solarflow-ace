from datetime import datetime
import json
import logging
import pathlib
import sys

from jinja2 import DebugUndefined, Environment, FileSystemLoader
from paho.mqtt import client as mqtt_client

from utils import RepeatedTimer, TimewindowBuffer

red = "\x1b[31;20m"
reset = "\x1b[0m"
FORMAT = '%(asctime)s:%(levelname)s: %(message)s'
logging.basicConfig(stream=sys.stdout, level="INFO", format=FORMAT)
log = logging.getLogger("")

HUB2000 = "A8yh63"
AC_MODE_OUTPUT = 2


class Solarflow:
    opts = {"product_id": str, "device_id": str}

    def default_calllback(self):
        log.info("default callback")

    def __init__(self, client: mqtt_client, product_id: str, device_id: str, callback=default_calllback):
        self.client = client
        self.productId = product_id
        self.deviceId = device_id
        self.property_topic = f'iot/{self.productId}/{self.deviceId}/properties/write'

        self.fwVersion = "unknown"
        self.solarInputValues = TimewindowBuffer(minutes=1)
        self.solarInputPower = -1
        self.outputPackPower = 0
        self.packInputPower = 0
        self.outputHomePower = -1
        self.bypass = False
        self.bypass_mode = -1
        self.electricLevel = -1
        self.batteriesSoC = {"none": -1}
        self.batteriesVol = {"none": -1}
        self.outputLimit = -1
        self.inverseMaxPower = 300
        self.batteryLow = -1
        self.batteryHigh = -1
        self.lastSolarInputTS = None

        RepeatedTimer(60, self.update)
        RepeatedTimer(600, self.pushHomeassistantConfig)
        self.pushHomeassistantConfig()
        self.update()

    def __str__(self):
        batteries_soc = "|".join([f'{value:>2}' for value in self.batteriesSoC.values()])
        batteries_vol = "|".join([f'{value:2.1f}' for value in self.batteriesVol.values()])
        average_voltage = sum(self.batteriesVol.values()) / len(self.batteriesVol)
        return ' '.join(f'{red}HUB: \
                        S:{self.solarInputPower:>3.1f}W {self.solarInputValues}, \
                        B:{self.electricLevel:>3}% ({batteries_soc}) low:{self.batteryLow} high:{self.batteryHigh}, \
                        V:{average_voltage:2.1f}V ({batteries_vol}), \
                        C:{self.outputPackPower - self.packInputPower:>4}W, \
                        P:{self.bypass} ({self.bypass_mode}), \
                        H:{self.outputHomePower:>3}W, \
                        L:{self.outputLimit:>3}W{reset}'.split())

    def update(self):
        log.info(f'Triggering telemetry update: iot/{self.productId}/{self.deviceId}/properties/read')
        self.client.publish(f'iot/{self.productId}/{self.deviceId}/properties/read', '{"properties": ["getAll"]}')

    def subscribe(self):
        topics = [
            f'/{self.productId}/{self.deviceId}/properties/report',
            f'solarflow-hub/{self.deviceId}/telemetry/solarInputPower',
            f'solarflow-hub/{self.deviceId}/telemetry/electricLevel',
            f'solarflow-hub/{self.deviceId}/telemetry/outputPackPower',
            f'solarflow-hub/{self.deviceId}/telemetry/packInputPower',
            f'solarflow-hub/{self.deviceId}/telemetry/outputHomePower',
            f'solarflow-hub/{self.deviceId}/telemetry/outputLimit',
            f'solarflow-hub/{self.deviceId}/telemetry/inverseMaxPower',
            f'solarflow-hub/{self.deviceId}/telemetry/masterSoftVersion',
            f'solarflow-hub/{self.deviceId}/telemetry/pass',
            f'solarflow-hub/{self.deviceId}/telemetry/passMode',
            f'solarflow-hub/{self.deviceId}/telemetry/socSet',
            f'solarflow-hub/{self.deviceId}/telemetry/minSoc',
            f'solarflow-hub/{self.deviceId}/telemetry/batteries/+/socLevel',
            f'solarflow-hub/{self.deviceId}/telemetry/batteries/+/totalVol',
        ]
        for topic in topics:
            self.client.subscribe(topic)
            log.info(f'Hub subscribing: {topic}')

    def ready(self):
        return self.electricLevel > -1 and self.solarInputPower > -1

    def timesync(self, ts):
        payload = {
            "zoneOffset": "+00:00",
            "messageId": 123,
            "timestamp": ts,
        }
        self.client.publish(f'iot/{self.productId}/{self.deviceId}/time-sync/reply', json.dumps(payload))

    def pushHomeassistantConfig(self):
        log.info("Publishing Homeassistant templates...")
        hatemplates = [file_path for file_path in pathlib.Path().glob("homeassistant/hub/*.json")]
        environment = Environment(loader=FileSystemLoader("homeassistant/hub/"), undefined=DebugUndefined)

        for hatemplate in hatemplates:
            template = environment.get_template(hatemplate.name)
            cfg_type = hatemplate.name.split(".")[0]
            cfg_name = hatemplate.name.split(".")[1]
            if cfg_name in {"maxTemp", "totalVol", "soh"}:
                for index, (serial, _) in enumerate(self.batteriesVol.items()):
                    if serial == "none":
                        continue
                    hacfg = template.render(
                        product_id=self.productId,
                        device_id=self.deviceId,
                        fw_version=self.fwVersion,
                        battery_serial=serial,
                        battery_index=index + 1,
                    )
                    self.client.publish(
                        f'homeassistant/{cfg_type}/solarflow-hub-{self.deviceId}-{serial}-{cfg_name}/config',
                        hacfg,
                        retain=True,
                    )
            else:
                hacfg = template.render(product_id=self.productId, device_id=self.deviceId, fw_version=self.fwVersion)
                self.client.publish(f'homeassistant/{cfg_type}/solarflow-hub-{self.deviceId}-{cfg_name}/config', hacfg, retain=True)
        log.info(f"Published {len(hatemplates)} Homeassistant templates for Hub.")

    def updSolarInput(self, value: int):
        self.solarInputValues.add(value)
        self.solarInputPower = self.getSolarInputPower()
        self.lastSolarInputTS = datetime.now()

    def updElectricLevel(self, value: int):
        self.electricLevel = value

    def updOutputPack(self, value: int):
        self.outputPackPower = value

    def updPackInput(self, value: int):
        self.packInputPower = value

    def updOutputHome(self, value: int):
        self.outputHomePower = value

    def updOutputLimit(self, value: int):
        self.outputLimit = value

    def updInverseMaxPower(self, value: int):
        self.inverseMaxPower = value

    def updBatterySoC(self, sn: str, value: int):
        self.batteriesSoC.pop("none", None)
        self.batteriesSoC.update({sn: value})

    def updMinSoC(self, value: int):
        self.batteryLow = int(value / 10)

    def updSocSet(self, value: int):
        self.batteryHigh = int(value / 10)

    def updBatteryVol(self, sn: str, value: int):
        self.batteriesVol.pop("none", None)
        self.batteriesVol.update({sn: value / 100})

    def updMasterSoftVersion(self, value: int):
        major = (value & 0xF000) >> 12
        minor = (value & 0x0F00) >> 8
        build = value & 0x00FF
        self.fwVersion = f'{major}.{minor}.{build}'

    def updByPass(self, value: int):
        self.bypass = bool(value)

    def updByPassMode(self, value: int):
        self.bypass_mode = value

    def handleMsg(self, msg):
        if self.productId in msg.topic:
            device_id = msg.topic.split('/')[2]
            payload = json.loads(msg.payload.decode())
            if "properties" in payload:
                for prop, value in payload["properties"].items():
                    self.client.publish(f'solarflow-hub/{device_id}/telemetry/{prop}', value)

            if "packData" in payload:
                for pack in payload["packData"]:
                    serial = pack.pop('sn')
                    for prop, value in pack.items():
                        self.client.publish(f'solarflow-hub/{device_id}/telemetry/batteries/{serial}/{prop}', value)

        if msg.topic.startswith(f'solarflow-hub/{self.deviceId}') and msg.payload:
            if self.lastSolarInputTS and (datetime.now() - self.lastSolarInputTS).total_seconds() > 120:
                self.updSolarInput(0)

            metric = msg.topic.split('/')[-1]
            value = msg.payload.decode()
            match metric:
                case "electricLevel":
                    self.updElectricLevel(int(value))
                case "solarInputPower":
                    self.updSolarInput(int(value))
                case "outputPackPower":
                    self.updOutputPack(int(value))
                case "packInputPower":
                    self.updPackInput(int(value))
                case "outputHomePower":
                    self.updOutputHome(int(value))
                case "outputLimit":
                    self.updOutputLimit(int(value))
                case "inverseMaxPower":
                    self.updInverseMaxPower(int(value))
                case "socLevel":
                    self.updBatterySoC(sn=msg.topic.split('/')[-2], value=int(value))
                case "minSoc":
                    self.updMinSoC(int(value))
                case "socSet":
                    self.updSocSet(int(value))
                case "totalVol":
                    self.updBatteryVol(sn=msg.topic.split('/')[-2], value=int(value))
                case "masterSoftVersion":
                    self.updMasterSoftVersion(int(value))
                case "pass":
                    self.updByPass(int(value))
                case "passMode":
                    self.updByPassMode(int(value))
                case _:
                    if "control" not in msg.topic:
                        log.warning(f'Ignoring solarflow-hub metric: {metric}')

    def setOutputLimit(self, limit: int):
        limit = max(0, int(limit))
        payload = {"properties": {"outputLimit": limit}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Setting solarflow output limit to {limit}W')
        return limit

    def setMasterSwitch(self, state: bool):
        payload = {"properties": {"masterSwitch": 1 if state else 0}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Turning hub master switch {"ON" if state else "OFF"}')

    def setBuzzer(self, state: bool):
        payload = {"properties": {"buzzerSwitch": 1 if state else 0}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Turning hub buzzer {"ON" if state else "OFF"}')

    def setACMode(self):
        payload = {"properties": {"acMode": AC_MODE_OUTPUT}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info('Ensuring hub AC mode is set to output')

    def setAutorecover(self, state: bool):
        payload = {"properties": {"autoRecover": 1 if state else 0}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Turning hub bypass autorecover {"ON" if state else "OFF"}')

    def setBypass(self, state: bool):
        payload = {"properties": {"passMode": 2 if state else 1}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Turning hub bypass {"ON" if state else "OFF"}')

    def getOutputHomePower(self):
        return self.outputHomePower

    def getDischargePower(self):
        return self.packInputPower

    def getSolarInputPower(self):
        return self.solarInputValues.last()

    def getElectricLevel(self):
        return self.electricLevel

    def getInverseMaxPower(self):
        return self.inverseMaxPower

    def getLimit(self):
        return self.outputLimit

    def getBypass(self):
        return self.bypass

    def setBatteryHighSoC(self, level: int) -> int:
        level = int(level)
        payload = {"properties": {"socSet": level * 10}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Setting maximum charge level to {level}%')
        return level

    def setBatteryLowSoC(self, level: int) -> int:
        level = int(level)
        payload = {"properties": {"minSoc": level * 10}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Setting minimum charge level to {level}%')
        return level

    def setInverseMaxPower(self, value: int) -> int:
        value = int(value)
        payload = {"properties": {"inverseMaxPower": value}}
        self.client.publish(self.property_topic, json.dumps(payload))
        log.info(f'Setting inverse max power to {value}W')
        return value
