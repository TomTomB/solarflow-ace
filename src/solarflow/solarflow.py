from datetime import datetime
import json
import logging
import pathlib
import sys

from jinja2 import DebugUndefined, Environment, FileSystemLoader
from paho.mqtt import client as mqtt_client

from utils import RepeatedTimer, TimewindowBuffer, ping_host

red = "\x1b[31;20m"
reset = "\x1b[0m"
FORMAT = '%(asctime)s:%(levelname)s: %(message)s'
logging.basicConfig(stream=sys.stdout, level="INFO", format=FORMAT)
log = logging.getLogger("")

class Solarflow:
    opts = {"product_id": str, "device_id": str, "device_ip": str}

    def __init__(self, client: mqtt_client, product_id: str, device_id: str, device_ip: str | None = None, telemetry_polling_enabled: bool = True):
        self.client = client
        self.productId = product_id
        self.deviceId = device_id
        self.deviceIp = device_ip

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
        self.lastTelemetryTS = None
        self.isAvailable = None
        self.telemetryPollingEnabled = telemetry_polling_enabled
        self.availabilityTopic = f'solarflow-hub/{self.deviceId}/availability'

        RepeatedTimer(60, self.update)
        RepeatedTimer(600, self.pushHomeassistantConfig)
        RepeatedTimer(10, self.refreshAvailability)
        self.pushHomeassistantConfig()
        self.refreshAvailability()
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
        if not self.telemetryPollingEnabled:
            return
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
                    hacfg = self.injectAvailability(hacfg)
                    self.client.publish(
                        f'homeassistant/{cfg_type}/solarflow-hub-{self.deviceId}-{serial}-{cfg_name}/config',
                        hacfg,
                        retain=True,
                    )
            else:
                hacfg = template.render(product_id=self.productId, device_id=self.deviceId, fw_version=self.fwVersion)
                hacfg = self.injectAvailability(hacfg)
                self.client.publish(f'homeassistant/{cfg_type}/solarflow-hub-{self.deviceId}-{cfg_name}/config', hacfg, retain=True)
        log.info(f"Published {len(hatemplates)} Homeassistant templates for Hub.")

    def injectAvailability(self, config_payload: str) -> str:
        config = json.loads(config_payload)
        config.update({
            'avty_t': self.availabilityTopic,
            'pl_avail': 'online',
            'pl_not_avail': 'offline',
        })
        return json.dumps(config)

    def publishAvailability(self, available: bool):
        payload = 'online' if available else 'offline'
        if self.isAvailable is not None and self.isAvailable == available:
            return

        self.isAvailable = available
        self.client.publish(self.availabilityTopic, payload, retain=True)
        log.info(f'Published Hub availability {payload} on {self.availabilityTopic}')

    def refreshAvailability(self):
        telemetry_is_fresh = self.lastTelemetryTS is not None and (datetime.now() - self.lastTelemetryTS).total_seconds() <= 120
        ping_is_reachable = ping_host(self.deviceIp) if self.deviceIp else False
        self.publishAvailability(telemetry_is_fresh or ping_is_reachable)

    def setTelemetryPolling(self, state: bool):
        if self.telemetryPollingEnabled == state:
            return

        self.telemetryPollingEnabled = state
        log.info(f'Hub telemetry polling {"enabled" if state else "paused"}')

        if state:
            self.update()

    def updSolarInput(self, value: int):
        self.solarInputValues.add(value)
        self.solarInputPower = self.solarInputValues.last()
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
                self.lastTelemetryTS = datetime.now()
                self.publishAvailability(True)

            if "packData" in payload:
                for pack in payload["packData"]:
                    serial = pack.pop('sn')
                    for prop, value in pack.items():
                        self.client.publish(f'solarflow-hub/{device_id}/telemetry/batteries/{serial}/{prop}', value)
                self.lastTelemetryTS = datetime.now()
                self.publishAvailability(True)

        if msg.topic.startswith(f'solarflow-hub/{self.deviceId}') and msg.payload:
            self.lastTelemetryTS = datetime.now()
            self.publishAvailability(True)
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

