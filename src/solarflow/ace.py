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


class Ace:
    opts = {"product_id": str, "device_id": str, "device_ip": str}

    def __init__(self, client: mqtt_client, product_id: str, device_id: str, device_ip: str | None = None, telemetry_polling_enabled: bool = True):
        self.client = client
        self.productId = product_id
        self.deviceId = device_id
        self.deviceIp = device_ip
        self.fwVersion = "unknown"

        self.lastSolarInputTS = None
        self.startTS = datetime.now()
        self.solarInputValues = TimewindowBuffer(minutes=1)
        self.solarInputPower = -1
        self.acSwitch = False
        self.dcSwitch = False
        self.gridInputPower = 0
        self.acOutputPower = 0
        self.dcOutputPower = 0
        self.outputPackPower = 0
        self.packInputPower = 0
        self.packState = -1
        self.acMode = -1
        self.lastTelemetryTS = None
        self.isAvailable = None
        self.telemetryPollingEnabled = telemetry_polling_enabled
        self.availabilityTopic = f'solarflow-hub/{self.deviceId}/availability'

        RepeatedTimer(600, self.pushHomeassistantConfig)
        RepeatedTimer(10, self.refreshAvailability)
        self.pushHomeassistantConfig()
        self.refreshAvailability()
        self.update()
        RepeatedTimer(60, self.update)

    def __str__(self):
        return ' '.join(f'{red}ACE: \
                        T:{self.__class__.__name__} \
                        P:{self.productId} \
                        D:{self.deviceId}{reset}'.split())

    def update(self):
        if not self.telemetryPollingEnabled:
            return
        log.info(f'Triggering Ace telemetry update: iot/{self.productId}/{self.deviceId}/properties/read')
        self.client.publish(f'iot/{self.productId}/{self.deviceId}/properties/read', '{"properties": ["getAll"]}')

    def subscribe(self):
        topics = [
            f'/{self.productId}/{self.deviceId}/properties/report',
            f'solarflow-hub/{self.deviceId}/telemetry/acSwitch',
            f'solarflow-hub/{self.deviceId}/telemetry/dcSwitch',
            f'solarflow-hub/{self.deviceId}/telemetry/gridInputPower',
            f'solarflow-hub/{self.deviceId}/telemetry/acOutputPower',
            f'solarflow-hub/{self.deviceId}/telemetry/dcOutputPower',
            f'solarflow-hub/{self.deviceId}/telemetry/outputPackPower',
            f'solarflow-hub/{self.deviceId}/telemetry/packInputPower',
            f'solarflow-hub/{self.deviceId}/telemetry/packState',
            f'solarflow-hub/{self.deviceId}/telemetry/acMode',
            f'solarflow-hub/{self.deviceId}/telemetry/masterSoftVersion',
            f'solarflow-hub/{self.deviceId}/telemetry/solarInputPower',
        ]

        for topic in topics:
            self.client.subscribe(topic)
            log.info(f'Ace subscribing: {topic}')

    def pushHomeassistantConfig(self):
        log.info("Publishing Homeassistant templates...")
        hatemplates = [file_path for file_path in pathlib.Path().glob("homeassistant/ace/*.json")]
        environment = Environment(loader=FileSystemLoader("homeassistant/ace/"), undefined=DebugUndefined)

        for hatemplate in hatemplates:
            template = environment.get_template(hatemplate.name)
            cfg_type = hatemplate.name.split(".")[0]
            cfg_name = hatemplate.name.split(".")[1]
            hacfg = template.render(product_id=self.productId, device_id=self.deviceId, fw_version=self.fwVersion)
            hacfg = self.injectAvailability(hacfg)
            self.client.publish(f'homeassistant/{cfg_type}/ace-{self.deviceId}-{cfg_name}/config', hacfg, retain=True)

        log.info(f"Published {len(hatemplates)} Homeassistant templates for Ace.")

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
        log.info(f'Published Ace availability {payload} on {self.availabilityTopic}')

    def refreshAvailability(self):
        telemetry_is_fresh = self.lastTelemetryTS is not None and (datetime.now() - self.lastTelemetryTS).total_seconds() <= 120
        ping_is_reachable = ping_host(self.deviceIp) if self.deviceIp else False
        self.publishAvailability(telemetry_is_fresh or ping_is_reachable)

    def setTelemetryPolling(self, state: bool):
        if self.telemetryPollingEnabled == state:
            return

        self.telemetryPollingEnabled = state
        log.info(f'Ace telemetry polling {"enabled" if state else "paused"}')

        if state:
            self.update()

    def handleMsg(self, msg):
        if self.productId in msg.topic:
            device_id = msg.topic.split('/')[2]
            payload = json.loads(msg.payload.decode())
            if "properties" in payload:
                for prop, value in payload["properties"].items():
                    self.client.publish(f'solarflow-hub/{device_id}/telemetry/{prop}', value)
                self.lastTelemetryTS = datetime.now()
                self.publishAvailability(True)

        if msg.topic.startswith(f'solarflow-hub/{self.deviceId}') and msg.payload:
            self.lastTelemetryTS = datetime.now()
            self.publishAvailability(True)
            last_known = self.lastSolarInputTS or self.startTS
            if (datetime.now() - last_known).total_seconds() > 120:
                self.updSolarInput(0)

            metric = msg.topic.split('/')[-1]
            value = msg.payload.decode()
            match metric:
                case "solarInputPower":
                    self.updSolarInput(int(value))
                case "masterSoftVersion":
                    self.updMasterFirmwareVersion(int(value))
                case "masterFirmwareVersion":
                    self.updMasterFirmwareVersion(int(value))
                case "acSwitch":
                    self.acSwitch = bool(int(value))
                case "dcSwitch":
                    self.dcSwitch = bool(int(value))
                case "gridInputPower":
                    self.gridInputPower = int(value)
                case "acOutputPower":
                    self.acOutputPower = int(value)
                case "dcOutputPower":
                    self.dcOutputPower = int(value)
                case "outputPackPower":
                    self.outputPackPower = int(value)
                case "packInputPower":
                    self.packInputPower = int(value)
                case "packState":
                    self.packState = int(value)
                case "acMode":
                    self.acMode = int(value)
                case _:
                    if "control" not in msg.topic:
                        log.warning(f'Ignoring solarflow-hub metric: {metric}')

    def updMasterFirmwareVersion(self, value: int):
        major = (value & 0xF000) >> 12
        minor = (value & 0x0F00) >> 8
        build = value & 0x00FF
        self.fwVersion = f'{major}.{minor}.{build}'

    def updSolarInput(self, value: int):
        self.solarInputValues.add(value)
        self.solarInputPower = self.solarInputValues.last()
        self.lastSolarInputTS = datetime.now()
