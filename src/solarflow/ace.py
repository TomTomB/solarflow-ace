from paho.mqtt import client as mqtt_client
from datetime import datetime
import logging
import json
import sys
import pathlib
from jinja2 import Environment, FileSystemLoader, DebugUndefined
from utils import TimewindowBuffer, RepeatedTimer, str2bool

TRIGGER_DIFF = 30

red = "\x1b[31;20m"
reset = "\x1b[0m"
FORMAT = '%(asctime)s:%(levelname)s: %(message)s'
logging.basicConfig(stream=sys.stdout, level="INFO", format=FORMAT)
log = logging.getLogger("")

class Ace:
    opts = {"product_id":str, "device_id":str}

    def default_calllback(self):
        log.info("default callback")

    def __init__(self, client: mqtt_client, product_id:str, device_id:str, callback = default_calllback):
        self.client = client
        self.productId = product_id
        self.deviceId = device_id
        self.property_topic = f'iot/{self.productId}/{self.deviceId}/properties/write'
        self.fwVersion = "unknown"
        
        self.lastSolarInputTS = None    # time of the last received solar input value
        self.solarInputValues = TimewindowBuffer(minutes=1)
        self.solarInputPower = -1
                
        haconfig = RepeatedTimer(600, self.pushHomeassistantConfig)
        self.pushHomeassistantConfig()        
                
        self.trigger_callback = callback
        self.update()
        
        updater = RepeatedTimer(60, self.update)

    def __str__(self):
        return ' '.join(f'{red}ACE: \
                        T:{self.__class__.__name__} \
                        P:{self.productId} \
                        D:{self.deviceId}{reset}'.split())
                            
    def update(self):
        log.info(f'Triggering Ace telemetry update: iot/{self.productId}/{self.deviceId}/properties/read')
        self.client.publish(f'iot/{self.productId}/{self.deviceId}/properties/read','{"properties": ["getAll"]}')

    def subscribe(self):
        topics = [
            f'/{self.productId}/{self.deviceId}/properties/report',
        ]
        for t in topics:
            self.client.subscribe(t)
            log.info(f'Ace subscribing: {t}')
            
    def pushHomeassistantConfig(self):
        log.info("Publishing Homeassistant templates...")
        hatemplates = [f for f in pathlib.Path().glob("homeassistant/ace/*.json")]
        environment = Environment(loader=FileSystemLoader("homeassistant/ace/"), undefined=DebugUndefined)

        for hatemplate in hatemplates:
            template = environment.get_template(hatemplate.name)
            cfg_type = hatemplate.name.split(".")[0]
            cfg_name = hatemplate.name.split(".")[1]

            hacfg = template.render(product_id=self.productId, device_id=self.deviceId, fw_version=self.fwVersion)
            self.client.publish(f'homeassistant/{cfg_type}/ace-{self.deviceId}-{cfg_name}/config',hacfg,retain=True)
            
        log.info(f"Published {len(hatemplates)} Homeassistant templates for Ace.")

    # handle content of mqtt message and update properties accordingly
    def handleMsg(self, msg):
        # transform the original messages sent by the SF hub into a better readable format
        if self.productId in msg.topic:
            device_id = msg.topic.split('/')[2]
            payload = json.loads(msg.payload.decode())
            if "properties" in payload:
                props = payload["properties"]
                for prop, val in props.items():
                    self.client.publish(f'solarflow-hub/{device_id}/telemetry/{prop}',val)

        if msg.topic.startswith(f'solarflow-hub/{self.deviceId}') and msg.payload:
            # check if we got regular updates on solarInputPower
            # if we haven't received any update on solarInputPower for 120s
            # we assume it's not producing and inject 0
            now = datetime.now()
            if self.lastSolarInputTS:
                diff = now - self.lastSolarInputTS
                seconds = diff.total_seconds()
                if seconds > 120:
                    self.updSolarInput(0)

            metric = msg.topic.split('/')[-1]
            value = msg.payload.decode()
            match metric:
                case "solarInputPower":
                    self.updSolarInput(int(value))
                case "masterFirmwareVersion":
                    self.updMasterFirmwareVersion(value=int(value))
                case _:
                    if not "control" in msg.topic:
                        log.warning(f'Ignoring solarflow-hub metric: {metric}')

    def getSolarInputPower(self):
        return self.solarInputValues.last()

    def updMasterFirmwareVersion(self, value:int):
        major = (value & 0xf000) >> 12
        minor = (value & 0x0f00) >> 8
        build = (value & 0x00ff)
        self.fwVersion = f'{major}.{minor}.{build}'
                        
    def updSolarInput(self, value:int):
        self.solarInputValues.add(value)
        self.solarInputPower = self.getSolarInputPower()
        self.lastSolarInputTS = datetime.now()

        previous = self.solarInputValues.previous()
        if abs(previous - self.getSolarInputPower()) >= TRIGGER_DIFF:
            log.info(f'Ace triggers limit function: {previous} -> {self.getSolarInputPower()}: {"executed" if self.trigger_callback(self.client) else "skipped"}')
            self.last_trigger_value = self.getSolarInputPower()

    def setBuzzer(self, state: bool):
        buzzer = {"properties": { "buzzerSwitch": 0 if not state else 1 }}
        self.client.publish(self.property_topic,json.dumps(buzzer))
        log.info(f'Turning hub buzzer {"ON" if state else "OFF"}')