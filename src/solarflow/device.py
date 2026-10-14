'''Common transport logic of a Zendure device: poll it, republish its reports as plain
telemetry topics, keep its availability topic current and announce it to Home Assistant.'''
from datetime import datetime
import json
import logging
import pathlib

from jinja2 import DebugUndefined, Environment, FileSystemLoader

from utils import (AVAILABILITY_PAYLOAD_OFFLINE, AVAILABILITY_PAYLOAD_ONLINE,
                   BRIDGE_AVAILABILITY_TOPIC, TELEMETRY_EXPIRE_AFTER_SECONDS,
                   RepeatedTimer)

log = logging.getLogger(__name__)

TEMPLATE_ROOT = pathlib.Path(__file__).resolve().parent / 'homeassistant'

# A device counts as available while its last report is at most this old.
AVAILABILITY_TIMEOUT_SECONDS = 120


def firmware_version(value) -> str:
    '''Zendure packs the firmware version into one integer: 4 bits major, 4 bits minor,
    8 bits build.'''
    value = int(value)
    return f'{(value & 0xF000) >> 12}.{(value & 0x0F00) >> 8}.{value & 0x00FF}'


def mqtt_payload(value):
    '''paho only accepts str, bytes, int, float and None. A report value that is a list or
    an object would otherwise abort the whole report.'''
    if value is None or isinstance(value, (str, bytes, bytearray, int, float)):
        return value
    return json.dumps(value)


class Device:
    name = 'Device'          # used in log lines
    template_dir = ''        # subdirectory of homeassistant/
    discovery_prefix = ''    # object id prefix of the discovery topics

    def __init__(self, client, product_id: str, device_id: str, telemetry_polling_enabled: bool = True):
        self.client = client
        self.productId = product_id
        self.deviceId = device_id
        self.fwVersion = 'unknown'
        self.lastTelemetryTS = None
        self.isAvailable = None
        self.telemetryPollingEnabled = telemetry_polling_enabled
        self.availabilityTopic = f'solarflow-hub/{self.deviceId}/availability'
        self.reportTopic = f'/{self.productId}/{self.deviceId}/properties/report'
        self.telemetryPrefix = f'solarflow-hub/{self.deviceId}/telemetry'
        self.templates = Environment(loader=FileSystemLoader(TEMPLATE_ROOT / self.template_dir),
                                     undefined=DebugUndefined)

        RepeatedTimer(600, self.pushHomeassistantConfig)
        RepeatedTimer(10, self.refreshAvailability)
        RepeatedTimer(60, self.update)
        self.pushHomeassistantConfig()
        self.refreshAvailability()
        self.update()

    def topics(self) -> list[str]:
        return [self.reportTopic]

    def update(self):
        if not self.telemetryPollingEnabled:
            return
        topic = f'iot/{self.productId}/{self.deviceId}/properties/read'
        log.debug(f'Triggering {self.name} telemetry update: {topic}')
        self.client.publish(topic, '{"properties": ["getAll"]}')

    def setTelemetryPolling(self, state: bool):
        if self.telemetryPollingEnabled == state:
            return
        self.telemetryPollingEnabled = state
        log.info(f'{self.name} telemetry polling {"enabled" if state else "paused"}')
        if state:
            self.update()

    # --- Home Assistant discovery -------------------------------------------------------

    def templateInstances(self, cfg_name: str):
        '''Yields (object id suffix, extra template variables) for one template. One instance
        per template by default; the hub renders its battery templates once per pack.'''
        yield cfg_name, {}

    def discoveryConfigs(self):
        '''Yields (topic, payload) for every discovery config of this device.'''
        for path in sorted((TEMPLATE_ROOT / self.template_dir).glob('*.json')):
            cfg_type, cfg_name = path.name.split('.')[:2]
            template = self.templates.get_template(path.name)
            for suffix, extra in self.templateInstances(cfg_name):
                payload = template.render(product_id=self.productId, device_id=self.deviceId,
                                          fw_version=self.fwVersion, **extra)
                yield (f'homeassistant/{cfg_type}/{self.discovery_prefix}-{self.deviceId}-{suffix}/config',
                       self.injectAvailability(payload, cfg_type))

    def pushHomeassistantConfig(self):
        count = 0
        for topic, payload in self.discoveryConfigs():
            self.client.publish(topic, payload, retain=True)
            count += 1
        log.debug(f'Published {count} Home Assistant discovery configs for {self.name}.')

    def injectAvailability(self, config_payload: str, cfg_type: str | None = None) -> str:
        config = json.loads(config_payload)

        # Two availability sources, both of which must be online: the device (fed by its own
        # reports) and the bridge process (fed by the last will). Either one going away has
        # to mark the entity unavailable, hence availability_mode 'all'.
        for legacy_key in ('avty_t', 'avty', 'pl_avail', 'pl_not_avail'):
            config.pop(legacy_key, None)
        config.update({
            'availability': [
                {
                    'topic': self.availabilityTopic,
                    'payload_available': AVAILABILITY_PAYLOAD_ONLINE,
                    'payload_not_available': AVAILABILITY_PAYLOAD_OFFLINE,
                },
                {
                    'topic': BRIDGE_AVAILABILITY_TOPIC,
                    'payload_available': AVAILABILITY_PAYLOAD_ONLINE,
                    'payload_not_available': AVAILABILITY_PAYLOAD_OFFLINE,
                },
            ],
            'availability_mode': 'all',
        })

        # Only sensors support expire_after. Controls must stay writable even when the
        # device is quiet, so they deliberately do not get it.
        if cfg_type in ('sensor', 'binary_sensor'):
            config['expire_after'] = TELEMETRY_EXPIRE_AFTER_SECONDS

        return json.dumps(config)

    # --- availability ---------------------------------------------------------------------

    def publishAvailability(self, available: bool):
        if self.isAvailable == available:
            return
        self.isAvailable = available
        payload = AVAILABILITY_PAYLOAD_ONLINE if available else AVAILABILITY_PAYLOAD_OFFLINE
        self.client.publish(self.availabilityTopic, payload, retain=True)
        log.info(f'Published {self.name} availability {payload} on {self.availabilityTopic}')

    def refreshAvailability(self):
        self.publishAvailability(
            self.lastTelemetryTS is not None
            and (datetime.now() - self.lastTelemetryTS).total_seconds() <= AVAILABILITY_TIMEOUT_SECONDS)

    def markAlive(self):
        self.lastTelemetryTS = datetime.now()
        self.publishAvailability(True)

    # --- reports ----------------------------------------------------------------------------

    def handleMsg(self, msg):
        if msg.topic != self.reportTopic:
            return
        try:
            payload = json.loads(msg.payload)
        except ValueError as exc:
            log.warning(f'{self.name}: ignoring unparsable report on {msg.topic}: {exc}')
            return
        if not isinstance(payload, dict):
            log.warning(f'{self.name}: ignoring report on {msg.topic} that is not a JSON object')
            return
        self.handleReport(payload)

    def handleReport(self, payload: dict):
        properties = payload.get('properties')
        if isinstance(properties, dict):
            for prop, value in properties.items():
                self.client.publish(f'{self.telemetryPrefix}/{prop}', mqtt_payload(value))
            if 'masterSoftVersion' in properties:
                try:
                    self.fwVersion = firmware_version(properties['masterSoftVersion'])
                except (TypeError, ValueError):
                    log.warning(f'{self.name}: unexpected masterSoftVersion {properties["masterSoftVersion"]!r}')
            self.markAlive()
