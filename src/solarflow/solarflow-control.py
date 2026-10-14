'''Transport between a Zendure Solarflow Hub 2000 + Ace 1500 on a local MQTT broker and
Home Assistant. No control logic: the regulation lives in Home Assistant.'''
import configparser
import logging
import os
import signal
import sys
import time
from pathlib import Path

from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

from ace import Ace
from solarflow import Solarflow
from utils import (AVAILABILITY_PAYLOAD_OFFLINE, AVAILABILITY_PAYLOAD_ONLINE,
                   BRIDGE_AVAILABILITY_TOPIC, str2bool)

logging.basicConfig(stream=sys.stdout, level='INFO', format='%(asctime)s:%(levelname)s: %(message)s')
log = logging.getLogger('solarflow-control')

DEFAULT_CONFIG = Path(__file__).resolve().parent / 'config.ini'
SUPPORTED_HUB_PRODUCT_ID = 'A8yh63'

# Fixed client id with a clean session: a restart takes over the previous connection
# instead of leaving one more orphaned persistent session on the broker, and every
# subscription is renewed in on_connect.
CLIENT_ID = 'solarflow-control'

TRANSPORT_TELEMETRY_POLLING_CONTROL_TOPIC = 'solarflow-transport/control/telemetryPolling'
TRANSPORT_TELEMETRY_POLLING_STATE_TOPIC = 'solarflow-transport/telemetry/telemetryPolling'


class ConfigError(Exception):
    pass


def load_config(path: Path) -> dict:
    parser = configparser.ConfigParser()
    try:
        with path.open() as config_file:
            parser.read_file(config_file)
    except OSError as exc:
        raise ConfigError(f'cannot read configuration file {path}: {exc}') from exc
    except configparser.Error as exc:
        raise ConfigError(f'invalid configuration file {path}: {exc}') from exc

    def get(section, option, required=True):
        try:
            value = parser.get(section, option).strip()
        except configparser.NoSectionError:
            raise ConfigError(f'{path}: section [{section}] is missing') from None
        except configparser.NoOptionError:
            if required:
                raise ConfigError(f'{path}: option "{option}" in section [{section}] is missing') from None
            return None
        if required and not value:
            raise ConfigError(f'{path}: option "{option}" in section [{section}] is empty')
        return value or None

    port = get('mqtt', 'mqtt_port', required=False) or '1883'
    try:
        port = int(port)
    except ValueError:
        raise ConfigError(f'{path}: mqtt_port in section [mqtt] must be a number, got {port!r}') from None

    config = {
        'hub': {'product_id': get('solarflow', 'product_id'), 'device_id': get('solarflow', 'device_id')},
        'ace': {'product_id': get('ace', 'product_id'), 'device_id': get('ace', 'device_id')},
        'mqtt': {
            'host': get('mqtt', 'mqtt_host'),
            'port': port,
            'user': get('mqtt', 'mqtt_user', required=False),
            'password': get('mqtt', 'mqtt_pwd', required=False),
        },
    }
    if config['hub']['product_id'] != SUPPORTED_HUB_PRODUCT_ID:
        raise ConfigError(f'{path}: unsupported product_id {config["hub"]["product_id"]!r} in [solarflow]; '
                          f'only the Hub 2000 ({SUPPORTED_HUB_PRODUCT_ID}) is supported')
    return config


def log_build_info():
    log.info(f'Solarflow Control version: {os.environ.get("APP_VERSION", "dev")}')
    log.info(f'Solarflow Control commit: {os.environ.get("GIT_COMMIT", "unknown")} '
             f'({os.environ.get("GIT_BRANCH", "unknown")})')
    log.info(f'Solarflow Control build date: {os.environ.get("BUILD_DATE", "unknown")}')


def publish_transport_polling_state(client, enabled: bool):
    client.publish(TRANSPORT_TELEMETRY_POLLING_STATE_TOPIC, 'ON' if enabled else 'OFF', retain=True)


def apply_transport_polling(client, userdata: dict, enabled: bool):
    previous = userdata['polling']
    userdata['polling'] = enabled
    for device in userdata['devices']:
        device.setTelemetryPolling(enabled)
    if userdata['devices']:
        publish_transport_polling_state(client, enabled)
    if previous != enabled:
        log.info(f'Transport telemetry polling control received: {"ON" if enabled else "OFF"}')


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        log.error(f'Failed to connect to MQTT broker: {reason_code}')
        return
    log.info('Connected to MQTT broker')
    # Counterpart to the last will. Retained, so Home Assistant sees it no matter when it
    # subscribes.
    client.publish(BRIDGE_AVAILABILITY_TOPIC, AVAILABILITY_PAYLOAD_ONLINE, retain=True)
    # A broker that lost its retained messages would otherwise never hear the device
    # availability again until it changes; republish it with the next refresh.
    for device in userdata['devices']:
        device.isAvailable = None
    # Clean session: the broker forgets all subscriptions on every reconnect.
    topics = [TRANSPORT_TELEMETRY_POLLING_CONTROL_TOPIC] + userdata['device_topics']
    client.subscribe([(topic, 0) for topic in topics])
    log.info(f'Subscribed to {", ".join(topics)}')


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    if reason_code.is_failure:
        log.error(f'Disconnected from MQTT broker: {reason_code}')
    else:
        log.info('Disconnected from MQTT broker')


def on_message(client, userdata, msg):
    # One bad message must not take the bridge down, and one failing device must not
    # starve the other.
    if msg.topic == TRANSPORT_TELEMETRY_POLLING_CONTROL_TOPIC:
        try:
            enabled = str2bool(msg.payload.decode(errors='replace')) if msg.payload else True
            apply_transport_polling(client, userdata, enabled)
        except Exception:
            log.exception(f'Error while handling message on {msg.topic}')
        return
    for device in userdata['devices']:
        try:
            device.handleMsg(msg)
        except Exception:
            log.exception(f'{device.name}: error while handling message on {msg.topic}')


def create_client(config: dict, userdata: dict):
    client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id=CLIENT_ID,
                                clean_session=True, userdata=userdata)
    mqtt = config['mqtt']
    if mqtt['user'] and mqtt['password']:
        client.username_pw_set(mqtt['user'], mqtt['password'])
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    # Last will: if this process dies without saying goodbye, the broker publishes 'offline'
    # for us. Every device entity carries this topic as a second availability source.
    client.will_set(BRIDGE_AVAILABILITY_TOPIC, AVAILABILITY_PAYLOAD_OFFLINE, retain=True)
    return client


def connect(client, host: str, port: int):
    delay = 5
    while True:
        try:
            client.connect(host, port)
            return
        except Exception as exc:
            log.error(f'Could not connect to MQTT broker at {host}:{port}: {exc}. Retrying in {delay}s...')
            time.sleep(delay)
            delay = min(delay * 2, 300)


def wait_for_polling_state(client, userdata: dict, timeout: float = 2.0) -> bool:
    '''The retained control topic decides whether polling starts enabled. Give the broker a
    moment to deliver it before the devices start polling; default is enabled.'''
    deadline = time.monotonic() + timeout
    while userdata['polling'] is None and time.monotonic() < deadline:
        if client.loop(timeout=0.2) != mqtt_client.MQTT_ERR_SUCCESS:
            break
    return True if userdata['polling'] is None else userdata['polling']


def main(argv):
    config_path = Path(argv[0]) if argv else DEFAULT_CONFIG
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        log.error(f'Configuration error: {exc}')
        sys.exit(2)

    log_build_info()
    mqtt = config['mqtt']
    log.info(f'MQTT broker: {mqtt["host"]}:{mqtt["port"]}, user: {mqtt["user"] or "(none)"}')
    log.info(f'Solarflow Hub: {config["hub"]["product_id"]}/{config["hub"]["device_id"]}, '
             f'Ace: {config["ace"]["product_id"]}/{config["ace"]["device_id"]}')

    userdata = {
        'devices': [],
        'polling': None,
        'device_topics': [f'/{c["product_id"]}/{c["device_id"]}/properties/report'
                          for c in (config['hub'], config['ace'])],
    }
    client = create_client(config, userdata)

    def stop(signum, frame):
        log.info(f'Received signal {signum}, shutting down')
        userdata['stopping'] = True
        # A clean disconnect suppresses the last will, so say goodbye ourselves.
        client.publish(BRIDGE_AVAILABILITY_TOPIC, AVAILABILITY_PAYLOAD_OFFLINE, retain=True)
        client.disconnect()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    connect(client, mqtt['host'], mqtt['port'])
    polling = wait_for_polling_state(client, userdata)
    userdata['polling'] = polling

    userdata['devices'] = [
        Solarflow(client=client, telemetry_polling_enabled=polling, **config['hub']),
        Ace(client=client, telemetry_polling_enabled=polling, **config['ace']),
    ]
    publish_transport_polling_state(client, polling)
    log.info('Transport mode active: Solarflow Hub 2000 and Ace 1500 polling, telemetry bridging and '
             f'Home Assistant discovery. Telemetry polling starts {"enabled" if polling else "paused"}.')

    try:
        client.loop_forever(retry_first_connection=True)
    except Exception:
        log.exception('MQTT loop aborted')
    finally:
        # Whatever ended the loop, end the process with it. The timer threads are daemons,
        # but never rely on that alone: a half-dead bridge that still publishes discovery
        # while ignoring every report is worse than a restart.
        stopping = userdata.get('stopping', False)
        if not stopping:
            log.error('MQTT loop ended unexpectedly, exiting')
        logging.shutdown()
        os._exit(0 if stopping else 1)


if __name__ == '__main__':
    main(sys.argv[1:])
