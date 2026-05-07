import random
import logging
import sys
import getopt
import os
import time
import configparser
from pathlib import Path
from paho.mqtt import client as mqtt_client
import solarflow
import ace

FORMAT = '%(asctime)s:%(levelname)s: %(message)s'
logging.basicConfig(stream=sys.stdout, level="INFO", format=FORMAT)
log = logging.getLogger("")


'''
Customizing ConfigParser to allow dynamic conversion of array options
'''
config: configparser.ConfigParser
def listoption(option):
    return [int(x) for x in list(filter(lambda x: x.isdigit(), list(option)))]

def stroption(option):
    return option

def load_config():
    config = configparser.ConfigParser(converters={"str":stroption, "list":listoption})
    script_dir = Path(__file__).resolve().parent
    src_dir = script_dir.parent
    cwd = Path.cwd()
    candidates = [
        src_dir / "config.local.ini",
        src_dir / "config.ini",
        cwd / "config.local.ini",
        cwd / "config.ini",
    ]

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            with candidate.open("r") as config_file:
                config.read_file(config_file)
            log.info(f'Loaded configuration from {candidate}')
            return config

    log.error("No configuration file found (preferred: config.local.ini, fallback: config.ini). Using environment variables.")
    return config

config = load_config()


def noop_callback(*args, **kwargs):
    return False


def log_build_info():
    version = os.environ.get('APP_VERSION', 'dev')
    commit = os.environ.get('GIT_COMMIT', 'unknown')
    branch = os.environ.get('GIT_BRANCH', 'unknown')
    build_date = os.environ.get('BUILD_DATE', 'unknown')

    log.info(f'Solarflow Control version: {version}')
    log.info(f'Solarflow Control commit: {commit} ({branch})')
    log.info(f'Solarflow Control build date: {build_date}')


'''
Configuration Options
'''
sf_device_id = config.get('solarflow', 'device_id', fallback=None) or os.environ.get('SF_DEVICE_ID',None)
sf_product_id = config.get('solarflow', 'product_id', fallback="A8yh63") or os.environ.get('SF_PRODUCT_ID',"A8yh63")
mqtt_user = config.get('mqtt', 'mqtt_user', fallback=None) or os.environ.get('MQTT_USER',None)
mqtt_pwd = config.get('mqtt', 'mqtt_pwd', fallback=None) or os.environ.get('MQTT_PWD',None)
mqtt_host = config.get('mqtt', 'mqtt_host', fallback=None) or os.environ.get('MQTT_HOST',None)
mqtt_port = config.getint('mqtt', 'mqtt_port', fallback=None) or int(os.environ.get('MQTT_PORT', 1883))
    

def on_message(client, userdata, msg):
    '''Delegate incoming MQTT messages to the Solarflow hub and ACE transport adapters only.'''
    hub = userdata["hub"]
    hub.handleMsg(msg)
    ace = userdata["ace"]
    ace.handleMsg(msg)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
    else:
        log.error("Failed to connect, return code %d\n", rc)

def on_disconnect(client, userdata, rc):
    if rc == 0:
        log.info("Disconnected from MQTT Broker on purpose!")
    else:
        log.error("Disconnected from MQTT broker!")

def connect_mqtt() -> mqtt_client:
    client_id = f'solarflow-ctrl-{random.randint(0, 100)}'
    client = mqtt_client.Client(client_id=client_id, clean_session=False)
    if mqtt_user is not None and mqtt_pwd is not None:
        client.username_pw_set(mqtt_user, mqtt_pwd)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    delay = 5
    while True:
        try:
            client.connect(mqtt_host, mqtt_port)
            return client
        except Exception as e:
            log.error(f'Could not connect to MQTT broker at {mqtt_host}:{mqtt_port}: {e}. Retrying in {delay}s...')
            time.sleep(delay)
            delay = min(delay * 2, 300)

def getOpts(configtype) -> dict:
    '''Get the configuration options for a specific section from the global config.ini'''
    global config
    opts = {}
    for opt,opt_type in configtype.opts.items():
        t = opt_type.__name__
        try: 
            if t == "bool": t = "boolean"
            converter = getattr(config,f'get{t}')
            opts.update({opt:opt_type(converter(configtype.__name__.lower(),opt))})
        except configparser.NoOptionError:
            log.info(f'No config setting found for option "{opt}" in section {configtype.__name__.lower()}!')
    return opts

def run():
    log_build_info()

    if sf_product_id != "A8yh63":
        log.error(f'Unsupported Solarflow product_id: {sf_product_id}. This transport layer only supports Hub 2000 (A8yh63).')
        sys.exit(1)

    hub_opts = getOpts(solarflow.Solarflow)
    ace_opts = getOpts(ace.Ace)

    client = connect_mqtt()
    hub = solarflow.Solarflow(client=client, callback=noop_callback, **hub_opts)
    aceUnit = ace.Ace(client=client, callback=noop_callback, **ace_opts)

    client.user_data_set({"hub": hub, "ace": aceUnit})

    client.on_message = on_message
    log.info('Transport mode active: only Solarflow Hub 2000 and ACE 1500 telemetry plus direct device writes are enabled.')

    hub.subscribe()
    aceUnit.subscribe()

    client.loop_forever()

def main(argv):
    global mqtt_host, mqtt_port, mqtt_user, mqtt_pwd
    global sf_device_id
    opts, args = getopt.getopt(argv,"hb:p:u:s:d:",["broker=","port=","user=","password="])
    for opt, arg in opts:
        if opt == '-h':
            log.info('solarflow-control.py -b <MQTT Broker Host> -p <MQTT Broker Port>')
            sys.exit()
        elif opt in ("-b", "--broker"):
            mqtt_host = arg
        elif opt in ("-p", "--port"):
            mqtt_port = arg
        elif opt in ("-u", "--user"):
            mqtt_user = arg
        elif opt in ("-s", "--password"):
            mqtt_pwd = arg
        elif opt in ("-d", "--device"):
            sf_device_id = arg

    if mqtt_host is None:
        log.error("You need to provide a local MQTT broker (environment variable MQTT_HOST or option --broker)!")
        sys.exit(0)
    else:
        log.info(f'MQTT Host: {mqtt_host}:{mqtt_port}')

    if mqtt_user is None or mqtt_pwd is None:
        log.info(f'MQTT User is not set, assuming authentication not needed')
    else:
        log.info(f'MQTT User: {mqtt_user}/{mqtt_pwd}')

    if sf_device_id is None:
        log.error(f'You need to provide a SF_DEVICE_ID (environment variable SF_DEVICE_ID or option --device)!')
        sys.exit()
    else:
        log.info(f'Solarflow Hub: {sf_product_id}/{sf_device_id}')

    run()

if __name__ == '__main__':
    main(sys.argv[1:])
