import json
import random, time, logging, sys, getopt, os
import shutil
import subprocess
from datetime import datetime, timedelta
from functools import reduce
from paho.mqtt import client as mqtt_client
from astral import LocationInfo
from astral.sun import sun
import requests
import configparser
import math
import solarflow
import ace
import dtus
import smartmeters
from utils import RepeatedTimer, str2bool

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
    try:
        with open("config.ini","r") as cf:
            config.read_file(cf)
    except:
        log.error("No configuration file (config.ini) found in execution directory! Using environment variables.")
    return config

config = load_config()


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
sf_product_id = config.get('solarflow', 'product_id', fallback="73bkTV") or os.environ.get('SF_PRODUCT_ID',"73bkTV")
mqtt_user = config.get('mqtt', 'mqtt_user', fallback=None) or os.environ.get('MQTT_USER',None)
mqtt_pwd = config.get('mqtt', 'mqtt_pwd', fallback=None) or os.environ.get('MQTT_PWD',None)
mqtt_host = config.get('mqtt', 'mqtt_host', fallback=None) or os.environ.get('MQTT_HOST',None)
mqtt_port = config.getint('mqtt', 'mqtt_port', fallback=None) or int(os.environ.get('MQTT_PORT', 1883))


DTU_TYPE = config.get('global', 'dtu_type', fallback=None) or os.environ.get('DTU_TYPE',"OpenDTU")
SMT_TYPE = config.get('global', 'smartmeter_type', fallback=None) or os.environ.get('SMARTMETER_TYPE',"Smartmeter")

# The amount of power that should be always reserved for charging, if available. Nothing will be fed to the house if less is produced
# MQTT config topic: solarflow-hub/control/minChargePower
# config.ini [control] min_charge_power
MIN_CHARGE_POWER = None

# The maximum discharge level of the packSoc. Even if there is more demand it will not go beyond that
# MQTT config topic: solarflow-hub/control/maxDischargePower
# config.ini [control] max_discharge_power
MAX_DISCHARGE_POWER = None

# battery SoC levels for normal operation cycles (when not in charge through mode)
# MQTT config topic: solarflow-hub/control/batteryTargetSoCMin
# config.ini [control] battery_low
BATTERY_LOW = None
# MQTT config topic: solarflow-hub/control/batteryTargetSoCMax
# config.ini [control] battery_high
BATTERY_HIGH = None

# the SoC that is required before discharging of the battery would start. To allow a bit of charging first in the morning.
BATTERY_DISCHARGE_START = config.getint('control', 'battery_discharge_start', fallback=None) \
                        or int(os.environ.get('BATTERY_DISCHARGE_START',10)) 

# enable grid charging via ACE when battery is below BATTERY_LOW and no solar is available
# config.ini [control] grid_charge_enabled
# set to false (default) to disable
GRID_CHARGE_ENABLED =   None

# interval for ping-based wake detection once device polling has been paused
DEVICE_PING_INTERVAL =  config.getint('control', 'device_ping_interval', fallback=None) \
                        or int(os.environ.get('DEVICE_PING_INTERVAL',30))

# the maximum allowed inverter output
MAX_INVERTER_LIMIT =    config.getint('control', 'max_inverter_limit', fallback=None) \
                        or int(os.environ.get('MAX_INVERTER_LIMIT',800))
MAX_INVERTER_INPUT =    config.getint('control', 'max_inverter_input', fallback=None) \
                        or int(os.environ.get('MAX_INVERTER_INPUT',400))

# this controls the internal calculation of limited growth for setting inverter limits
INVERTER_START_LIMIT = 5

# interval/rate limit for performing control steps
steering_interval =     config.getint('control', 'steering_interval', fallback=None) \
                        or int(os.environ.get('STEERING_INTERVAL',15))

# flag, which can be set to allow discharging the battery during daytime
# MQTT config topic: solarflow-hub/control/dischargeDuringDaytime
# config.ini [control] discharge_during_daytime
DISCHARGE_DURING_DAYTIME = None

#Adjustments possible to sunrise and sunset offset
# MQTT config topic: solarflow-hub/control/sunriseOffset
# config.ini [control] sunrise_offset
SUNRISE_OFFSET = None
# MQTT config topic: solarflow-hub/control/sunsetOffset
# config.ini [control] sunset_offset
SUNSET_OFFSET = None

# Location Info
LAT = config.getfloat('global', 'latitude', fallback=None) or float(os.environ.get('LATITUDE',0))
LNG = config.getfloat('global', 'longitude', fallback=None) or float(os.environ.get('LONGITUDE',0))
location: LocationInfo

lastTriggerTS:datetime = None
shutdownPendingSince:datetime = None
pingCommandLoggedMissing = False
pingBinary = shutil.which('ping')

class MyLocation:
    def getCoordinates(self) -> tuple:
        lat = lon = 0.0
        try:
            result = requests.get('http://ip-api.com/json/') # call without IP uses my IP
            response = result.json()
            log.info(f'IP Address: {response["query"]}')
            log.info(f'Location: {response["city"]}, {response["regionName"]}, {response["country"]}')
            log.info(f'Coordinates: (Lat: {response["lat"]}, Lng: {response["lon"]}')
            lat = response["lat"]
            lon = response["lon"]
        except Exception as e:
            log.error(f'Can\'t determine location from my IP. Location detection failed, no accurate sunrise/sunset detection possible',e.args)

        return (lat,lon)

def on_config_message(client, userdata, msg):
    '''The MQTT client callback function for intial connects - mainly retained messages, where we are not yet fully up and running but still read potential config parameters from MQTT'''

    global SUNRISE_OFFSET, SUNSET_OFFSET, MIN_CHARGE_POWER, MAX_DISCHARGE_POWER, DISCHARGE_DURING_DAYTIME, BATTERY_LOW, BATTERY_HIGH, GRID_CHARGE_ENABLED
    # handle own messages (control parameters)
    if msg.topic.startswith('solarflow-hub') and "control" in msg.topic and msg.payload:
        parameter = msg.topic.split('/')[-1]
        value = msg.payload.decode()
        match parameter:
            case "sunriseOffset":
                SUNRISE_OFFSET = int(value)
                log.info(f'Found control/sunriseOffset, set SUNRISE_OFFSET to {SUNRISE_OFFSET} minutes')
            case "sunsetOffset":
                SUNSET_OFFSET = int(value)
                log.info(f'Found control/sunsetOffset, set SUNSET_OFFSET to {SUNSET_OFFSET} minutes')
            case "minChargePower":
                MIN_CHARGE_POWER = int(value)
                log.info(f'Found control/minChargePower, set MIN_CHARGE_POWER to {MIN_CHARGE_POWER}W')
            case "maxDischargePower":
                MAX_DISCHARGE_POWER = int(value)
                log.info(f'Found control/maxDiscahrgePiwer, set MAX_DISCHARGE_POWER to {MAX_DISCHARGE_POWER}W')
            case "dischargeDuringDaytime":
                DISCHARGE_DURING_DAYTIME = str2bool(value)
                log.info(f'Found control/dischargeDuringDaytime, set DISCHARGE_DURING_DAYTIME to {DISCHARGE_DURING_DAYTIME}')
            case "batteryTargetSoCMin":
                BATTERY_LOW = int(value)
                log.info(f'Found control/batteryTargetSoCMin, set BATTERY_LOW to {BATTERY_LOW}%')
            case "batteryTargetSoCMax":
                BATTERY_HIGH = int(value)
                log.info(f'Found control/batteryTargetSoCMax, set BATTERY_HIGH to {BATTERY_HIGH}%')
            case "gridChargeEnabled":
                GRID_CHARGE_ENABLED = str2bool(value)
                log.info(f'Found control/gridChargeEnabled, set GRID_CHARGE_ENABLED to {GRID_CHARGE_ENABLED}')
    

def on_message(client, userdata, msg):
    '''The MQTT client callback function for continous oepration, messages are delegated to hub, dtu and smartmeter handlers as well as own control parameter updates'''
    global SUNRISE_OFFSET, SUNSET_OFFSET, MIN_CHARGE_POWER, MAX_DISCHARGE_POWER, DISCHARGE_DURING_DAYTIME, BATTERY_LOW, BATTERY_HIGH, GRID_CHARGE_ENABLED
    #delegate message handling to hub,smartmeter, dtu
    smartmeter = userdata["smartmeter"]
    smartmeter.handleMsg(msg)
    hub = userdata["hub"]
    hub.handleMsg(msg)
    dtu = userdata["dtu"]
    dtu.handleMsg(msg)
    ace = userdata["ace"]
    ace.handleMsg(msg)

    # handle own messages (control parameters)
    if msg.topic.startswith('solarflow-hub') and "control" in msg.topic and msg.payload:
        parameter = msg.topic.split('/')[-1]
        value = msg.payload.decode()
        match parameter:
            case "sunriseOffset":
                log.info(f'Updating SUNRISE_OFFSET to {int(value)} minutes') if SUNRISE_OFFSET != int(value) else None
                SUNRISE_OFFSET = int(value)
            case "sunsetOffset":
                log.info(f'Updating SUNSET_OFFSET to {int(value)} minutes') if SUNSET_OFFSET != int(value) else None
                SUNSET_OFFSET = int(value)
            case "minChargePower":
                log.info(f'Updating MIN_CHARGE_POWER to {int(value)}W') if MIN_CHARGE_POWER != int(value) else None
                MIN_CHARGE_POWER = int(value)
            case "maxDischargePower":
                log.info(f'Updating MAX_DISCHARGE_POWER to {int(value)}W') if MAX_DISCHARGE_POWER != int(value) else None
                MAX_DISCHARGE_POWER = int(value) 
            case "controlBypass":
                log.info(f'Updating control bypass to {value}')
                hub.setControlBypass(value)
            case "fullChargeInterval":
                log.info(f'Updating full charge interval to {int(value)}hrs')
                hub.updFullChargeInterval(int(value))
            case "dischargeDuringDaytime":
                log.info(f'Updating DISCHARGE_DURING_DAYTIME to {str2bool(value)}') if DISCHARGE_DURING_DAYTIME != str2bool(value) else None
                DISCHARGE_DURING_DAYTIME = str2bool(value)
            case "batteryTargetSoCMin":
                log.info(f'Updating BATTERY_LOW to {int(value)}%') if BATTERY_LOW != int(value) else None
                BATTERY_LOW = int(value)
                hub.updBatteryTargetSoCMin(BATTERY_LOW)
            case "batteryTargetSoCMax":
                log.info(f'Updating BATTERY_HIGH to {int(value)}%') if BATTERY_HIGH != int(value) else None
                BATTERY_HIGH = int(value)
                hub.updBatteryTargetSoCMax(BATTERY_HIGH)
            case "gridChargeEnabled":
                log.info(f'Updating GRID_CHARGE_ENABLED to {str2bool(value)}') if GRID_CHARGE_ENABLED != str2bool(value) else None
                GRID_CHARGE_ENABLED = str2bool(value)


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
    client.on_message = on_config_message

    delay = 5
    while True:
        try:
            client.connect(mqtt_host, mqtt_port)
            return client
        except Exception as e:
            log.error(f'Could not connect to MQTT broker at {mqtt_host}:{mqtt_port}: {e}. Retrying in {delay}s...')
            time.sleep(delay)
            delay = min(delay * 2, 300)

def subscribe(client: mqtt_client):
    topics = [
            f'solarflow-hub/{sf_device_id}/control/#'
    ]
    for t in topics:
        client.subscribe(t)
        log.info(f'SF Control subscribing: {t}')

def limitedRise(x) -> int:
    rise = MAX_INVERTER_LIMIT-(MAX_INVERTER_LIMIT-INVERTER_START_LIMIT)*math.exp(-MAX_INVERTER_LIMIT/100000*x)
    log.info(f'Adjusting inverter limit from {x:.1f}W to {rise:.1f}W')
    return int(rise)


# calculate the safe inverter limit for direct panels, to avoid output over legal limits
def getDirectPanelLimit(inv, hub, smt) -> int:
    # if hub is in bypass mode we can treat it just like a direct panel
    direct_panel_power = inv.getDirectACPower() + (inv.getHubACPower() if hub.getBypass() else 0)
    if direct_panel_power < MAX_INVERTER_LIMIT:
        dc_values = (inv.getDirectDCPowerValues() + inv.getHubDCPowerValues()) if hub.getBypass() else inv.getDirectDCPowerValues()
        return math.ceil(max(dc_values) * (inv.getEfficiency()/100)) if smt.getPower() - smt.zero_offset < 0 else limitedRise(max(dc_values) * (inv.getEfficiency()/100))
    else:
        return int(MAX_INVERTER_LIMIT*(inv.getNrHubChannels()/inv.getNrProducingChannels()))


def getBypassExportLimit(inv) -> float:
    return inv.acLimit / inv.getNrTotalChannels()


def isBypassExportActive(hub) -> bool:
    return hub.getBypass()


def setTelemetryPolling(hub, ace_unit, state: bool):
    hub.setTelemetryPolling(state)
    if ace_unit:
        ace_unit.setTelemetryPolling(state)


def pingDevice(ip_address: str):
    global pingCommandLoggedMissing

    if not ip_address:
        return None

    if pingBinary is None:
        if not pingCommandLoggedMissing:
            log.warning('ping command not found — device wake detection via ICMP is disabled.')
            pingCommandLoggedMissing = True
        return None

    result = subprocess.run(
        [pingBinary, '-c', '1', '-W', '1', ip_address],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def checkDeviceWakeState(device, label: str) -> bool:
    if device.masterSwitch:
        return False

    reachable = pingDevice(device.deviceIP)
    if reachable is None:
        return False

    if device.pingReachable != reachable:
        log.info(f'{label} ping {device.deviceIP} is {"online" if reachable else "offline"}.')
        device.pingReachable = reachable

    if not reachable:
        if not device.pingWakeArmed:
            device.pingWakeArmed = True
            log.info(f'{label} is offline after master switch off — wake detection armed.')
        return False

    if device.pingWakeArmed:
        device.pingWakeArmed = False
        device.masterSwitch = True
        device.setTelemetryPolling(True)
        log.info(f'{label} is reachable again after being offline — resuming MQTT telemetry polling.')
        device.update()
        return True

    return False


def checkDevicePresence(client: mqtt_client):
    global shutdownPendingSince

    hub = client._userdata.get('hub')
    ace_unit = client._userdata.get('ace')
    if hub is None:
        return

    hub_woke = checkDeviceWakeState(hub, 'Hub') if hub.deviceIP else False
    ace_woke = checkDeviceWakeState(ace_unit, 'Ace') if ace_unit and ace_unit.deviceIP else False

    if hub_woke:
        shutdownPendingSince = None
        hub.setShouldStandby(False)
        if ace_unit and ace_unit.masterSwitch:
            ace_unit.setTelemetryPolling(True)

    if hub_woke or ace_woke:
        limit_callback(client, force=True)


def getLiveSolarState(hub, ace_unit):
    now = datetime.now()

    hub_stale = hub.lastSolarInputTS is None or (now - hub.lastSolarInputTS).total_seconds() > 120
    hub_solar = 0 if hub_stale else hub.getSolarInputPower()

    if ace_unit:
        ace_last = ace_unit.lastSolarInputTS or ace_unit.startTS
        ace_stale = (now - ace_last).total_seconds() > 120
        ace_solar = 0 if ace_stale else ace_unit.getSolarInputPower()
    else:
        ace_stale = False
        ace_solar = 0

    return hub_solar, ace_solar, hub_stale, ace_stale


def shouldPauseHubControlForStandby(hub, ace_unit) -> bool:
    if not hub.shouldStandby:
        return False

    battery_soc = hub.getElectricLevel()
    if battery_soc < 0 or hub.batteryLow < 0:
        return False

    hub_solar, ace_solar, _, _ = getLiveSolarState(hub, ace_unit)
    return battery_soc <= hub.batteryLow and hub_solar == 0 and ace_solar == 0 and not (ace_unit and ace_unit.acSwitch)

def getSFPowerLimit(hub, demand) -> int:
    hub_electricLevel = hub.getElectricLevel()
    hub_solarpower = hub.getSolarInputPower()
    now = datetime.now(tz=location.tzinfo)   
    s = sun(location.observer, date=now, tzinfo=location.timezone)
    sunrise = s['sunrise']
    sunset = s['sunset']
    path = ""

    sunrise_off = timedelta(minutes = SUNRISE_OFFSET)
    sunset_off = timedelta(minutes = SUNSET_OFFSET)

    # fallback in case byPass is not yet identifieable after a change (HUB2k)
    limit = hub.getLimit()

    # if the hub is currently in bypass mode we don't really worry about any limit
    if hub.getBypass():
        path += "0."
        # leave bypass after sunset/offset
        if (now < (sunrise + sunrise_off) or now > sunset - sunset_off) and hub.control_bypass and demand > hub_solarpower:
            hub.allowBypass(False)
            hub.setBypass(False)
            path += "1."
        else:
            path += "2."
            limit = hub.getInverseMaxPower()

    if not hub.getBypass():
        surplus = hub_solarpower - demand
        in_night_window = (now < (sunrise + sunrise_off) or now > sunset - sunset_off)
        in_sunrise_window = sunrise < now < (sunrise + sunrise_off)

        log.info(f'getSFPowerLimit: demand={demand:.1f}W, hub_solar={hub_solarpower:.1f}W, surplus={surplus:.1f}W, '
                 f'battery={hub_electricLevel}%, discharge_start={BATTERY_DISCHARGE_START}%, '
                 f'in_night_window={in_night_window}, in_sunrise_window={in_sunrise_window}, '
                 f'discharge_daytime={DISCHARGE_DURING_DAYTIME}, batteryTarget={hub.batteryTarget}')

        if surplus > MIN_CHARGE_POWER:
            path += "1."
            # Überschuss vorhanden: Solar kann Demand decken + laden
            limit = demand
            log.info(f'Solar surplus ({surplus:.1f}W) > MIN_CHARGE_POWER ({MIN_CHARGE_POWER}W) → covering demand, battery charges with excess: limit={limit:.1f}W')
        if surplus <= MIN_CHARGE_POWER:
            path += "2."
            if (in_night_window or DISCHARGE_DURING_DAYTIME):
                path += "1."
                if in_sunrise_window and hub_electricLevel <= BATTERY_DISCHARGE_START and hub.batteryTarget != solarflow.BATTERY_TARGET_DISCHARGING:
                    path += "1."
                    limit = 0
                    log.info(f'Sunrise window: battery={hub_electricLevel}% <= discharge_start={BATTERY_DISCHARGE_START}% and not yet discharging → holding limit=0 to allow charging first')
                else:
                    path += "2."
                    # No solar surplus but discharging allowed: try to cover full demand with battery help
                    limit = demand
                    log.info(f'No solar surplus (discharge allowed): discharging to cover demand={demand:.1f}W → limit={limit:.1f}W')
            else:
                path += "2."
                limit = max(0, hub_solarpower - MIN_CHARGE_POWER)
                log.info(f'Daytime, discharge not allowed → solar pass-through only with MIN_CHARGE reservation: limit={limit:.1f}W')
        if demand < 0:
            log.info(f'Demand is negative ({demand:.1f}W), overproducing → setting limit=0')
            limit = 0

    # get battery Soc at sunset/sunrise
    td = timedelta(minutes = 3)
    if now > sunset and now < sunset + td:
        hub.setSunsetSoC(hub_electricLevel)
    if now > sunrise and now < sunrise + td:
        hub.setSunriseSoC(hub_electricLevel)
        log.info(f'Good morning! We have consumed {hub.getNightConsumption()}% of the battery tonight!')
        ts = int(time.time())
        log.info(f'Syncing time of solarflow hub (UTC): {datetime.fromtimestamp(ts).strftime("%Y-%m-%d, %H:%M:%S")}')
        hub.timesync(ts)

        # sometimes bypass resets to default (auto)
        if hub.control_bypass:
            hub.allowBypass(True)
            hub.setBypass(False)
            hub.setAutorecover(False)
            
        # calculate expected daylight in hours
        diff = sunset - sunrise
        daylight = diff.total_seconds()/3600

        # check if we should run a full charge cycle today
        hub.checkChargeThrough(daylight)

    log.info(f'Based on time, solarpower ({hub_solarpower:4.1f}W) minimum charge power ({MIN_CHARGE_POWER}W) and bypass state ({hub.getBypass()}), hub could contribute {limit:4.1f}W - Decision path: {path}')
    return int(limit)


def limitHomeInput(client: mqtt_client):
    global location

    hub = client._userdata['hub']
    log.info(f'{hub}')
    ace_unit = client._userdata.get('ace')
    inv = client._userdata['dtu']
    log.info(f'{inv}')
    smt = client._userdata['smartmeter']
    log.info(f'{smt}')

    if shouldPauseHubControlForStandby(hub, ace_unit):
        hub_solar, ace_solar, _, _ = getLiveSolarState(hub, ace_unit)
        log.info(f'Hub standby pending and idle conditions still hold (battery={hub.getElectricLevel()}%, hub_low={hub.batteryLow}%, hub_solar={hub_solar}W, ace_solar={ace_solar}W) — forcing hub limit to 0 and skipping demand-based regulation.')
        hub.setOutputLimit(0)
        return

    # ensure we have data to work on
    if not(hub.ready() and inv.ready() and smt.ready()):
        return

    inv_limit = inv.getLimit()
    hub_limit = hub.getLimit()
    direct_limit = None
    bypass_export_active = isBypassExportActive(hub)
    efficiency = inv.getEfficiency()/100

    # convert DC Power into AC power by applying current efficiency for more precise calculations
    direct_panel_power = inv.getDirectDCPower() * efficiency
    # consider DC power of panels below 10W as 0 to avoid fluctuation in very low light.
    direct_panel_power = 0 if direct_panel_power < 10 else direct_panel_power

    hub_power = inv.getHubDCPower() * efficiency

    grid_power = smt.getPower() - smt.zero_offset
    inv_acpower = inv.getCurrentACPower()

    demand = grid_power + direct_panel_power + hub_power

    remainder = demand - direct_panel_power - hub_power        # eq grid_power
    hub_contribution_ask = hub_power+remainder     # the power we need from hub
    hub_contribution_ask = 0 if hub_contribution_ask < 0 else hub_contribution_ask

    log.info(f'Power breakdown: grid={grid_power:.1f}W, panels_ac={direct_panel_power:.1f}W, hub_ac={hub_power:.1f}W → demand={demand:.1f}W, hub_ask={hub_contribution_ask:.1f}W')


    # sunny, producing
    if direct_panel_power > 0:
        if demand < direct_panel_power:
            # Direct panels can cover the load. Usually we pull the inverter back to the
            # actual demand to avoid feed-in, except when the hub is already in bypass:
            # then the battery is full and excess solar should be exported instead of curtailed.
            log.info(f'Direct connected panels ({direct_panel_power:.1f}W) can cover demand ({demand:.1f}W)')
            if bypass_export_active:
                direct_limit = getBypassExportLimit(inv)
                hub_limit = hub.getInverseMaxPower()
                log.info(f'Bypass is active (battery full), opening inverter for export up to AC limit: direct_limit={direct_limit:.1f}W/channel, hub_limit={hub_limit:.1f}W')
            elif hub.getElectricLevel() >= hub.batteryHigh and not hub.chargeThrough and hub.getSolarInputPower() > 0:
                remaining_ac_headroom = max(0, inv.acLimit - direct_panel_power)
                remaining_dc_headroom = remaining_ac_headroom / efficiency if efficiency > 0 else 0
                hub_limit = hub.setOutputLimit(min(hub.getInverseMaxPower(), remaining_dc_headroom))
                direct_limit = getDirectPanelLimit(inv, hub, smt)
                log.info(f'Battery is full ({hub.getElectricLevel()}% >= {hub.batteryHigh}%), allowing hub solar export up to AC headroom: ac_headroom={remaining_ac_headroom:.1f}W, dc_headroom={remaining_dc_headroom:.1f}W, hub_limit={hub_limit:.1f}W')
            else:
                direct_producing_channels = max(1, len(list(filter(lambda value: value > 0, inv.getDirectDCPowerValues()))))
                direct_limit = demand / direct_producing_channels
                log.info(f'Reducing direct panel limit to demand: {demand:.1f}W across {direct_producing_channels} direct channels → {direct_limit:.1f}W/channel')
                hub_limit = hub.setOutputLimit(0)
        else:
            # we need contribution from hub, if possible and/or try to get more from direct panels
            log.info(f'Direct connected panels ({direct_panel_power:.1f}W) can\'t cover demand ({demand:.1f}W), trying to get {hub_contribution_ask:.1f}W from hub.')
            if hub_contribution_ask > 5:
                # is there potentially more to get from direct panels?
                # if the direct channel power is below what is theoretically possible, it is worth trying to increase the limit
                direct_channel_max = max(inv.getDirectDCPowerValues()) * efficiency

                # if the max of direct channel power is close to the channel limit we should increase the limit first to eventually get more from direct panels 
                if inv.isWithin(direct_channel_max,inv.getChannelLimit(),10*inv.getNrTotalChannels()):
                    log.info(f'The current max direct channel power {direct_channel_max:.1f}W is close to the current channel limit {inv.getChannelLimit():.1f}W, trying to get more from direct panels.')

                    sf_contribution = getSFPowerLimit(hub,hub_contribution_ask)
                    if bypass_export_active:
                        hub_limit = hub.getInverseMaxPower()
                        direct_limit = getBypassExportLimit(inv)
                        log.info(f'Bypass is active, skipping demand-based panel tracking and opening inverter for export: direct_limit={direct_limit:.1f}W/channel, hub_limit={hub_limit:.1f}W')
                    else:
                        hub_limit = hub.getLimit()
                        # in case of hub contribution ask has changed to lower than current value, we should lower it
                        if sf_contribution < hub_limit:
                            hub.setOutputLimit(sf_contribution)
                        direct_limit = getDirectPanelLimit(inv,hub,smt)
                else:
                    # check what hub is currently  willing to contribute
                    sf_contribution = getSFPowerLimit(hub,hub_contribution_ask)

                    # would the hub's contribution plus direct panel power cross the AC limit? If yes only contribute up to the limit
                    if sf_contribution * efficiency + direct_panel_power  > inv.acLimit:
                        log.info(f'Hub could contribute {sf_contribution:.1f}W, but this would exceed the configured AC limit ({inv.acLimit}W), so only asking for {inv.acLimit - direct_panel_power:.1f}W')
                        sf_contribution = inv.acLimit - direct_panel_power

                    # if the hub's contribution (per channel) is larger than what the direct panels max is delivering (night, low light)
                    # then we can open the hub to max limit and use the inverter to limit it's output (more precise)
                    if sf_contribution/inv.getNrHubChannels() >= direct_channel_max:
                        log.info(f'Hub should contribute more ({sf_contribution:.1f}W) than what we currently get max from panels ({direct_channel_max:.1f}W), we will use the inverter for fast/precise limiting!')
                        hub_limit = hub.getInverseMaxPower() if bypass_export_active else hub.setOutputLimit(hub.getInverseMaxPower())
                        direct_limit = sf_contribution/inv.getNrHubChannels()
                    else:
                        hub_limit = hub.getInverseMaxPower() if bypass_export_active else hub.setOutputLimit(sf_contribution)
                        log.info(f'Hub is willing to contribute {min(hub_limit,hub_contribution_ask):.1f}W of the requested {hub_contribution_ask:.1f}!')
                        direct_limit = getDirectPanelLimit(inv,hub,smt)
                        log.info(f'Direct connected panel limit is {direct_limit}W.')

    # likely no sun, not producing, eveything comes from hub
    else:
        log.info(f'Direct connected panel are producing {direct_panel_power:.1f}W, trying to get {hub_contribution_ask:.1f}W from hub.')
        # check what hub is currently  willing to contribute
        sf_contribution = getSFPowerLimit(hub,hub_contribution_ask)
        hub_limit = hub.setOutputLimit(hub.getInverseMaxPower())
        direct_limit = sf_contribution/inv.getNrHubChannels()
        log.info(f'Solarflow is willing to contribute {min(hub_limit,direct_limit):.1f}W (per channel) of the requested {hub_contribution_ask:.1f}!')


    if direct_limit != None:

        limit = direct_limit

        if hub_limit > direct_limit > hub_limit - 10:
            limit = hub_limit - 10
        if direct_limit < hub_limit - 10 and hub_limit < hub.getInverseMaxPower():
            limit = hub_limit - 10

        inv_limit = inv.setLimit(limit)

    if remainder < 0:
        source = f'unknown: {-remainder:.1f}'
        if direct_panel_power == 0 and hub_power > 0 and hub.getDischargePower() > 0:
            source = f'battery: {-grid_power:.1f}W'
        # since we usually set the inverter limit not to zero there is always a little bit drawn from the hub (10-15W)
        if direct_panel_power == 0 and hub_power > 15 and hub.getDischargePower() == 0 and not hub.getBypass():
            source = f'hub solarpower: {-grid_power:.1f}W'
        if direct_panel_power > 0 and hub_power > 15  and hub.getDischargePower() == 0 and hub.getBypass():
            source = f'hub bypass: {-grid_power:.1f}W'
        if direct_panel_power > 0 and hub_power < 15:
            source = f'panels connected directly to inverter: {-remainder:.1f}'

        log.info(f'Grid feed in from {source}!')

    panels_dc = "|".join([f'{v:>2}' for v in inv.getDirectDCPowerValues()])
    hub_dc = "|".join([f'{v:>2}' for v in inv.getHubDCPowerValues()])

    now = datetime.now(tz=location.tzinfo)   
    s = sun(location.observer, date=now, tzinfo=location.timezone)
    sunrise = s['sunrise']
    sunset = s['sunset']

    effective_hub_limit = hub.getInverseMaxPower() if bypass_export_active else hub_limit

    log.info(' '.join(f'Sun: {sunrise.strftime("%H:%M")} - {sunset.strftime("%H:%M")} \
             Demand: {demand:.1f}W, \
             Panel DC: ({direct_panel_power:.1f}W), \
             Hub DC: ({hub_power:.1f}W), \
             Inverter Limit: {inv_limit:.1f}W, \
             Hub Limit: {effective_hub_limit:.1f}W'.split()))

    _checkIdleShutdown(client, hub)
    _checkGridCharge(client, hub)

def _checkIdleShutdown(client: mqtt_client, hub):
    """Shut down hub and ace when there is no solar input for at least 15 minutes.
    Devices wake themselves up again once solar returns."""
    global shutdownPendingSince

    # hub.batteryLow is set from hub telemetry; -1 means not yet received
    if hub.batteryLow < 0:
        return

    # hub is already shut down — nothing to do until it wakes up on its own
    if not hub.masterSwitch:
        return

    ace_unit = client._userdata.get('ace')
    hub_solar, ace_solar, hub_stale, ace_stale = getLiveSolarState(hub, ace_unit)

    battery_soc = hub.getElectricLevel()

    # do not shut down while grid charging is active
    if ace_unit and ace_unit.acSwitch:
        if shutdownPendingSince is not None:
            log.info('Grid charging active — resetting idle shutdown timer.')
            shutdownPendingSince = None
        hub.setShouldStandby(False)
        setTelemetryPolling(hub, ace_unit, True)
        return

    # idle = battery fully discharged to minimum AND no solar on any source
    # use hub.batteryLow (the hub's actual configured minimum) rather than the global
    # BATTERY_LOW which may be stale/out-of-sync with the hub's own retained value
    battery_at_min = battery_soc > -1 and battery_soc <= hub.batteryLow
    idle = battery_at_min and hub_solar == 0 and ace_solar == 0
    log.info(f'Idle check: battery={battery_soc}% (hub_low={hub.batteryLow}%, at_min={battery_at_min}), hub_solar={hub_solar}W (stale={hub_stale}), ace_solar={ace_solar}W (stale={ace_stale}) → idle={idle}')

    if idle:
        if shutdownPendingSince is None:
            shutdownPendingSince = datetime.now()
            hub.setShouldStandby(True)
            log.info(f'Idle shutdown timer started: battery at {battery_soc}% (≤ {hub.batteryLow}%), no solar on hub or ace.')
        elif (datetime.now() - shutdownPendingSince).total_seconds() >= 900:
            log.info('Battery at minimum with no solar for 15 minutes — shutting down hub and ace.')
            hub.setMasterSwitch(False)
            if ace_unit:
                ace_unit.setMasterSwitch(False)
            shutdownPendingSince = None
    else:
        if shutdownPendingSince is not None:
            log.info(f'Idle conditions no longer met (battery: {battery_soc}%, hub solar: {hub_solar}W, ace solar: {ace_solar}W) — resetting shutdown timer.')
        hub.setShouldStandby(False)
        setTelemetryPolling(hub, ace_unit, True)
        shutdownPendingSince = None


def _checkGridCharge(client: mqtt_client, hub):
    """Use the ACE 1500 AC input to charge the battery from the grid when:
    - grid_charge_enabled is true
    - Battery is at or below BATTERY_LOW
    - No solar input on hub or ace (avoid unnecessary grid draw while sun is up)
    Stop charging once battery reaches BATTERY_LOW (the configured discharge minimum)."""
    if not GRID_CHARGE_ENABLED:
        return

    ace_unit = client._userdata.get('ace')
    if ace_unit is None:
        return

    battery_low = hub.batteryLow if hub.batteryLow >= 0 else BATTERY_LOW
    hub_solar, ace_solar, hub_stale, ace_stale = getLiveSolarState(hub, ace_unit)
    battery_soc = hub.getElectricLevel()
    no_solar = hub_solar == 0 and ace_solar == 0

    log.info(f'Grid charge check: battery={battery_soc}% (low={battery_low}%), hub_solar={hub_solar:.1f}W (stale={hub_stale}), ace_solar={ace_solar:.1f}W (stale={ace_stale}), no_solar={no_solar}, acSwitch={ace_unit.acSwitch}')

    if battery_low is None:
        return

    if battery_soc <= battery_low and no_solar and not ace_unit.acSwitch:
        log.info(f'Battery at {battery_soc}% (min {battery_low}%), no solar — enabling grid charging via ACE until {battery_low}%.')
        ace_unit.setAcSwitch(True)
    elif ace_unit.acSwitch and (battery_soc >= battery_low or not no_solar):
        reason = f'target SoC {battery_low}% reached' if battery_soc >= battery_low else f'solar available (hub: {hub_solar}W, ace: {ace_solar}W)'
        log.info(f'Stopping grid charging: {reason}.')
        ace_unit.setAcSwitch(False)
    else:
        log.info(f'Grid charging conditions not met (acSwitch={ace_unit.acSwitch}, battery={battery_soc}% vs low={battery_low}%, no_solar={no_solar}) — no change.')


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

def limit_callback(client: mqtt_client,force=False):
    global lastTriggerTS
    #log.info("Smartmeter Callback!")
    now = datetime.now()
    if lastTriggerTS:
        elapsed = now - lastTriggerTS
        # ensure the limit function is not called too often (avoid flooding DTUs)
        if elapsed.total_seconds() >= steering_interval or force:
            lastTriggerTS = now
            limitHomeInput(client)
            return True
        else:
            return False
    else:
        lastTriggerTS = now
        limitHomeInput(client)
        return True

def deviceInfo(client:mqtt_client):
    limitHomeInput(client)

def updateConfigParams(client):
    global config, DISCHARGE_DURING_DAYTIME, SUNRISE_OFFSET, SUNSET_OFFSET, MIN_CHARGE_POWER, MAX_DISCHARGE_POWER, BATTERY_HIGH, BATTERY_LOW, GRID_CHARGE_ENABLED

    # only update if configparameters haven't been updated/read from MQTT
    def cfg(section, key, fallback, env_key, env_default, getter='getint'):
        val = getattr(config, getter)(section, key, fallback=fallback)
        if val is None:
            val = env_default if os.environ.get(env_key) is None else type(env_default)(os.environ.get(env_key))
        return val

    if DISCHARGE_DURING_DAYTIME == None:
        DISCHARGE_DURING_DAYTIME = cfg('control', 'discharge_during_daytime', None, 'DISCHARGE_DURING_DAYTIME', False, 'getboolean')
        log.info(f'Updating DISCHARGE_DURING_DAYTIME from config file to {DISCHARGE_DURING_DAYTIME}')
        client.publish(f'solarflow-hub/{sf_device_id}/control/dischargeDuringDaytime',str(DISCHARGE_DURING_DAYTIME),retain=True)

    if SUNRISE_OFFSET == None:
        SUNRISE_OFFSET = cfg('control', 'sunrise_offset', 60, 'SUNRISE_OFFSET', 60)
        log.info(f'Updating SUNRISE_OFFSET from config file to {SUNRISE_OFFSET} minutes')
        client.publish(f'solarflow-hub/{sf_device_id}/control/sunriseOffset',SUNRISE_OFFSET,retain=True)

    if SUNSET_OFFSET == None:
        SUNSET_OFFSET = cfg('control', 'sunset_offset', 60, 'SUNSET_OFFSET', 60)
        log.info(f'Updating SUNSET_OFFSET from config file to {SUNSET_OFFSET} minutes')
        client.publish(f'solarflow-hub/{sf_device_id}/control/sunsetOffset',SUNSET_OFFSET,retain=True)

    if MIN_CHARGE_POWER == None:
        MIN_CHARGE_POWER = cfg('control', 'min_charge_power', 0, 'MIN_CHARGE_POWER', 0)
        log.info(f'Updating MIN_CHARGE_POWER from config file to {MIN_CHARGE_POWER}W')
        client.publish(f'solarflow-hub/{sf_device_id}/control/minChargePower',MIN_CHARGE_POWER,retain=True)

    if MAX_DISCHARGE_POWER == None:
        MAX_DISCHARGE_POWER = cfg('control', 'max_discharge_power', 145, 'MAX_DISCHARGE_POWER', 145)
        log.info(f'Updating MAX_DISCHARGE_POWER from config file to {MAX_DISCHARGE_POWER}W')
        client.publish(f'solarflow-hub/{sf_device_id}/control/maxDischargePower',MAX_DISCHARGE_POWER,retain=True)

    if BATTERY_LOW == None:
        BATTERY_LOW = cfg('control', 'battery_low', 2, 'BATTERY_LOW', 2)
        log.info(f'Updating BATTERY_LOW from config file to {BATTERY_LOW}%')
        client.publish(f'solarflow-hub/{sf_device_id}/control/batteryTargetSoCMin',BATTERY_LOW,retain=True)

    if BATTERY_HIGH == None:
        BATTERY_HIGH = cfg('control', 'battery_high', 98, 'BATTERY_HIGH', 98)
        log.info(f'Updating BATTERY_HIGH from config file to {BATTERY_HIGH}%')
        client.publish(f'solarflow-hub/{sf_device_id}/control/batteryTargetSoCMax',BATTERY_HIGH,retain=True)

    if GRID_CHARGE_ENABLED == None:
        GRID_CHARGE_ENABLED = cfg('control', 'grid_charge_enabled', False, 'GRID_CHARGE_ENABLED', False, 'getboolean')
        log.info(f'Updating GRID_CHARGE_ENABLED from config file to {GRID_CHARGE_ENABLED}')
        client.publish(f'solarflow-hub/{sf_device_id}/control/gridChargeEnabled',str(GRID_CHARGE_ENABLED),retain=True)



def run():
    log_build_info()

    hub_opts = getOpts(solarflow.Solarflow)
    ace_opts = getOpts(ace.Ace)
    dtuType = getattr(dtus, DTU_TYPE)
    dtu_opts = getOpts(dtuType)
    smtType = getattr(smartmeters, SMT_TYPE)
    smt_opts = getOpts(smtType)

    client = connect_mqtt()
    subscribe(client=client)

    log.info("Reading retained config settings from MQTT...")
    log.info("Note: Solarflow Control persists initial configuration settings in your MQTT broker and will use those first (if found) to allow on-the-fly updates!")
    log.info("If you want to override these values from your config.ini you need to clear those retained topics in your broker first!")
    client.loop_start()
    time.sleep(10)
  
    # if no config setting were found in MQTT (retained) then update config from config file
    updateConfigParams(client)

    log.info("Control Parameters:")
    log.info(f'  MIN_CHARGE_POWER = {MIN_CHARGE_POWER}')
    log.info(f'  MAX_DISCHARGE_LEVEL = {MAX_DISCHARGE_POWER}')
    log.info(f'  MAX_INVERTER_LIMIT = {MAX_INVERTER_LIMIT}')
    log.info(f'  MAX_INVERTER_INPUT = {MAX_INVERTER_INPUT}')
    log.info(f'  SUNRISE_OFFSET = {SUNRISE_OFFSET}')
    log.info(f'  SUNSET_OFFSET = {SUNSET_OFFSET}')
    log.info(f'  BATTERY_LOW = {BATTERY_LOW}')
    log.info(f'  BATTERY_HIGH = {BATTERY_HIGH}')
    log.info(f'  BATTERY_DISCHARGE_START = {BATTERY_DISCHARGE_START}')
    log.info(f'  DISCHARGE_DURING_DAYTIME = {DISCHARGE_DURING_DAYTIME}')
    log.info(f'  GRID_CHARGE_ENABLED = {GRID_CHARGE_ENABLED}')
    log.info(f'  DEVICE_PING_INTERVAL = {DEVICE_PING_INTERVAL}')

    
    hub = solarflow.Solarflow(client=client,callback=limit_callback,**hub_opts)
    aceUnit = ace.Ace(client=client,callback=limit_callback,**ace_opts)
    dtu = dtuType(client=client,ac_limit=MAX_INVERTER_LIMIT,callback=limit_callback,**dtu_opts)
    smt = smtType(client=client,callback=limit_callback, **smt_opts)

    client.user_data_set({"hub":hub, "dtu":dtu, "smartmeter":smt, "ace":aceUnit})

    # switch the callback function for received MQTT messages to the delegating function
    client.on_message = on_message

    infotimer = RepeatedTimer(120, deviceInfo, client)
    idletimer = RepeatedTimer(300, _checkIdleShutdown, client, hub)
    pingtimer = RepeatedTimer(DEVICE_PING_INTERVAL, checkDevicePresence, client)

    # subscribe Hub, DTU and Smartmeter so that they can react on received messages
    hub.subscribe()
    aceUnit.subscribe()
    dtu.subscribe()
    smt.subscribe()

    # ensure that the hubs min/max battery levels are set upon startup according to configuration, adjustments will be done if required by CT mode
    hub.setBatteryHighSoC(BATTERY_HIGH)
    hub.setBatteryLowSoC(BATTERY_LOW)

    # turn off the hub's buzzer (audio feedback for config settings change)
    hub.setBuzzer(False)
    # ensure hub's maximum inverter feed power is set according to configuration
    hub.setInverseMaxPower(MAX_INVERTER_INPUT)
    # ensure hub is in AC output mode
    hub.setACMode()
    # initially turn off bypass and disable auto-recover from bypass
    if hub.control_bypass:
        hub.setBypass(False)
        hub.setAutorecover(False)

def main(argv):
    global mqtt_host, mqtt_port, mqtt_user, mqtt_pwd
    global sf_device_id
    global location
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

    loc = MyLocation()
    if not LNG and not LAT:
        coordinates = loc.getCoordinates()
        if loc is None:
            coordinates = (LAT,LNG)
            log.info(f'Geocoordinates: {coordinates}')
    else:
        coordinates = (LAT,LNG)

    # location info for determining sunrise/sunset
    location = LocationInfo(timezone='Europe/Berlin',latitude=coordinates[0], longitude=coordinates[1])

    run()

if __name__ == '__main__':
    main(sys.argv[1:])
