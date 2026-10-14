from threading import Timer

# Availability topic of the bridge process itself, separate from the per-device topics.
# It is published as 'online' on connect and registered as the MQTT last will, so that a
# bridge that crashes or is killed marks every entity unavailable. Without it the retained
# 'online' of the per-device topics survives the process and Home Assistant keeps serving
# the last received value forever, which is indistinguishable from a live measurement.
BRIDGE_AVAILABILITY_TOPIC = 'solarflow-hub/bridge/availability'
AVAILABILITY_PAYLOAD_ONLINE = 'online'
AVAILABILITY_PAYLOAD_OFFLINE = 'offline'

# Backstop written into the Home Assistant discovery configs of all telemetry sensors: a
# sensor that receives nothing for this long reports itself unavailable, independent of any
# availability topic and independent of this process still running.
TELEMETRY_EXPIRE_AFTER_SECONDS = 600


class RepeatedTimer:
    '''Calls function every interval seconds. The timer threads are daemons: they must
    never keep the process alive once the MQTT loop in the main thread has ended.'''

    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.daemon = True
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def str2bool(val) -> bool:
    return str(val).strip().lower() in ('y', 'yes', 't', 'true', 'on', '1')
