'''Solarflow Hub 2000: device report plus the battery packs behind it.'''
import logging

from device import Device, mqtt_payload

log = logging.getLogger(__name__)

# Templates rendered once per battery pack instead of once per hub.
BATTERY_TEMPLATES = {'maxTemp', 'totalVol', 'soh'}


class Solarflow(Device):
    name = 'Hub'
    template_dir = 'hub'
    discovery_prefix = 'solarflow-hub'

    def __init__(self, client, product_id: str, device_id: str, telemetry_polling_enabled: bool = True):
        # Serial numbers of the battery packs in the order they first reported.
        self.batterySerials: list[str] = []
        super().__init__(client, product_id, device_id, telemetry_polling_enabled)

    def templateInstances(self, cfg_name: str):
        if cfg_name not in BATTERY_TEMPLATES:
            yield cfg_name, {}
            return
        for index, serial in enumerate(self.batterySerials):
            yield f'{serial}-{cfg_name}', {'battery_serial': serial, 'battery_index': index + 1}

    def handleReport(self, payload: dict):
        super().handleReport(payload)

        packs = payload.get('packData')
        if isinstance(packs, list):
            for pack in packs:
                if not isinstance(pack, dict) or 'sn' not in pack:
                    log.warning(f'Hub: ignoring battery pack without serial number: {pack!r}')
                    continue
                serial = str(pack['sn'])
                if serial not in self.batterySerials:
                    self.batterySerials.append(serial)
                for prop, value in pack.items():
                    if prop != 'sn':
                        self.client.publish(f'{self.telemetryPrefix}/batteries/{serial}/{prop}',
                                            mqtt_payload(value))
            self.markAlive()
