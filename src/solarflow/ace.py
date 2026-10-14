'''Ace 1500: forwards the device report, nothing else.'''
from device import Device


class Ace(Device):
    name = 'Ace'
    template_dir = 'ace'
    discovery_prefix = 'ace'
