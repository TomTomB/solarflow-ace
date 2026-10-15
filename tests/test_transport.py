'''Transport tests without a broker. Run from the repository root:
    python3 -m unittest discover -s tests
fixtures/expected.json pins the discovery configs and telemetry topics/payloads Home
Assistant depends on. It equals the output of the code before the cleanup (8f44a71) for
the same reports, except for the select passMode value template.'''
import json
import pathlib
import sys
import types
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'src' / 'solarflow'))

import device  # noqa: E402
device.RepeatedTimer = lambda *args, **kwargs: None
from ace import Ace  # noqa: E402
from solarflow import Solarflow  # noqa: E402

FIXTURES = json.loads((HERE / 'fixtures' / 'reports.json').read_text())
EXPECTED = json.loads((HERE / 'fixtures' / 'expected.json').read_text())


class Client:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload=None, retain=False, qos=0):
        if isinstance(payload, (int, float)) and not isinstance(payload, bool):
            payload = str(payload)
        self.published.append([topic, payload, retain])


def message(topic, payload):
    return types.SimpleNamespace(topic=topic, payload=payload.encode() if isinstance(payload, str) else payload)


def report_topic(name):
    return f'/{FIXTURES[name]["product_id"]}/{FIXTURES[name]["device_id"]}/properties/report'


class TransportTest(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.hub = Solarflow(client=self.client, product_id=FIXTURES['hub']['product_id'],
                             device_id=FIXTURES['hub']['device_id'])
        self.ace = Ace(client=self.client, product_id=FIXTURES['ace']['product_id'],
                       device_id=FIXTURES['ace']['device_id'])
        self.client.published = []

    def deliver(self, topic, payload):
        for dev in (self.hub, self.ace):
            dev.handleMsg(message(topic, payload))

    def test_reports_are_forwarded_unchanged(self):
        for name in ('hub', 'ace'):
            self.deliver(report_topic(name), json.dumps(FIXTURES[name]['report']))
        telemetry = sorted(p for p in self.client.published if '/telemetry/' in p[0])
        self.assertEqual(telemetry, EXPECTED['telemetry'])
        self.assertEqual([self.hub.fwVersion, self.ace.fwVersion], EXPECTED['fw'])

    def test_discovery_is_unchanged(self):
        for name in ('hub', 'ace'):
            self.deliver(report_topic(name), json.dumps(FIXTURES[name]['report']))
        self.client.published = []
        self.hub.pushHomeassistantConfig()
        self.ace.pushHomeassistantConfig()
        discovery = {t: p for t, p, retain in self.client.published if retain}
        self.assertEqual(discovery, EXPECTED['discovery'])

    def test_report_makes_device_available(self):
        self.deliver(report_topic('hub'), json.dumps(FIXTURES['hub']['report']))
        self.assertIn(['solarflow-hub/k8k1Cx54/availability', 'online', True], self.client.published)
        self.assertNotIn(['solarflow-hub/0d9w2UDT/availability', 'online', True], self.client.published)

    def test_broken_json_is_ignored(self):
        for payload in (b'{"properties": {', b'\xff\xfe', b'', b'[1, 2]', b'42', b'null'):
            with self.assertLogs(level='WARNING'):
                self.deliver(report_topic('hub'), payload)
        self.assertEqual(self.client.published, [])
        self.assertIsNone(self.hub.isAvailable or None)

    def test_odd_values_do_not_abort_the_report(self):
        report = {'properties': {'masterSoftVersion': 'x', 'list': [1, 2], 'obj': {'a': 1}, 'electricLevel': 5},
                  'packData': [{'socLevel': 3}, {'sn': 'S1', 'totalVol': 5000}]}
        with self.assertLogs(level='WARNING'):
            self.deliver(report_topic('hub'), json.dumps(report))
        published = {t: p for t, p, _ in self.client.published}
        self.assertEqual(published['solarflow-hub/k8k1Cx54/telemetry/list'], '[1, 2]')
        self.assertEqual(published['solarflow-hub/k8k1Cx54/telemetry/obj'], '{"a": 1}')
        self.assertEqual(published['solarflow-hub/k8k1Cx54/telemetry/electricLevel'], '5')
        self.assertEqual(published['solarflow-hub/k8k1Cx54/telemetry/batteries/S1/totalVol'], '5000')
        self.assertEqual(self.hub.fwVersion, 'unknown')
        self.assertEqual(self.hub.batterySerials, ['S1'])

    def test_ace_prefers_master_firmware_version(self):
        report = {'properties': {'masterFirmwareVersion': 4110, 'masterSoftVersion': 4105}}
        self.deliver(report_topic('ace'), json.dumps(report))
        self.assertEqual(self.ace.fwVersion, '1.0.14')
        self.client.published = []
        self.ace.pushHomeassistantConfig()
        configs = [json.loads(p) for _, p, retain in self.client.published if retain and p]
        self.assertTrue(configs)
        self.assertEqual({c['dev']['sw_version'] for c in configs}, {'1.0.14'})

    def test_hub_keeps_master_soft_version(self):
        report = {'properties': {'masterFirmwareVersion': 4110, 'masterSoftVersion': 12312}}
        self.deliver(report_topic('hub'), json.dumps(report))
        self.assertEqual(self.hub.fwVersion, '3.0.24')

    def test_other_topics_are_ignored(self):
        self.deliver('solarflow-hub/k8k1Cx54/telemetry/electricLevel', '57')
        self.deliver('/A8yh63/OTHER/properties/report', json.dumps(FIXTURES['hub']['report']))
        self.assertEqual(self.client.published, [])


if __name__ == '__main__':
    unittest.main()
