# solarflow-ace

Transportschicht zwischen einem Zendure **Solarflow Hub 2000** samt **Ace 1500** und
**Home Assistant**. Hervorgegangen aus einem Fork von
[solarflow-control](https://github.com/reinhard-brandstaedter/solarflow-control); die
Regellogik ist entfernt. Geregelt wird ausschließlich in Home Assistant, dieser Dienst
reicht nur Daten und Befehle durch:

- fragt Hub und Ace jede Minute ab (`iot/<product_id>/<device_id>/properties/read`),
- veröffentlicht jeden Gerätebericht (`/<product_id>/<device_id>/properties/report`)
  unverändert als einzelne Topics,
- meldet die Entitäten per MQTT-Discovery bei Home Assistant an (alle 10 min erneut),
- führt die Erreichbarkeit von Geräten und Brücke.

Hub und Ace müssen per BT-Manager auf den lokalen Broker umgestellt sein.

## Konfiguration

Ausschließlich über `config.ini` (Vorlage: `src/config.example.ini`), im Container unter
`/solarflow/config.ini`. Der Container läuft als UID 568; die Datei muss für sie lesbar sein.

| Sektion        | Option       | Pflicht | Bedeutung                                   |
|----------------|--------------|---------|---------------------------------------------|
| `[solarflow]`  | `product_id` | ja      | Hub 2000: `A8yh63` (nur dieser wird unterstützt) |
|                | `device_id`  | ja      | Geräte-ID des Hubs                          |
| `[ace]`        | `product_id` | ja      | Ace 1500: `8bM93H`                          |
|                | `device_id`  | ja      | Geräte-ID des Ace                           |
| `[mqtt]`       | `mqtt_host`  | ja      | lokaler Broker                              |
|                | `mqtt_port`  | nein    | Vorgabe 1883                                |
|                | `mqtt_user`, `mqtt_pwd` | nein | Anmeldung am Broker                 |

Fehlt eine Sektion oder Pflichtoption, bricht der Start mit einer Meldung und Exit-Code 2 ab.

## Topics

| Topic | Richtung | Inhalt |
|-------|----------|--------|
| `solarflow-hub/<device_id>/telemetry/<property>` | aus | jede Eigenschaft eines Berichts, Wert unverändert |
| `solarflow-hub/<hub_id>/telemetry/batteries/<sn>/<property>` | aus | Werte je Akkupack (`packData`) |
| `solarflow-hub/<device_id>/availability` | aus, retained | `online`, solange der letzte Bericht ≤ 120 s alt ist, sonst `offline` |
| `solarflow-hub/bridge/availability` | aus, retained | `online` nach dem Verbinden, `offline` als Last Will und beim Beenden |
| `solarflow-transport/control/telemetryPolling` | ein, retained | `ON`/`OFF`: Abfrage der Geräte an/aus (Vorgabe `ON`) |
| `solarflow-transport/telemetry/telemetryPolling` | aus, retained | aktueller Zustand der Abfrage |
| `homeassistant/<typ>/solarflow-hub-<hub_id>-<name>/config`, `homeassistant/<typ>/ace-<ace_id>-<name>/config` | aus, retained | Discovery, Vorlagen in `src/solarflow/homeassistant/` |

Jede Entität hängt an beiden Availability-Topics (`availability_mode: all`); Sensoren
tragen zusätzlich `expire_after: 600`. Schalter und Zahlen schreiben direkt nach
`iot/<product_id>/<device_id>/properties/write`.

Ist die Abfrage pausiert (etwa nachts), melden sich die Geräte nicht und stehen auf
`offline` – das ist gewollt.

Die MQTT-Verbindung nutzt die feste Client-ID `solarflow-control` mit Clean Session; die
Abos werden bei jedem Verbinden neu gesetzt. Zwei Instanzen gegen denselben Broker werfen
sich daher gegenseitig hinaus.

## Verhalten bei Fehlern

- Unlesbare Berichte werden mit einer Warnung verworfen, der Prozess läuft weiter.
- Endet die MQTT-Schleife unerwartet (etwa durch eine Ausnahme in einem Callback), beendet
  sich der Prozess mit Exit-Code 1, damit Docker ihn neu startet.
- `SIGTERM` veröffentlicht `offline` auf der Brücken-Availability und endet mit 0.

## Tests

```sh
python3 -m unittest discover -s tests
```

`tests/fixtures/expected.json` hält Discovery-Configs und Telemetrie fest, die Home
Assistant erwartet. Ändert sich dort etwas, ändern sich Entitäten.

## Build und Deploy

Ein Push auf `master` baut per GitHub Actions (`.github/workflows/docker-publish.yml`)
`ghcr.io/tomtomb/solarflow-ace:master` und zusätzlich `master-<sha>` (Rückweg) für
linux/amd64. Lokal:

```sh
cd src && podman build -t solarflow-ace:test .
```

Betrieb als Container `solarflow-control` (Compose im elrond-Repo,
`smarthome/smart-home/docker-compose.yml`):

```sh
docker compose pull solarflow-control && docker compose up -d solarflow-control
```

Zurück auf einen früheren Stand: im Compose `image:` auf `…:master-<sha>` setzen.
