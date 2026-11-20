"""Ontvang BIOHUB-metingen via HTTP, bewaar ze en lever een live dashboard.

Start vanuit PowerShell met een ingestelde BIOHUB_TOKEN:
    python "server script python voor hub.py" --listen 0.0.0.0 --port 8765

Een Python-retourwaarde is iets anders dan een HTTP-antwoord: de functies
van Handler geven None terug aan Python, maar sturen gegevens naar de client.
"""

import argparse
import hmac
import json
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Tuple, cast
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parent
# Gebruik het dashboard uit het wifipakket naast de map Scripts.
DASHBOARD_PATH = ROOT.parent / "BIOHUB-WiFi" / "dashboard.html"
DEFAULT_DATABASE = DASHBOARD_PATH.parent / "biohub.sqlite3"
# Verwacht bericht, bijvoorbeeld: BIOHUB1,abcdef01,1,1200,in,84
# De vijf groepen tussen haakjes worden hieronder uitgelezen als
# boot, seq, uptime, event en duration, in die volgorde.
# re.VERBOSE maakt witruimte en uitleg in het patroon mogelijk.
# rb betekent dat dit patroon de ontvangen bytes controleert.
EVENT = re.compile(rb"""
    BIOHUB1,                       # Vaste aanduiding van het berichtformaat.
    ([0-9a-f]{8}),                  # Opstart-ID: precies 8 hexadecimale tekens (0-9, a-f).
    ([0-9]{1,10}),                  # Volgnummer van de gebeurtenis: 1 tot 10 cijfers.
    ([0-9]{1,10}),                  # Tijd sinds opstarten, in milliseconden: 1 tot 10 cijfers.
    (in|uit|timeout|onduidelijk),    # Toegestaan type gebeurtenis.
    ([0-9]{1,10})                   # Duur van de passage, in milliseconden: 1 tot 10 cijfers.
""", re.VERBOSE)

# Hub-ID: 1 tot 32 tekens; alleen letters, cijfers, underscores en koppeltekens.
# De aanroep fullmatch() vereist dat het volledige ID aan deze regel voldoet.
HUB = re.compile(r"[A-Za-z0-9_-]{1,32}")


def period_boundaries(period: str, now: int) -> Tuple[List[int], str]:
    """Bepaal tijdvakgrenzen voor de gekozen dashboardperiode.

    Vandaag begint om lokale middernacht; deze week begint op maandag.
    Kalendergrenzen gebruiken de Windows-tijdzone, inclusief zomer-/wintertijd.
    Alleen begonnen tijdvakken worden getoond; het laatste vak is nog bezig.

    :param period: Een van 15m, hour, today of week.
    :type period: str
    :param now: Huidige Unix-tijd in hele seconden.
    :type now: int
    :returns: Oplopende Unix-grenzen (één meer dan het aantal vakken) en periodenaam.
    :rtype: tuple[list[int], str]
    :raises ValueError: Als de periode niet wordt ondersteund.
    """
    current_minute = now // 60 * 60
    if period in ("15m", "hour"):
        count = 15 if period == "15m" else 60
        return list(range(current_minute - (count - 1) * 60,
                          current_minute + 61, 60)), (
                              "Laatste 15 minuten" if period == "15m" else "Afgelopen uur")
    local = datetime.fromtimestamp(now)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "today":
        start = int(midnight.timestamp())
        # Absolute uren houden de twee verschillende 02:00-uren in oktober apart.
        return list(range(start, start + ((now - start) // 3600 + 2) * 3600, 3600)), "Vandaag"
    if period == "week":
        monday = midnight - timedelta(days=local.weekday())
        return [int((monday + timedelta(days=day)).timestamp())
                for day in range(local.weekday() + 2)], "Deze week"
    raise ValueError("Kies 15m, hour, today of week.")


def chart_data(database: Path, period: str, now: int) -> Dict[str, object]:
    """Lees tellingen voor de app, met lege tijdvakken en periodetotalen.

    Gegevens worden op ontvangsttijd gegroepeerd: per minuut voor 15m/hour,
    per uur voor today en per kalenderdag voor week. Diagnose telt apart.
    activity is In + Uit; complete geeft aan of het tijdvak is afgelopen.
    De ongewijzigde minutes-velden blijven beschikbaar bij de periode 15m,
    zodat de bestaande YAML-tests en andere clients blijven werken.

    :param database: Pad naar de geïnitialiseerde SQLite-database.
    :type database: pathlib.Path
    :param period: Gewenste periode: 15m, hour, today of week.
    :type period: str
    :param now: Tijdstip waarop het overzicht wordt samengesteld, als Unix-seconden.
    :type now: int
    :returns: JSON-geschikte periode-informatie, buckets, totalen en laatste ontvangsttijd.
    :rtype: dict[str, object]
    :raises ValueError: Als period ongeldig is.
    :raises sqlite3.Error: Als de database niet kan worden gelezen.
    """
    bounds, title = period_boundaries(period, now)
    # De oorspronkelijke 15m-koppeling omvatte de hele lopende minuut.
    # Nieuwe periodes tellen tot en met de huidige seconde.
    end = bounds[-1] if period == "15m" else now + 1
    counts: List[Dict[str, int]] = [
        {"time": start, "in": 0, "uit": 0, "diagnose": 0} for start in bounds[:-1]
    ]
    parameters = []
    for index, (start, stop) in enumerate(zip(bounds, bounds[1:])):
        parameters.extend((index, start, min(stop, end)))
    placeholders = ",".join("(?,?,?)" for _ in counts)
    with closing(sqlite3.connect(database)) as db:
        # Beide SELECTs zien dezelfde databaseversie, ook tijdens nieuwe passages.
        db.execute("BEGIN")
        rows = db.execute(
            "WITH periods(bucket, start, stop) AS (VALUES " + placeholders + ") "
            "SELECT periods.bucket, events.event, COUNT(*) FROM periods "
            "JOIN events ON events.received >= periods.start AND events.received < periods.stop "
            "GROUP BY periods.bucket, events.event", parameters,
        )
        for index, event, count in rows:
            key = event if event in ("in", "uit") else "diagnose"
            counts[index][key] += count
        last_received = db.execute("SELECT MAX(received) FROM events").fetchone()[0]
    weekdays = ("ma", "di", "wo", "do", "vr", "za", "zo")
    labels = []
    for start in bounds[:-1]:
        local = datetime.fromtimestamp(start)
        labels.append("{} {}".format(weekdays[local.weekday()], local.strftime("%d-%m"))
                      if period == "week" else local.strftime("%H:%M"))
    buckets = []
    for index, count in enumerate(counts):
        label = labels[index]
        if labels.count(label) > 1:
            label += datetime.fromtimestamp(bounds[index]).astimezone().strftime(" %z")
        buckets.append(dict(count, end=min(bounds[index + 1], end), label=label,
                            activity=count["in"] + count["uit"],
                            complete=bounds[index + 1] <= now))
    totals = {key: sum(count[key] for count in counts) for key in ("in", "uit", "diagnose")}
    totals["activity"] = totals["in"] + totals["uit"]
    result: Dict[str, object] = {
        "period": period, "period_label": title,
        "start": bounds[0], "end": end, "generated_at": now,
        "timezone": datetime.fromtimestamp(now).astimezone().tzname(),
        "time_basis": "received",
        "bucket_label": "dag" if period == "week" else "uur" if period == "today" else "minuut",
        "buckets": buckets,
        "totals": totals,
        "last_received": last_received,
        "last_received_label": (datetime.fromtimestamp(last_received).strftime("%d-%m-%Y %H:%M:%S")
                                if last_received is not None else None),
    }
    if period == "15m":
        result["minutes"] = counts
    return result


class Handler(BaseHTTPRequestHandler):
    """Verwerk HTTP-verzoeken voor het dashboard en de passageopslag."""

    @property
    def biohub_server(self) -> "BiohubServer":
        """Geef de server door met zijn BIOHUB-specifieke type."""
        return cast("BiohubServer", self.server)

    def setup(self) -> None:
        """Bereid de verbinding voor met een socket-time-out van vijf seconden.

        Heeft geen functieparameters. De HTTP-server roept deze methode aan
        voordat een verzoek wordt gelezen. Een vastgelopen verbinding mag
        maximaal vijf seconden op een socketbewerking wachten.

        :returns: Geen waarde; stelt de lees- en schrijfstreams in.
        :rtype: None
        :raises OSError: Als de socket niet kan worden ingesteld.
        """
        self.request.settimeout(5)
        super().setup()

    def respond(self, status: int, content: bytes, content_type: str) -> None:
        """Stuur een HTTP-antwoord dat de browser niet mag cachen.

        :param status: HTTP-statuscode, bijvoorbeeld 200 of 400.
        :type status: int
        :param content: De inhoud van het antwoord als bytes, bijvoorbeeld
            b'{"stored": true}'. Tekst moet eerst met encode worden omgezet.
        :type content: bytes
        :param content_type: MIME-type, eventueel met tekencodering.
        :type content_type: str
        :returns: Geen waarde; schrijft headers en inhoud naar de verbinding.
        :rtype: None
        :raises OSError: Als het antwoord niet kan worden verzonden.
        """
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def send_json(self, status: int, data: object) -> None:
        """Zet gegevens om naar JSON en stuur ze als UTF-8-antwoord.

        :param status: HTTP-statuscode voor het antwoord.
        :type status: int
        :param data: Gegevens die naar JSON kunnen worden omgezet, bijvoorbeeld
            {"stored": True}. Gebruik dictionaries, lijsten, tekst, getallen,
            booleans of None; geef hier geen reeds gecodeerde bytes mee.
        :type data: object
        :returns: Geen waarde; verstuurt het JSON-antwoord.
        :rtype: None
        :raises TypeError: Als data een niet-ondersteund object bevat.
        :raises ValueError: Als data bijvoorbeeld een kringverwijzing bevat.
        :raises OSError: Als het antwoord niet kan worden verzonden.
        """
        self.respond(status, json.dumps(data).encode("utf-8"), "application/json")

    def do_POST(self) -> None:
        """Controleer en bewaar een gebeurtenis voor POST /api/events.

        Leest pad, headers en berichtinhoud uit deze verzoekinstantie.
        Heeft geen functieparameters: de HTTP-server vult self.path,
        self.headers en self.rfile in voordat deze methode wordt aangeroepen.
        De combinatie hub-ID, opstart-ID en volgnummer voorkomt dubbeltellingen.
        Voorbeeld van een bericht: BIOHUB1,abcdef01,1,1200,in,84.
        Dit is passage 1 van opstart abcdef01, 1200 ms na opstarten,
        met richting in en een duur van 84 ms.
        Antwoordt met 201 bij nieuwe opslag, 200 bij een duplicaat, 400 bij
        ongeldige invoer, 401 bij een ongeldige token, 404 bij een ander pad
        en 503 bij een databasefout. Databasefouten worden hier afgehandeld.

        :returns: None aan de Python-aanroeper. De verzender ontvangt via HTTP
            bijvoorbeeld {"stored": True} bij nieuwe opslag of
            {"stored": False} wanneer hetzelfde bericht al was opgeslagen.
        :rtype: None
        :raises OSError: Bij een lees-, schrijf- of socket-time-outfout.
        """
        if self.path != "/api/events":
            self.send_error(404)
            return
        token = self.headers.get("X-Biohub-Token", "").encode("utf-8")
        if not hmac.compare_digest(token, self.biohub_server.token):
            self.send_json(401, {"error": "Ongeldige BIOHUB-token"})
            return
        hub = self.headers.get("X-Biohub-Id", "")
        length = self.headers.get("Content-Length", "")
        if not HUB.fullmatch(hub) or not re.fullmatch(r"[0-9]{1,3}", length):
            self.send_json(400, {"error": "Controleer hub-ID en berichtlengte"})
            return
        if not 1 <= int(length) <= 128:
            self.send_json(400, {"error": "Ongeldige berichtlengte"})
            return
        raw = self.rfile.read(int(length))
        match = EVENT.fullmatch(raw.rstrip(b"\r\n"))
        if not match or len(raw) != int(length):
            self.send_json(400, {"error": "Ongeldige BIOHUB1-regel"})
            return
        boot, seq, uptime, event, duration = (part.decode("ascii") for part in match.groups())
        try:
            with closing(sqlite3.connect(self.biohub_server.database)) as db, db:
                cursor = db.execute(
                    """INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (hub, boot, seq) DO NOTHING""",
                    (hub, boot, int(seq), int(uptime), event, int(duration), int(time.time())),
                )
                stored = cursor.rowcount == 1
        except sqlite3.Error:
            self.send_json(503, {"error": "Opslag tijdelijk niet beschikbaar"})
            return
        self.send_json(201 if stored else 200, {"stored": stored})

    def do_GET(self) -> None:
        """Lever het mobiele dashboard of tellingen voor de gekozen periode.

        Heeft geen functieparameters; leest het pad uit self.path.
        Het pad / levert HTML;
        /api/chart?period=hour levert bijvoorbeeld het afgelopen uur.
        Toegestaan zijn 15m (standaard), hour, today en week. De uitvoer bevat
        buckets met time, end, label, in, uit en diagnose, periodetotalen en
        last_received (Unix-tijd in seconden, of None bij een lege database).
        Tellingen gebruiken de ontvangsttijd op de pc. Een onbekend pad geeft
        404; een databasefout geeft 503 en wordt hier afgehandeld.

        :returns: Geen waarde; schrijft HTML of JSON naar de verbinding.
        :rtype: None
        :raises OSError: Als het antwoord niet kan worden verzonden.
        """
        url = urlsplit(self.path)
        if url.path == "/":
            self.respond(200, self.biohub_server.dashboard, "text/html; charset=utf-8")
            return
        if url.path != "/api/chart":
            self.send_error(404)
            return
        query = parse_qs(url.query, keep_blank_values=True)
        if set(query) - {"period"} or len(query.get("period", [])) > 1:
            self.send_json(400, {"error": "Gebruik één period: 15m, hour, today of week."})
            return
        period = query.get("period", ["15m"])[0]
        try:
            data = chart_data(self.biohub_server.database, period, int(time.time()))
        except ValueError as error:
            self.send_json(400, {"error": str(error)})
            return
        except sqlite3.Error:
            self.send_json(503, {"error": "Opslag tijdelijk niet beschikbaar"})
            return
        self.send_json(200, data)

    def log_message(self, format: str, *args: object) -> None:
        """Schrijf een logregel, behalve voor de herhaalde grafiekverzoeken.

        :param format: Opmaaksjabloon met procentnotatie van de HTTP-server.
        :type format: str
        :param args: Waarden die in de opmaaksjabloon worden ingevuld.
        :type args: object
        :returns: Geen waarde; schrijft zo nodig naar standaardfoutuitvoer.
        :rtype: None
        """
        # De browser vraagt de grafiek elke seconde op.
        if urlsplit(getattr(self, "path", "")).path != "/api/chart":
            super().log_message(format, *args)


class BiohubServer(ThreadingHTTPServer):
    """HTTP-server met expliciet getypeerde BIOHUB-instellingen.

    Elke Handler gebruikt dezelfde token, database en dashboardinhoud.
    De constructor zet deze gegevens klaar voordat verzoeken binnenkomen.
    """

    def __init__(
        self,
        address: Tuple[str, int],
        token: bytes,
        database: Path,
        dashboard: bytes,
    ) -> None:
        """Bewaar instellingen en open de netwerkpoort voor de server.

        :param address: Paar (luisteradres, poort), bijvoorbeeld ('0.0.0.0', 8765).
            Het adres 0.0.0.0 accepteert verbindingen op alle IPv4-netwerkinterfaces.
        :type address: tuple[str, int]
        :param token: Verwachte geheime token, gecodeerd als ASCII-bytes.
        :type token: bytes
        :param database: Pad naar de reeds geïnitialiseerde SQLite-database.
        :type database: pathlib.Path
        :param dashboard: Volledige HTML-inhoud die bij GET / wordt verstuurd.
        :type dashboard: bytes
        :returns: Geen waarde; initialiseert de server. Verzoeken worden pas
            verwerkt wanneer serve_forever wordt aangeroepen.
        :rtype: None
        :raises OSError: Als het luisteradres of de poort niet beschikbaar is.
        """
        self.token = token
        self.database = database
        self.dashboard = dashboard
        super().__init__(address, Handler)


def initialize_database(path: Path) -> None:
    """Maak de gebeurtenissentabel en tijdindex aan als ze nog ontbreken.

    Bestaande metingen blijven behouden. De verbinding wordt na afloop gesloten.

    :param path: Bestandspad van de SQLite-database.
    :type path: pathlib.Path
    :returns: Geen waarde; maakt of initialiseert het databasebestand.
    :rtype: None
    :raises sqlite3.Error: Als openen of initialiseren van de database mislukt.
    """
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS events (
            hub TEXT NOT NULL, boot TEXT NOT NULL, seq INTEGER NOT NULL,
            uptime INTEGER NOT NULL, event TEXT NOT NULL, duration INTEGER NOT NULL,
            received INTEGER NOT NULL, PRIMARY KEY (hub, boot, seq))""")
        db.execute("CREATE INDEX IF NOT EXISTS events_received ON events(received)")


def main() -> None:
    """Start de lokale server met opdrachtregelopties en BIOHUB_TOKEN.

    Leest --listen, --port en --database uit de opdrachtregel en de token uit
    de omgevingsvariabele BIOHUB_TOKEN. Blijft verzoeken verwerken tot Ctrl+C;
    daarna wordt de server gesloten. Deze functie heeft geen parameters.

    :returns: Geen waarde; keert terug nadat de server met Ctrl+C is gestopt.
    :rtype: None
    :raises SystemExit: Bij --help, ongeldige opties of een ongeldige token.
    :raises sqlite3.Error: Als de database niet kan worden geïnitialiseerd.
    :raises OSError: Als de poort of het dashboardbestand niet kan worden geopend.
    """
    parser = argparse.ArgumentParser(description="Lokale BIOHUB-server")
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    parser.add_argument("--database", default=DEFAULT_DATABASE, type=Path)
    args = parser.parse_args()
    token = os.environ.get("BIOHUB_TOKEN", "")
    if not re.fullmatch(r"[a-zA-Z0-9_-]{16,128}", token):
        parser.error("Stel BIOHUB_TOKEN in op 16 tot 128 letters, cijfers, - of _.")

    initialize_database(args.database)

    with BiohubServer(
        (args.listen, args.port),
        token.encode("ascii"),
        args.database,
        DASHBOARD_PATH.read_bytes(),
    ) as server:
        print(f"BIOHUB luistert op {args.listen}:{server.server_port}", flush=True)
        print(f"Dashboard op deze pc: http://localhost:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
