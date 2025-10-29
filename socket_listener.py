import argparse
import csv
import datetime
import socket
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from db import SessionLocal, init_db
from models import (
    Base, Driver, Event, RaceClass, Session as RaceSession, Entry, Lap, Result,
    BasisEnum, SessionTypeEnum
)

# --- Helpers ---------------------------------------------------------------

def parseTimeSTR(s: str) -> Optional[float]:
   
    s = (s or "").strip().strip('"')
    if not s or s in {"0", "00:00.000", "00:00:00", "00:00:00.000"}:
        return None
    try:
        parts = s.split(":")
        if len(parts) == 3:
            h = int(parts[0]); m = int(parts[1]); sec = float(parts[2])
            return h * 3600 + m * 60 + sec
        elif len(parts) == 2:
            m = int(parts[0]); sec = float(parts[1])
            return m * 60 + sec
        else:
            return float(parts[0])
    except:
        return None

def csvFields(line: str) -> List[str]:
    reader = csv.reader([line.rstrip("\r\n")], delimiter=',', quotechar='"', skipinitialspace=False)
    return next(reader)

def fmt(sec: Optional[float]) -> str:
    if sec is None:
        return ""
    if sec >= 60:
        m = int(sec // 60); s = sec - 60 * m
        return f"{m}:{s:06.3f}"
    return f"{sec:.3f}"

def to_ms(seconds: Optional[float]) -> Optional[int]:
    return int(round(seconds * 1000)) if seconds is not None else None


# --- State containers ------------------------------------------------------

@dataclass
class DriverState:
    number: str
    first: str = ""
    last: str = ""
    team: str = ""
    chassis: str = ""
    active: bool = True

@dataclass
class TimingState:
    session_name: str = ""
    class_name: str = ""
    track_name: str = ""
    track_length: Optional[float] = None
    flag: str = ""
    last_lap: Dict[str, Optional[float]] = field(default_factory=dict)  # seconds
    best_lap: Dict[str, Optional[float]] = field(default_factory=dict)  # seconds
    order: List[str] = field(default_factory=list)  # list of numbers in running order (1-based)
    drivers: Dict[str, DriverState] = field(default_factory=dict)


# --- Parser ---------------------------------------------------------------

class OrbitsParser:
    def __init__(self):
        self.s = TimingState()

    def parseLine(self, line: str):
        if not line.strip():
            return
        try:
            f = csvFields(line)
        except:
            return
        if not f or not f[0].startswith("$"):
            return

        tag = f[0]

        if tag == "$B" and len(f) >= 3:
            self.s.session_name = f[2].strip('"')

        elif tag == "$C" and len(f) >= 3:
            self.s.class_name = f[2].strip('"')

        elif tag == "$E" and len(f) >= 3:
            key = f[1].strip('"').upper()
            val = f[2].strip('"')
            if key == "TRACKNAME":
                self.s.track_name = val
            elif key == "TRACKLENGTH":
                try:
                    self.s.track_length = float(val)
                except:
                    pass

        elif tag == "$A":
            number = f[1].strip('"')
            if not number:
                return
            d = self.s.drivers.get(number, DriverState(number))
            d.first   = f[4].strip('"') if len(f) > 4 else d.first
            d.last    = f[5].strip('"') if len(f) > 5 else d.last
            d.chassis = f[6].strip('"') if len(f) > 6 else d.chassis
            if len(f) > 7:
                try:
                    d.active = int(f[7]) == 1
                except:
                    pass
            self.s.drivers[number] = d

        elif tag == "$COMP":
            number = f[1].strip('"')
            if not number:
                return
            d = self.s.drivers.get(number, DriverState(number))
            d.first   = f[4].strip('"') if len(f) > 4 else d.first
            d.last    = f[5].strip('"') if len(f) > 5 else d.last
            d.chassis = f[6].strip('"') if len(f) > 6 else d.chassis
            d.team    = f[7].strip('"') if len(f) > 7 else d.team
            self.s.drivers[number] = d

        elif tag == "$F":
            if len(f) > 5:
                self.s.flag = f[5].strip('"').strip()

        elif tag == "$G":
            # $G,<position>,"<number>",,"<last_lap>"
            number = f[2].strip('"')
            try:
                pos = int(f[1])
            except:
                return

            # Remove existing occurrance, then insert at pos-1
            self.s.order = [c for c in self.s.order if c != number]
            while len(self.s.order) < pos - 1:
                self.s.order.append("")
            self.s.order.insert(pos - 1, number)
            # cleanup trailing placeholders (fix)
            while self.s.order and self.s.order[-1] == "":
                self.s.order.pop()

        elif tag == "$H" and len(f) >= 5:
            # $H,<pos>,"<number>",0,"<last>"
            number = f[2].strip('"')
            lap = parseTimeSTR(f[4])
            if lap is not None:
                self.s.last_lap[number] = lap

        elif tag == "$SP" and len(f) >= 5:
            number = f[2].strip('"')
            lap = parseTimeSTR(f[4])
            if lap is not None:
                self.s.last_lap[number] = lap

        elif tag == "$SR" and len(f) >= 5:
            # $SR,<pos>,"<number>",,"<best>",0
            number = f[2].strip('"')
            best = parseTimeSTR(f[4])
            if best is not None:
                cur = self.s.best_lap.get(number)
                self.s.best_lap[number] = min(best, cur) if cur is not None else best

        elif tag == "$J" and len(f) >= 3:
            # $J,"<number>","<best>","<last>"
            number = f[1].strip('"')
            best = parseTimeSTR(f[2])
            if best is not None:
                cur = self.s.best_lap.get(number)
                self.s.best_lap[number] = min(best, cur) if cur is not None else best


# --- DB ingest ------------------------------------------------------------

class DBIngestor:
    def __init__(self, SessionLocal, event_id: int, class_id: int, session_type: str):
        self.SessionLocal = SessionLocal
        self.event_id = event_id
        self.class_id = class_id
        self.session_type = session_type
        self.lap_counter: Dict[str, int] = {}

    def get_or_create_driver(self, db_session, number: str, driver_state: DriverState) -> Driver:
       
        driver = (
            db_session.query(Driver)
            .filter(
                getattr(Driver, "first_name") == driver_state.first,
            )
            .first()
        )
        if not driver:
            driver = Driver(
                first_name=driver_state.first,
                last_name=driver_state.last,
                team=driver_state.team,
                chassis=driver_state.chassis,
            )
            db_session.add(driver)
            db_session.commit()
            db_session.refresh(driver)
        else:
            # optional: keep metadata fresh if provided
            changed = False
            if driver_state.first and driver.first_name != driver_state.first:
                driver.first_name = driver_state.first; changed = True
            if driver_state.last and driver.last_name != driver_state.last:
                driver.last_name = driver_state.last; changed = True
            if driver_state.team and driver.team != driver_state.team:
                driver.team = driver_state.team; changed = True
            if driver_state.chassis and driver.chassis != driver_state.chassis:
                driver.chassis = driver_state.chassis; changed = True
            if changed:
                db_session.commit()
        return driver

    def get_or_create_entry(self, db: Session, drv: Driver, number: str) -> Entry:
        ent = (
            db.query(Entry)
            .filter(
                Entry.event_id == self.event_id,
                Entry.class_id == self.class_id,
                Entry.number == number,
            )
            .first()
        )
        if not ent:
            ent = Entry(
                event_id=self.event_id,
                class_id=self.class_id,
                driver_id=drv.id,
                number=number,
                transponder=None,
            )
            db.add(ent)
            db.commit()
            db.refresh(ent)
        elif ent.driver_id != drv.id:
            ent.driver_id = drv.id
            db.commit()
        return ent

    def get_or_create_result(self, db: Session, session_id: int, driver_id: int) -> Result:
        res = (
            db.query(Result)
            .filter(
                Result.session_id == session_id,
                Result.driver_id == driver_id,
                Result.basis == "provisional",
                Result.version == 1,
            )
            .first()
        )
        if not res:
            res = Result(
                session_id=session_id,
                driver_id=driver_id,
                basis="provisional",
                version=1,
            )
            db.add(res)
            db.commit()
            db.refresh(res)
        return res

    def get_or_create_session(self, db: Session, session_name: str) -> RaceSession:
        sess = (
            db.query(RaceSession)
            .filter(
                RaceSession.event_id == self.event_id,
                RaceSession.class_id == self.class_id,
                RaceSession.session_name == session_name,
            )
            .first()
        )
        if not sess:
            sess = RaceSession(
                event_id=self.event_id,
                class_id=self.class_id,
                session_name=session_name or self.session_type,
                session_type=self.session_type,
                status="live",
            )
            db.add(sess)
            db.commit()
            db.refresh(sess)
        return sess

    def apply(self, parsed: OrbitsParser):
        s = parsed.s
        with self.SessionLocal() as db_session:
            # 1) Ensure the session exists (per event/class + session_name)
            session = self.get_or_create_session(db_session, s.session_name)

            # 2) Ensure Drivers + Entries exist; build num->driver_id map
            num_to_driver_id: Dict[str, int] = {}
            for number, driver_state in s.drivers.items():
                drv = self.get_or_create_driver(db_session, number, driver_state)
                self.get_or_create_entry(db_session, drv, number)  # bind to (event_id, class_id)
                num_to_driver_id[number] = drv.id

            # 3) Normalize ordering and compute the leader's best lap (ms) once
            ordered_nums = [c for c in s.order if c and c in num_to_driver_id]

            leader_best_ms: Optional[int] = None
            for c in ordered_nums:
                b_sec = s.best_lap.get(c)
                if b_sec is not None:
                    leader_best_ms = to_ms(b_sec)
                    break

            # 4) Upsert results for each ordered driver
            for idx, number in enumerate(ordered_nums, start=1):
                driver_id = num_to_driver_id[number]
                result = self.get_or_create_result(db_session, session.id, driver_id)

                last_sec = s.last_lap.get(number)
                best_sec = s.best_lap.get(number)

                last_ms = to_ms(last_sec)
                best_ms = to_ms(best_sec)

                result.position = idx
                result.last_lap_ms = last_ms

                # Only improve best (avoid overwriting with None or slower)
                if best_ms is not None and (result.best_lap_ms is None or best_ms < result.best_lap_ms):
                    result.best_lap_ms = best_ms

                if leader_best_ms is not None and result.best_lap_ms is not None:
                    gap = result.best_lap_ms - leader_best_ms
                    result.gap_to_p1_ms = gap if gap >= 0 else 0
                else:
                    result.gap_to_p1_ms = None

            # 5) Append Lap rows for any last laps we saw this tick
            for number, last_sec in s.last_lap.items():
                if last_sec is None or number not in num_to_driver_id:
                    continue
                driver_id = num_to_driver_id[number]
                next_idx = self.lap_counter.get(number, 1)

                db_session.add(
                    Lap(
                        session_id=session.id,
                        driver_id=driver_id,
                        lap_number=next_idx,
                        lap_time_ms=to_ms(last_sec),
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        is_valid=1,
                    )
                )
                self.lap_counter[number] = next_idx + 1

            db_session.commit()

class OrbitsTCPReader:
   
    def __init__(
        self,
        host: str,
        port: int,
        parser: "OrbitsParser",
        ingestor: "DBIngestor",
        connect_timeout: float = 5.0,
        read_timeout: float = 5.0,
        max_backoff: float = 10.0,
    ):
        self.host = host
        self.port = port
        self.parser = parser
        self.ingestor = ingestor
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_backoff = max_backoff
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        import socket, time
        backoff = 1.0
        while not self._stop:
            sock = None
            f = None
            try:
                # Connect
                sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
                sock.settimeout(self.read_timeout)
                # Wrap with text file for clean line iteration
                f = sock.makefile("r", encoding="utf-8", errors="ignore", newline="\n")

                # Reset backoff after successful connection
                backoff = 1.0

                # Read loop: one Orbits packet per line
                for line in f:
                    if self._stop:
                        break
                    line = line.strip("\r\n")
                    if not line:
                        continue

                    # Parse & ingest
                    self.parser.parseLine(line)
                    self.ingestor.apply(self.parser)

                # Connection closed by peer; loop will reconnect
            except (socket.timeout, ConnectionError, OSError):
                time.sleep(backoff)
                backoff = min(self.max_backoff, backoff * 2)
            finally:
                try:
                    if f: f.close()
                except Exception:
                    pass
                try:
                    if sock: sock.close()
                except Exception:
                    pass

if __name__ == "__main__":
    import argparse

    # Initialize DB (uses your existing init_db)
    init_db()

    ap = argparse.ArgumentParser(description="Orbits TCP -> Parser -> DB ingestor")
    ap.add_argument("--host", default="127.0.0.1", help="Orbits TCP host")
    ap.add_argument("--port", type=int, default=50000, help="Orbits TCP port")
    ap.add_argument("--event-id", type=int, required=True, help="Event ID")
    ap.add_argument("--class-id", type=int, required=True, help="Class ID")
    ap.add_argument(
        "--session-type",
        choices=["Practice", "Qualifying", "Heat", "Prefinal", "Final"],
        required=True,
        help="Session type enum value",
    )
    args = ap.parse_args()

    # Wire up your existing parser and ingestor
    parser = OrbitsParser()
    ingestor = DBIngestor(SessionLocal, args.event_id, args.class_id, args.session_type)

    # Start the TCP reader (blocking)
    OrbitsTCPReader(
        host=args.host,
        port=args.port,
        parser=parser,
        ingestor=ingestor,
        connect_timeout=5.0,
        read_timeout=5.0,
        max_backoff=10.0,
    ).run()