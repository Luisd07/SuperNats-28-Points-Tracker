import argparse
import csv
from datetime import datetime, timezone, date
import socket
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func  # for seeding last saved lap_no

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

def lock_session_type(name: str) -> str:
    n = (name or "").lower()
    if "qual" in n:
        return "Qualifying"
    if "heat" in n:
        return "Heat"
    elif "prefinal" in n or "pre-final" in n or "pre final" in n:
        return "Prefinal"  # match enum spelling
    elif "final" in n:
        return "Final"
    elif "practice" in n or "happy hour" in n:
        return "Practice"
    return ""

# --- State containers ------------------------------------------------------

@dataclass
class DriverState:
    number: str
    first: str = ""
    last: str = ""
    team: str = ""
    chassis: str = ""
    transponder: str = ""
    active: bool = True

@dataclass
class TimingState:
    session_name: str = ""
    session_type: str = ""
    class_name: str = ""
    event_name: str = ""            # <- captured from $E when available
    track_name: str = ""
    track_length: Optional[float] = None
    flag: str = ""
    last_lap: Dict[str, Optional[float]] = field(default_factory=dict)  # seconds
    best_lap: Dict[str, Optional[float]] = field(default_factory=dict)  # seconds
    lap_no: Dict[str, int] = field(default_factory=dict)  # per-driver lap number
    order: List[str] = field(default_factory=list)  
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
            self.s.session_type = lock_session_type(self.s.session_name)

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
            elif key in {"MEETING", "EVENT", "EVENTNAME", "TITLE"}:
                self.s.event_name = val  # capture event/meeting name if present

        elif tag == "$A":
            number = f[1].strip('"')
            if not number:
                return
            d = self.s.drivers.get(number, DriverState(number))
            d.transponder = f[2].strip('"') if len(f) > 2 else d.transponder
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

        elif tag == "$G" and len(f) >= 5:
            # $G,<position>,"<number>",<lap_no>,"<last_lap>"
            number = f[2].strip('"')
            try:
                pos = int(f[1])
            except:
                return
            # capture lap_no
            try:
                lap_no = int(f[3])
                self.s.lap_no[number] = max(lap_no, self.s.lap_no.get(number, 0))
            except:
                pass

            lap = parseTimeSTR(f[4])
            if lap is not None:
                self.s.last_lap[number] = lap

            # maintain order
            self.s.order = [c for c in self.s.order if c != number]
            while len(self.s.order) < pos - 1:
                self.s.order.append("")
            self.s.order.insert(pos - 1, number)
            while self.s.order and self.s.order[-1] == "":
                self.s.order.pop()

        elif tag == "$H" and len(f) >= 5:
            # $H,<pos>,"<number>",?, "<last>"
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
            # $SR,<pos>,"<number>",<lap_no>,"<best>",0
            number = f[2].strip('"')
            try:
                lap_no = int(f[3])
                self.s.lap_no[number] = max(lap_no, self.s.lap_no.get(number, 0))
            except:
                pass
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
    def __init__(self, SessionLocal):
        self.SessionLocal = SessionLocal
        self.lap_counter: Dict[str, int] = {}     # not used for lap numbering anymore
        self.saved_lap_no: Dict[str, int] = {}    # number -> last persisted lap_no

    # ----- Event/Class/Session resolution from packets -----

    def _event_name_or_default(self, s: TimingState) -> str:
        if s.event_name:
            return s.event_name
        base = s.track_name or "Auto Event"
        return f"{base} {date.today().isoformat()}"

    def get_or_create_event(self, db: Session, s: TimingState) -> Event:
        name = self._event_name_or_default(s)
        ev = db.query(Event).filter(Event.name == name).one_or_none()
        if not ev:
            ev = Event(
                name=name,
                start_date=date.today(),
                end_date=date.today(),
                location=s.track_name or None,
            )
            db.add(ev); db.flush()
        return ev

    def get_or_create_class(self, db: Session, event_id: int, class_name: str) -> RaceClass:
        cname = class_name or "Unknown Class"
        rc = db.query(RaceClass).filter(
            RaceClass.event_id == event_id,
            RaceClass.name == cname
        ).one_or_none()
        if not rc:
            rc = RaceClass(event_id=event_id, name=cname)
            db.add(rc); db.flush()
        return rc

    def get_or_create_session(self, db: Session, event_id: int, class_id: int,
                              session_name: str, session_type: str) -> RaceSession:
        name = session_name or session_type or "Session"
        sess = db.query(RaceSession).filter(
            RaceSession.event_id == event_id,
            RaceSession.class_id == class_id,
            RaceSession.session_name == name,
        ).one_or_none()
        if not sess:
            sess = RaceSession(
                event_id=event_id,
                class_id=class_id,
                session_name=name,
                session_type=session_type or "Practice",
                status="live",
            )
            db.add(sess); db.flush()
        else:
            # Sync session type if we later infer it
            if session_type and sess.session_type != session_type:
                sess.session_type = session_type
                db.flush()
        return sess

    # ----- Existing helpers (lightly adapted) -----

    def _last_saved_lap(self, db: Session, session_id: int, driver_id: int, number: str) -> int:
        """Seed/return the last saved lap_no for this driver in this session."""
        if number in self.saved_lap_no:
            return self.saved_lap_no[number]
        max_no = db.query(func.max(Lap.lap_number)).filter(
            Lap.session_id == session_id, Lap.driver_id == driver_id
        ).scalar() or 0
        self.saved_lap_no[number] = max_no
        return max_no

    def get_or_create_driver(self, db: Session, number: str, driver_state: DriverState) -> Driver:
        driver = (
            db.query(Driver)
            .filter(
                getattr(Driver, "first_name") == driver_state.first,
                getattr(Driver, "last_name") == driver_state.last,
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
            db.add(driver)
            db.flush()
        else:
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
                db.flush()
        return driver

    def get_or_create_entry(self, db: Session, drv: Driver, number: str, event_id: int, class_id: int) -> Entry:
        ent = (
            db.query(Entry)
            .filter(
                Entry.event_id == event_id,
                Entry.class_id == class_id,
                Entry.number == number,
            )
            .first()
        )
        if not ent:
            ent = Entry(
                event_id=event_id,
                class_id=class_id,
                driver_id=drv.id,
                number=number,
                transponder=None,
            )
            db.add(ent)
            db.flush()
        elif ent.driver_id != drv.id:
            ent.driver_id = drv.id
            db.flush()
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
            db.flush()
        return res

    def apply(self, parsed: OrbitsParser):
        s = parsed.s
        with self.SessionLocal() as db_session:
            # Resolve Event, Class, Session from packets
            ev = self.get_or_create_event(db_session, s)
            rc = self.get_or_create_class(db_session, ev.id, s.class_name)
            sess = self.get_or_create_session(db_session, ev.id, rc.id, s.session_name, s.session_type)

            # Drivers + Entries map
            num_to_driver_id: Dict[str, int] = {}
            for number, driver_state in s.drivers.items():
                drv = self.get_or_create_driver(db_session, number, driver_state)
                self.get_or_create_entry(db_session, drv, number, ev.id, rc.id)
                num_to_driver_id[number] = drv.id

            # Leader best lap
            ordered_nums = [c for c in s.order if c and c in num_to_driver_id]
            leader_best_ms: Optional[int] = None
            for c in ordered_nums:
                b_sec = s.best_lap.get(c)
                if b_sec is not None:
                    leader_best_ms = to_ms(b_sec)
                    break

            # Upsert results (provisional)
            for idx, number in enumerate(ordered_nums, start=1):
                driver_id = num_to_driver_id[number]
                result = self.get_or_create_result(db_session, sess.id, driver_id)

                last_sec = s.last_lap.get(number)
                best_sec = s.best_lap.get(number)

                last_ms = to_ms(last_sec)
                best_ms = to_ms(best_sec)

                result.position = idx
                result.last_lap_ms = last_ms

                if best_ms is not None and (result.best_lap_ms is None or best_ms < result.best_lap_ms):
                    result.best_lap_ms = best_ms

                if leader_best_ms is not None and result.best_lap_ms is not None:
                    gap = result.best_lap_ms - leader_best_ms
                    result.gap_to_p1_ms = gap if gap >= 0 else 0
                else:
                    result.gap_to_p1_ms = None

            # Persist laps only when feed lap_no increases and we have a last-lap time
            for number, feed_lap_no in s.lap_no.items():
                if number not in num_to_driver_id:
                    continue
                driver_id = num_to_driver_id[number]
                last_saved = self._last_saved_lap(db_session, sess.id, driver_id, number)

                if feed_lap_no <= last_saved:
                    continue

                last_sec = s.last_lap.get(number)
                last_ms = to_ms(last_sec) if last_sec is not None else None
                if last_ms is None:
                    # wait until we receive the last-lap time
                    continue

                # If the feed jumped more than one, backfill sequentially with same time
                for ln in range(last_saved + 1, feed_lap_no + 1):
                    db_session.add(
                        Lap(
                            session_id=sess.id,
                            driver_id=driver_id,
                            lap_number=ln,
                            lap_time_ms=last_ms,
                            timestamp=datetime.now(timezone.utc),
                            is_valid=1,
                        )
                    )
                self.saved_lap_no[number] = feed_lap_no

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
                sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
                sock.settimeout(self.read_timeout)
                f = sock.makefile("r", encoding="utf-8", errors="ignore", newline="\n")
                backoff = 1.0

                for line in f:
                    if self._stop:
                        break
                    line = line.strip("\r\n")
                    if not line:
                        continue

                    self.parser.parseLine(line)
                    self.ingestor.apply(self.parser)

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
    init_db()

    ap = argparse.ArgumentParser(description="Orbits TCP -> Parser -> DB ingestor")
    ap.add_argument("--host", default="127.0.0.1", help="Orbits TCP host")
    ap.add_argument("--port", type=int, default=50000, help="Orbits TCP port")
    args = ap.parse_args()

    parser = OrbitsParser()
    ingestor = DBIngestor(SessionLocal)  # no event/class/session args

    OrbitsTCPReader(
        host=args.host,
        port=args.port,
        parser=parser,
        ingestor=ingestor,
        connect_timeout=5.0,
        read_timeout=5.0,
        max_backoff=10.0,
    ).run()
