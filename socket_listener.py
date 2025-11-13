# socket_listener.py
import argparse
import logging
import csv
from datetime import datetime, timezone, date
import socket
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func

from db import SessionLocal, init_db
from models import (
    Base, Driver, Event, RaceClass, Session as RaceSession, Entry, Lap, Result,
    BasisEnum, SessionTypeEnum, Penalty
)

# Exposed runtime handles so external launcher/UI can attach to a running reader
# These will be set by the launcher (`cli.py`) when it starts a listener thread,
# and by `run_socket_listener` when the OrbitsTCPReader is created.
_launched_reader: Optional[object] = None
_launched_thread: Optional[object] = None

# ---------------------- Helpers ----------------------

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

def to_ms(seconds: Optional[float]) -> Optional[int]:
    return int(round(seconds * 1000)) if seconds is not None else None

def lock_session_type(name: str) -> str:
    n = (name or "").lower()
    if "qual" in n:
        return "Qualifying"
    if "heat" in n:
        return "Heat"
    elif "prefinal" in n or "pre-final" in n or "pre final" in n:
        return "Prefinal"
    elif "final" in n or "main" in n or "main event" in n:
        return "Final"
    elif "practice" in n or "happy hour" in n:
        return "Practice"
    return ""

CHECKERED_STRINGS = {"checkered", "chequered", "finish", "finished", "chequer", "check"}

# ---------------------- State ----------------------

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
    event_name: str = ""            # from $E if present
    track_name: str = ""
    track_length: Optional[float] = None
    flag: str = ""

    # live timing
    last_lap: Dict[str, Optional[float]] = field(default_factory=dict)  # seconds
    best_lap: Dict[str, Optional[float]] = field(default_factory=dict)  # seconds
    lap_no: Dict[str, int] = field(default_factory=dict)                # feed lap number per kart
    order: List[str] = field(default_factory=list)                      # running order by number

    drivers: Dict[str, DriverState] = field(default_factory=dict)

# ---------------------- Parser ----------------------

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
                self.s.event_name = val

        elif tag == "$A":
            # $A,"48","48",12807698,"James","Overbeck","Coyote",1
            number = f[1].strip('"')
            if not number:
                return
            d = self.s.drivers.get(number, DriverState(number))
            d.transponder = f[3].strip('"') if len(f) > 3 else d.transponder
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
            # $COMP,"48","48",12807698,"James","Overbeck","Coyote","TeamName"
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
            # $F,9999,"00:01:16","16:45:26","00:06:43","Green "
            if len(f) > 5:
                self.s.flag = (f[5] or "").strip().strip('"').strip()

        elif tag == "$G" and len(f) >= 5:
            # $G,<position>,"<number>",<lap_no>,"<last_lap>"
            number = f[2].strip('"')
            try:
                pos = int(f[1])
            except:
                return
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

# ---------------------- DB ingest ----------------------

class DBIngestor:
    def __init__(self, SessionLocal):
        self.SessionLocal = SessionLocal
        self.saved_lap_no: Dict[str, int] = {}    # number -> last persisted lap_no
        self.current_event_id: Optional[int] = None
        self.current_class_id: Optional[int] = None
        self._last_session_id: Optional[int] = None

    # ---- Event/Class: placeholder then rename/merge ----

    def _event_name_or_default(self, s: TimingState) -> str:
        if s.event_name:
            return s.event_name.strip()
        base = (s.track_name or "Auto Event").strip()
        return f"{base} {date.today().isoformat()}"

    def get_or_create_event(self, db: Session, s: TimingState) -> Event:
        if self.current_event_id:
            ev = db.get(Event, self.current_event_id)
            if ev:
                target = (s.event_name or "").strip()
                if target and ev.name != target:
                    existing = db.query(Event).filter(Event.name == target).one_or_none()
                    if existing and existing.id != ev.id:
                        # merge
                        db.query(RaceClass).filter_by(event_id=ev.id).update({"event_id": existing.id})
                        db.query(RaceSession).filter_by(event_id=ev.id).update({"event_id": existing.id})
                        db.query(Entry).filter_by(event_id=ev.id).update({"event_id": existing.id})
                        db.flush()
                        db.delete(ev); db.flush()
                        self.current_event_id = existing.id
                        return existing
                    else:
                        ev.name = target
                        if s.track_name: ev.location = s.track_name
                        db.flush()
                return ev

        # no cached event; try by final name if present
        name = (s.event_name or "").strip()
        if name:
            ev = db.query(Event).filter(Event.name == name).one_or_none()
            if not ev:
                ev = Event(name=name, start_date=date.today(), end_date=date.today(), location=s.track_name or None)
                db.add(ev); db.flush()
        else:
            placeholder = self._event_name_or_default(s)
            ev = db.query(Event).filter(Event.name == placeholder).one_or_none()
            if not ev:
                ev = Event(name=placeholder, start_date=date.today(), end_date=date.today(), location=s.track_name or None)
                db.add(ev); db.flush()

        self.current_event_id = ev.id
        return ev

    def get_or_create_class(self, db: Session, event_id: int, class_name: str) -> RaceClass:
        if self.current_class_id:
            rc = db.get(RaceClass, self.current_class_id)
            if rc:
                target = (class_name or "").strip()
                if target and rc.name != target:
                    existing = db.query(RaceClass).filter(
                        RaceClass.event_id == event_id, RaceClass.name == target
                    ).one_or_none()
                    if existing and existing.id != rc.id:
                        # merge, but avoid UNIQUE constraint violation on sessions
                        # For each session with class_id=rc.id, check if a session with same event_id, session_name exists for existing.id
                        sessions_to_update = db.query(RaceSession).filter_by(class_id=rc.id).all()
                        for sess in sessions_to_update:
                            duplicate = db.query(RaceSession).filter_by(
                                event_id=sess.event_id,
                                class_id=existing.id,
                                session_name=sess.session_name
                            ).first()
                            if not duplicate:
                                sess.class_id = existing.id
                            else:
                                # Optionally, handle merging or deleting duplicate sessions
                                db.delete(sess)
                        db.query(Entry).filter_by(class_id=rc.id).update({"class_id": existing.id})
                        db.flush()
                        db.delete(rc); db.flush()
                        self.current_class_id = existing.id
                        return existing
                    else:
                        rc.name = target
                        db.flush()
                return rc

        name = (class_name or "").strip() or "Unknown Class"
        rc = db.query(RaceClass).filter(
            RaceClass.event_id == event_id, RaceClass.name == name
        ).one_or_none()
        if not rc:
            rc = RaceClass(event_id=event_id, name=name)
            db.add(rc); db.flush()
        self.current_class_id = rc.id
        return rc

    # ---- Session ----

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
            if session_type and sess.session_type != session_type:
                sess.session_type = session_type
                db.flush()
        self._last_session_id = sess.id
        return sess

    # ---- Entities ----

    def _last_saved_lap(self, db: Session, session_id: int, driver_id: int, number: str) -> int:
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
            .filter(Driver.first_name == driver_state.first, Driver.last_name == driver_state.last)
            .first()
        )
        if not driver:
            driver = Driver(
                first_name=driver_state.first,
                last_name=driver_state.last,
                team=driver_state.team,
                chassis=driver_state.chassis,
                transponder=driver_state.transponder or None
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
            if driver_state.transponder and driver.transponder != driver_state.transponder:
                driver.transponder = driver_state.transponder; changed = True
            if changed:
                db.flush()
        return driver

    def get_or_create_entry(self, db: Session, drv: Driver, number: str,
                            event_id: int, class_id: int, driver_state: Optional[DriverState]) -> Entry:
        ent = db.query(Entry).filter(
            Entry.event_id == event_id,
            Entry.class_id == class_id,
            Entry.number == number,
        ).one_or_none()

        want_team = (driver_state.team if driver_state and driver_state.team else drv.team)
        want_chas = (driver_state.chassis if driver_state and driver_state.chassis else drv.chassis)
        want_trans = (driver_state.transponder if driver_state and driver_state.transponder else None)

        if not ent:
            ent = Entry(
                event_id=event_id,
                class_id=class_id,
                driver_id=drv.id,
                number=number,
                transponder=want_trans,
                team=want_team,
                chassis=want_chas,
            )
            db.add(ent); db.flush()
        else:
            changed = False
            if ent.driver_id != drv.id:
                ent.driver_id = drv.id; changed = True
            if want_team and ent.team != want_team:
                ent.team = want_team; changed = True
            if want_chas and ent.chassis != want_chas:
                ent.chassis = want_chas; changed = True
            if want_trans and ent.transponder != want_trans:
                ent.transponder = want_trans; changed = True
            if changed:
                db.flush()
        return ent

    def get_or_create_result(self, db: Session, session_id: int, driver_id: int) -> Result:
        res = db.query(Result).filter(
            Result.session_id == session_id,
            Result.driver_id == driver_id,
            Result.basis == "provisional",
            Result.version == 1,
        ).one_or_none()
        if not res:
            res = Result(
                session_id=session_id,
                driver_id=driver_id,
                basis="provisional",
                version=1,
            )
            db.add(res); db.flush()
        return res

    # ---- Apply one parser tick ----

    def apply(self, parsed: OrbitsParser):
        s = parsed.s
        with self.SessionLocal() as db_session:
            # 1) Resolve Event, Class, Session from packets (rename/merge safe)
            ev = self.get_or_create_event(db_session, s)
            rc = self.get_or_create_class(db_session, ev.id, s.class_name)
            sess = self.get_or_create_session(db_session, ev.id, rc.id, s.session_name, s.session_type)

            # 2) Ensure Drivers + Entries exist; build num->driver_id map
            num_to_driver_id: Dict[str, int] = {}
            for number, driver_state in s.drivers.items():
                drv = self.get_or_create_driver(db_session, number, driver_state)
                self.get_or_create_entry(db_session, drv, number, ev.id, rc.id, driver_state)
                num_to_driver_id[number] = drv.id

            # 3) Persist laps only when feed lap_no increases and we have a last-lap time
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
                    continue
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

            # 4) Upsert results per session type
            # Build "ordered" list and "best laps" map
            ordered_nums = [c for c in s.order if c and c in num_to_driver_id]
            best_map_ms: Dict[str, int] = {}
            for num, sec in s.best_lap.items():
                if sec is not None:
                    best_map_ms[num] = to_ms(sec)  # type: ignore

            # Global fastest best lap for gap (P/Q)
            fastest_best_ms = min(best_map_ms.values()) if best_map_ms else None

            def upsert(num: str, position: Optional[int]):
                driver_id = num_to_driver_id[num]
                result = self.get_or_create_result(db_session, sess.id, driver_id)

                last_ms = to_ms(s.last_lap.get(num))
                best_ms = best_map_ms.get(num)

                result.position = position
                result.last_lap_ms = last_ms

                if best_ms is not None and (result.best_lap_ms is None or best_ms < result.best_lap_ms):
                    result.best_lap_ms = best_ms

                if sess.session_type in ("Practice", "Qualifying"):
                    if fastest_best_ms is not None and result.best_lap_ms is not None:
                        result.gap_to_p1_ms = max(0, result.best_lap_ms - fastest_best_ms)
                    else:
                        result.gap_to_p1_ms = None
                else:
                    # race gap requires pass times/total time; leave None for now
                    result.gap_to_p1_ms = None

            if sess.session_type in ("Practice", "Qualifying"):
                # rank by best lap ascending; if none, push to bottom; tie-break by current order index
                # build candidates
                candidates = list(num_to_driver_id.keys())
                # stable index from current order for tie-break
                idx_in_order = {n: (ordered_nums.index(n) if n in ordered_nums else 10**6) for n in candidates}
                # Build a last-lap map (ms) to use as fallback when best lap is not present.
                last_map_ms: Dict[str, Optional[int]] = {}
                for n in candidates:
                    sec = s.last_lap.get(n)
                    last_map_ms[n] = to_ms(sec) if sec is not None else None

                def key_fn(n: str):
                    bm = best_map_ms.get(n)
                    if bm is not None:
                        # primary: best lap ascending
                        return (0, bm, idx_in_order[n], n)
                    lm = last_map_ms.get(n)
                    if lm is not None:
                        # secondary: last lap ascending (recent lap time) to approximate running pace
                        return (1, lm, idx_in_order[n], n)
                    # tertiary: no times -> push to bottom, tie-break by running order then number
                    return (2, 10**12, idx_in_order[n], n)

                # Debug: log inputs used for ranking to help diagnose ordering issues
                logging.getLogger(__name__).debug(
                    "Practice/Qual ranking inputs: candidates=%s best_map_ms=%s last_map_ms=%s ordered_nums=%s idx_in_order=%s",
                    candidates, best_map_ms, last_map_ms, ordered_nums, idx_in_order,
                )
                ranked = sorted(candidates, key=key_fn)
                logging.getLogger(__name__).debug("Practice/Qual ranked order: %s", ranked)
                pos = 1
                for n in ranked:
                    # assign a position only if the driver has a best lap or a last lap
                    assign_pos = pos if (best_map_ms.get(n) is not None or last_map_ms.get(n) is not None) else None
                    upsert(n, assign_pos)
                    if assign_pos is not None:
                        pos += 1
            else:
                # Heat / Prefinal / Final => position by leader on track
                # Determine candidates (all known numbers) and stable index from current order
                candidates = list(num_to_driver_id.keys())
                idx_in_order = {n: (ordered_nums.index(n) if n in ordered_nums else 10**6) for n in candidates}
                # lap numbers from feed (higher lap_no -> ahead on track)
                lap_no_map = {n: s.lap_no.get(n, 0) for n in candidates}

                # Sort by: most laps completed (desc), then running order index (asc), then driver number
                def race_key(n: str):
                    return (-lap_no_map.get(n, 0), idx_in_order.get(n, 10**6), n)

                ranked = sorted(candidates, key=race_key)
                for pos, n in enumerate(ranked, start=1):
                    upsert(n, pos)

            db_session.commit()

# ---------------------- TCP Reader ----------------------

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
        import time
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

                    # Optional: flip to provisional on checker (if you want it here)
                    flag = (self.parser.s.flag or "").strip().lower()
                    if flag in CHECKERED_STRINGS:
                        sid = getattr(self.ingestor, "_last_session_id", None)
                        if sid:
                            with SessionLocal() as db:
                                sess = db.get(RaceSession, sid)
                                if sess and sess.status != "provisional":
                                    sess.status = "provisional"
                                    if not sess.ended_at:
                                        sess.ended_at = datetime.now(timezone.utc)
                                    db.commit()

            except (socket.timeout, ConnectionError, OSError) as e:
                logging.getLogger(__name__).debug("Socket/connect/read error: %s", e)
                time.sleep(backoff)
                backoff = min(self.max_backoff, backoff * 2)
            except Exception:
                # Catch-all to prevent silent crashes — log and retry with backoff
                logging.getLogger(__name__).exception("Uncaught exception in OrbitsTCPReader; will retry")
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

# ---------------------- Main ----------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    ap = argparse.ArgumentParser(description="Orbits TCP -> Parser -> DB ingestor")
    ap.add_argument("--host", default="127.0.0.1", help="Orbits TCP host")
    ap.add_argument("--port", type=int, default=50000, help="Orbits TCP port")
    args = ap.parse_args()

    parser = OrbitsParser()
    ingestor = DBIngestor(SessionLocal)  # zero-arg IDs; resolved from packets

    logging.info("Starting Orbits TCP reader on %s:%s", args.host, args.port)
    OrbitsTCPReader(
        host=args.host,
        port=args.port,
        parser=parser,
        ingestor=ingestor,
        connect_timeout=5.0,
        read_timeout=5.0,
        max_backoff=10.0,
    ).run()


if __name__ == "__main__":
    main()
