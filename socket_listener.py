# socket_listener.py (fixed)
from __future__ import annotations

import argparse
import logging
import csv
import time
import socket
from datetime import datetime, timezone, date
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func

from db import SessionLocal, init_db
from models import (
    Base, Driver, Event, RaceClass, Session as RaceSession, Entry, Lap, Result,
    BasisEnum, SessionTypeEnum, Penalty
)

# ==========================
# Tunables (match logOrbits)
# ==========================
MIN_VALID_LAP_MS = 30_000       # ignore < 30.000s (spikes / pits / noise)
CROSS_WINDOW_MS = 1100          # window to collect same-lap $G lines after leader
SAME_LAP_GRACE_WINDOWS = 1      # grace windows for unseen-but-same-lap drivers

CHECKERED_STRINGS = {"checkered", "chequered", "finish", "finished", "chequer", "check"}

# Exposed handles if you embed in threads
_launched_reader: Optional[object] = None
_launched_thread: Optional[object] = None

# ---------------------- Helpers ----------------------

def parse_csv_row(line: str) -> Tuple[Optional[str], List[str]]:
    line = (line or "").strip()
    if not line or not line.startswith("$"):
        return None, []
    reader = csv.reader([line], delimiter=",", quotechar='"', skipinitialspace=False)
    row = next(reader)
    return (row[0], row[1:]) if row else (None, [])

def time_to_ms(s: str) -> int:
    """00:mm:ss.mmm or hh:mm:ss.mmm → ms. Returns a HUGE sentinel for empties."""
    s = (s or "").strip()
    if not s or s in ("00:00:00", "00:00:00.000"):
        return 10**12
    parts = s.split(":")
    if len(parts) == 3:
        h, m, sec = parts
    else:
        h, m, sec = "0", parts[0], parts[1]
    if "." in sec:
        sec_i, ms = sec.split(".", 1)
    else:
        sec_i, ms = sec, "0"
    try:
        return int(h) * 3_600_000 + int(m) * 60_000 + int(sec_i) * 1000 + int(ms.ljust(3, "0")[:3])
    except ValueError:
        return 10**12

def parseTimeSTR(s: str) -> Optional[float]:
    """Legacy helper kept for compatibility with existing code that uses seconds."""
    ms = time_to_ms((s or "").strip().strip('"'))
    if ms >= 10**12:
        return None
    return ms / 1000.0

def to_ms(seconds: Optional[float]) -> Optional[int]:
    return int(round(seconds * 1000)) if seconds is not None else None

def lock_session_type(name: str) -> str:
    n = (name or "").lower()
    if "qual" in n:
        return "Qualifying"
    if "heat" in n:
        return "Heat"
    if "prefinal" in n or "pre-final" in n or "pre final" in n:
        return "Prefinal"
    if "final" in n or "main" in n or "main event" in n:
        return "Final"
    if "practice" in n or "happy hour" in n or "warm" in n:
        return "Practice"
    return ""

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
    event_name: str = ""
    track_name: str = ""
    track_length: Optional[float] = None
    flag: str = ""

    # live timing
    best_lap_str: Dict[str, str] = field(default_factory=dict)    # "mm:ss.mmm"
    last_lap_str: Dict[str, str] = field(default_factory=dict)    # "mm:ss.mmm"
    lap_no: Dict[str, int] = field(default_factory=dict)          # per kart
    order_pos: Dict[str, int] = field(default_factory=dict)       # last seen $G pos
    status_by_num: Dict[str, str] = field(default_factory=dict)   # from $H/$SR/$SP status

    # crossing bookkeeping (from $G session elapsed)
    g_last_cross_ms: Dict[str, int] = field(default_factory=dict)
    g_last_cross_lap: Dict[str, int] = field(default_factory=dict)

    # race commit model
    display_order: List[str] = field(default_factory=list)
    leader_lap: int = 0
    window_active: bool = False
    window_lap: int = 0
    window_deadline_monotonic: float = 0.0
    win_pos: Dict[str, int] = field(default_factory=dict)
    win_cross_ms: Dict[str, int] = field(default_factory=dict)
    win_seen: Dict[str, bool] = field(default_factory=dict)
    missed_windows_same_lap: Dict[str, int] = field(default_factory=dict)

    drivers: Dict[str, DriverState] = field(default_factory=dict)

    def reset_for_new_session(self):
        self.best_lap_str.clear()
        self.last_lap_str.clear()
        self.lap_no.clear()
        self.order_pos.clear()
        self.status_by_num.clear()
        self.g_last_cross_ms.clear()
        self.g_last_cross_lap.clear()
        self.display_order.clear()
        self.leader_lap = 0
        self.window_active = False
        self.window_lap = 0
        self.window_deadline_monotonic = 0.0
        self.win_pos.clear()
        self.win_cross_ms.clear()
        self.win_seen.clear()
        self.missed_windows_same_lap.clear()

# ---------------------- Parser ----------------------

class OrbitsParser:
    def __init__(self):
        self.s = TimingState()

    def _open_window(self, lap: int):
        self.s.window_active = True
        self.s.window_lap = lap
        self.s.window_deadline_monotonic = time.monotonic() + (CROSS_WINDOW_MS / 1000.0)
        self.s.win_pos.clear()
        self.s.win_cross_ms.clear()
        self.s.win_seen.clear()

    def _close_window(self):
        self.s.window_active = False
        self.s.window_lap = 0
        self.s.window_deadline_monotonic = 0.0
        self.s.win_pos.clear()
        self.s.win_cross_ms.clear()
        self.s.win_seen.clear()

    def _window_collect(self, num: str, pos: Optional[int], cross_ms: Optional[int], lap: int):
        if not self.s.window_active or lap != self.s.window_lap:
            return
        if pos is not None:
            self.s.win_pos[num] = pos
        if cross_ms is not None:
            self.s.win_cross_ms[num] = cross_ms
        self.s.win_seen[num] = True

    def _maybe_commit_window(self):
        if not self.s.window_active:
            return
        if time.monotonic() < self.s.window_deadline_monotonic:
            return

        target_lap = self.s.window_lap

        # ensure everyone exists in display_order
        for num in self.s.drivers.keys():
            if num not in self.s.display_order:
                self.s.display_order.append(num)

        def key_for(num: str) -> Tuple[int, int, int, int, str]:
            laps = self.s.lap_no.get(num, 0)
            k1 = -laps  # more laps first

            # rank same-lap by window pos, unseen get grace then sent behind seen
            if laps >= target_lap:
                if self.s.win_seen.get(num, False):
                    posk = self.s.win_pos.get(num, 99999)
                    grace_pen = 0
                else:
                    missed = self.s.missed_windows_same_lap.get(num, 0)
                    grace_pen = 0 if missed < SAME_LAP_GRACE_WINDOWS else 1
                    posk = 99998 + grace_pen
            else:
                posk = 99999
                grace_pen = 0

            cross = self.s.win_cross_ms.get(num, self.s.g_last_cross_ms.get(num, 10**12))
            try:
                idx = self.s.display_order.index(num)
            except ValueError:
                idx = 99999
            return (k1, posk, cross, idx, num)

        nums = list(self.s.display_order)
        nums.sort(key=key_for)

        # maintain missed-window counters for same-lap unseen
        for num in nums:
            laps = self.s.lap_no.get(num, 0)
            if laps >= target_lap:
                if not self.s.win_seen.get(num, False):
                    self.s.missed_windows_same_lap[num] = self.s.missed_windows_same_lap.get(num, 0) + 1
                else:
                    self.s.missed_windows_same_lap[num] = 0

        self.s.display_order = nums
        self.s.leader_lap = max(self.s.leader_lap, target_lap)
        self._close_window()

    def parseLine(self, line: str):
        tag, f = parse_csv_row(line)
        if not tag:
            return

        def get(i: int, default: str = "") -> str:
            return f[i] if i < len(f) else default

        if tag == "$B":
            # $B, <ignored>, "Session Name"
            new_name = get(1, "").strip() or get(2, "").strip()
            if new_name:
                if new_name != self.s.session_name:
                    # session changed → reset race state
                    self.s.reset_for_new_session()
                self.s.session_name = new_name.strip('"')
                self.s.session_type = lock_session_type(self.s.session_name)

        elif tag == "$C":
            self.s.class_name = get(1, "").strip().strip('"')

        elif tag == "$E":
            key = get(0, "").strip().strip('"').upper()
            val = get(1, "").strip().strip('"')
            if key == "TRACKNAME":
                self.s.track_name = val
            elif key == "TRACKLENGTH":
                try:
                    self.s.track_length = float(val)
                except Exception:
                    pass
            elif key in {"MEETING", "EVENT", "EVENTNAME", "TITLE"}:
                self.s.event_name = val

        elif tag == "$A":
            # $A,"<num>","<num>",<transponder>,"First","Last","Chassis",1
            num = get(0, "").strip().strip('"') or get(1, "").strip().strip('"')
            if not num:
                return
            d = self.s.drivers.get(num, DriverState(num))
            d.transponder = get(2, d.transponder).strip().strip('"')
            d.first = get(3, d.first).strip().strip('"')
            d.last = get(4, d.last).strip().strip('"')
            d.chassis = get(5, d.chassis).strip().strip('"')
            act = get(6, "")
            if act:
                try:
                    d.active = int(act) == 1
                except Exception:
                    pass
            self.s.drivers[num] = d
            if num not in self.s.display_order:
                self.s.display_order.append(num)

        elif tag == "$COMP":
            # $COMP,"<num>","<num>",<transponder>,"First","Last","Chassis","Team"
            num = get(0, "").strip().strip('"') or get(1, "").strip().strip('"')
            if not num:
                return
            d = self.s.drivers.get(num, DriverState(num))
            d.first = get(3, d.first).strip().strip('"')
            d.last = get(4, d.last).strip().strip('"')
            d.chassis = get(5, d.chassis).strip().strip('"')
            d.team = get(6, d.team).strip().strip('"')
            self.s.drivers[num] = d
            if num not in self.s.display_order:
                self.s.display_order.append(num)

        elif tag == "$F":
            # $F,9999,"00:01:16","16:45:26","00:06:43","Green "
            self.s.flag = get(4, get(5, "")).strip().strip('"')
            # We don’t flip status here; DB ingest will set provisional on checkered.

        elif tag == "$G":
            # $G, <pos>, "<num>", <lap_no>, "<session_elapsed>"
            pos_str = get(0, "").strip()
            raw_num = get(1, "").strip()
            num = raw_num.strip('"') if raw_num else raw_num
            laps_str = get(2, "").strip()
            sess_elapsed = get(3, "").strip().strip('"')

            if num:
                if pos_str.isdigit():
                    self.s.order_pos[num] = int(pos_str)
                if laps_str.isdigit():
                    cur_lap = int(laps_str)
                    self.s.lap_no[num] = max(self.s.lap_no.get(num, 0), cur_lap)
                else:
                    cur_lap = None

                if sess_elapsed and cur_lap is not None:
                    cur_ms = time_to_ms(sess_elapsed)
                    prev_lap = self.s.g_last_cross_lap.get(num)
                    prev_ms = self.s.g_last_cross_ms.get(num)
                    if prev_lap is not None and prev_ms is not None and cur_lap > prev_lap:
                        delta = cur_ms - prev_ms
                        if MIN_VALID_LAP_MS <= delta < 15 * 60 * 1000:
                            # derive LAST + update BEST if improved
                            mm = delta // 60000
                            ss = (delta // 1000) % 60
                            ms = delta % 1000
                            lap_str = f"{mm:02d}:{ss:02d}.{ms:03d}"
                            self.s.last_lap_str[num] = lap_str
                            prev_best = self.s.best_lap_str.get(num)
                            if not prev_best or delta < time_to_ms(prev_best):
                                self.s.best_lap_str[num] = lap_str
                    # update crossing refs
                    self.s.g_last_cross_lap[num] = cur_lap
                    self.s.g_last_cross_ms[num] = cur_ms

                # Open window when leader starts a new lap
                if pos_str == "1" and cur_lap is not None:
                    if (not self.s.window_active) and (cur_lap > self.s.leader_lap):
                        self._open_window(cur_lap)

                # Collect to current window
                if cur_lap is not None:
                    cross_ms = self.s.g_last_cross_ms.get(num, None)
                    pos_val = int(pos_str) if pos_str.isdigit() else None
                    self._window_collect(num, pos_val, cross_ms, cur_lap)

        elif tag in ("$H", "$SR", "$SP"):
            # Normalized: pos, num, laps, last_time, status
            pos = get(0, "").strip()
            num = get(1, "").strip().strip('"')
            laps_str = get(2, "").strip()
            last_time = get(3, "").strip().strip('"')
            status = get(4, "0").strip().strip('"')
            if laps_str.isdigit():
                self.s.lap_no[num] = max(self.s.lap_no.get(num, 0), int(laps_str))
            if num and last_time:
                ms = time_to_ms(last_time)
                if ms >= MIN_VALID_LAP_MS:
                    self.s.last_lap_str[num] = last_time
                    prev_best = self.s.best_lap_str.get(num)
                    if not prev_best or ms < time_to_ms(prev_best):
                        self.s.best_lap_str[num] = last_time
            if pos and pos.isdigit():
                self.s.order_pos[num] = int(pos)
            if status:
                self.s.status_by_num[num] = status

        elif tag == "$J":
            # $J,"<num>","<best>","<last>"
            num = get(0, "").strip().strip('"')
            best = get(1, "").strip().strip('"')
            if num and best:
                ms = time_to_ms(best)
                if ms >= MIN_VALID_LAP_MS:
                    prev = self.s.best_lap_str.get(num)
                    if not prev or ms < time_to_ms(prev):
                        self.s.best_lap_str[num] = best

        # try committing window after each line
        self._maybe_commit_window()

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
                target = (class_name or "").strip() or "Unknown Class"
                if target and rc.name != target:
                    existing = db.query(RaceClass).filter(
                        RaceClass.event_id == event_id, RaceClass.name == target
                    ).one_or_none()
                    if existing and existing.id != rc.id:
                        # merge sessions carefully to avoid unique collisions
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
            db.add(driver); db.flush()
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
        with self.SessionLocal() as db:
            # 1) Resolve Event, Class, Session
            ev = self.get_or_create_event(db, s)
            rc = self.get_or_create_class(db, ev.id, s.class_name)
            sess = self.get_or_create_session(db, ev.id, rc.id, s.session_name, s.session_type)

            # 2) Ensure Drivers + Entries exist
            num_to_driver_id: Dict[str, int] = {}
            for number, driver_state in s.drivers.items():
                drv = self.get_or_create_driver(db, number, driver_state)
                self.get_or_create_entry(db, drv, number, ev.id, rc.id, driver_state)
                num_to_driver_id[number] = drv.id

            # 3) Persist laps when lap_no increases & delta valid (derive from last_lap_str)
            for num, cur_no in s.lap_no.items():
                if num not in num_to_driver_id:
                    continue
                driver_id = num_to_driver_id[num]
                last_saved = self._last_saved_lap(db, sess.id, driver_id, num)
                if cur_no <= last_saved:
                    continue

                # derive lap time from g-cross delta if available
                # fallback to last_lap_str (already floor-filtered)
                if s.g_last_cross_lap.get(num) is not None and s.g_last_cross_ms.get(num) is not None:
                    # When lap increased, we should have set last_lap_str[num]; look it up:
                    lap_str = s.last_lap_str.get(num, "")
                    ms = time_to_ms(lap_str) if lap_str else 10**12
                else:
                    ms = time_to_ms(s.last_lap_str.get(num, ""))

                if ms >= MIN_VALID_LAP_MS and ms < 15 * 60 * 1000:
                    for ln in range(last_saved + 1, cur_no + 1):
                        db.add(Lap(
                            session_id=sess.id,
                            driver_id=driver_id,
                            lap_number=ln,
                            lap_time_ms=ms,
                            timestamp=datetime.now(timezone.utc),
                            is_valid=1
                        ))
                    self.saved_lap_no[num] = cur_no
                else:
                    # invalid → do not persist; keep saved_lap_no unchanged
                    pass

            # 4) Upsert results
            # Prepare best map (ms)
            best_ms_by_num: Dict[str, int] = {}
            for n, t in s.best_lap_str.items():
                ms = time_to_ms(t)
                if ms < 10**12:
                    best_ms_by_num[n] = ms
            fastest_ms = min(best_ms_by_num.values()) if best_ms_by_num else None

            def upsert(num: str, position: Optional[int]):
                driver_id = num_to_driver_id[num]
                res = self.get_or_create_result(db, sess.id, driver_id)

                # last/best
                last_ms = time_to_ms(s.last_lap_str.get(num, "")); last_ms = None if last_ms >= 10**12 else last_ms
                best_ms = best_ms_by_num.get(num)

                res.position = position
                res.last_lap_ms = last_ms
                if best_ms is not None and (res.best_lap_ms is None or best_ms < res.best_lap_ms):
                    res.best_lap_ms = best_ms

                # status mapping (0=OK, 1=DNF, 2=DNS, 3=DQ)
                st = s.status_by_num.get(num, "0")
                if st == "1":
                    res.status_code = "DNF"
                elif st == "2":
                    res.status_code = "DNS"
                elif st == "3":
                    res.status_code = "DQ"
                else:
                    res.status_code = None

                if sess.session_type in ("Practice", "Qualifying"):
                    if fastest_ms is not None and res.best_lap_ms is not None:
                        res.gap_to_p1_ms = max(0, res.best_lap_ms - fastest_ms)
                    else:
                        res.gap_to_p1_ms = None
                else:
                    # Race gap requires full timing deltas; we keep None here.
                    res.gap_to_p1_ms = None

            if sess.session_type in ("Practice", "Qualifying"):
                # rank by best (asc), tiebreak by last seen $G position then number
                nums = list(num_to_driver_id.keys())
                SENTINEL = 10**12
                def key_fn(n: str):
                    b = best_ms_by_num.get(n, SENTINEL)
                    p = s.order_pos.get(n, 99999)
                    return (b, p, n)
                ranked = sorted(nums, key=key_fn)
                for pos, n in enumerate(ranked, start=1):
                    upsert(n, pos)
            else:
                # Race: if no committed order yet, approximate like logOrbits fallback
                if not s.display_order:
                    approx = list(num_to_driver_id.keys())
                    def approx_key(n: str) -> Tuple[int, int, int, str]:
                        laps = s.lap_no.get(n, 0)
                        pos = s.order_pos.get(n, 99999)
                        cross = s.g_last_cross_ms.get(n, 10**12)
                        return (-laps, pos, cross, n)
                    approx.sort(key=approx_key)
                    s.display_order = approx

                for pos, n in enumerate(s.display_order, start=1):
                    upsert(n, pos)

            db.commit()

            # 5) Optionally flip to provisional on checker
            flag = (parsed.s.flag or "").strip().lower()
            if flag in CHECKERED_STRINGS:
                if sess and sess.status != "provisional":
                    sess.status = "provisional"
                    if not sess.ended_at:
                        sess.ended_at = datetime.now(timezone.utc)
                    db.commit()

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

            except (socket.timeout, ConnectionError, OSError) as e:
                logging.getLogger(__name__).debug("Socket/connect/read error: %s", e)
                time.sleep(backoff)
                backoff = min(self.max_backoff, backoff * 2)
            except Exception:
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
    ingestor = DBIngestor(SessionLocal)  # resolved from packets

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
