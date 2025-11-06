# official.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from models import (
    Session as RaceSession,
    Result, Penalty, Lap, Driver,
    BasisEnum, SessionTypeEnum, Point, PointScale, PointAward
)

# ---------- Lightweight result view used for preview ----------
@dataclass
class ResultView:
    session_id: int
    driver_id: int
    position: Optional[int]
    best_lap_ms: Optional[int]
    last_lap_ms: Optional[int]
    total_time_ms: Optional[int]
    gap_to_p1_ms: Optional[int]
    status_code: Optional[str]

# ---------- helpers ----------
def _latest_provisional_results(db: Session, session_id: int) -> List[Result]:
    ver_row = (
        db.query(Result.version)
          .filter(Result.session_id == session_id, Result.basis == "provisional")
          .order_by(Result.version.desc())
          .first()
    )
    if not ver_row:
        return []
    v = ver_row[0]
    return (
        db.query(Result)
          .filter(Result.session_id == session_id,
                  Result.basis == "provisional",
                  Result.version == v)
          .all()
    )

def _copy_to_view(r: Result) -> ResultView:
    return ResultView(
        session_id=r.session_id,
        driver_id=r.driver_id,
        position=r.position,
        best_lap_ms=r.best_lap_ms,
        last_lap_ms=r.last_lap_ms,
        total_time_ms=r.total_time_ms,
        gap_to_p1_ms=r.gap_to_p1_ms,
        status_code=r.status_code,
    )

def _recompute_best_last_after_lap_invalid(db: Session, sess_id: int, driver_id: int, lap_no: int) -> Tuple[Optional[int], Optional[int]]:
    """
    Mark the given lap invalid and recompute best/last for preview.
    We do NOT persist the lap invalidation here (preview). We compute as-if.
    """
    # Pull laps for this driver/session and treat lap_no as invalid
    laps = (
        db.query(Lap)
          .filter(Lap.session_id == sess_id, Lap.driver_id == driver_id)
          .order_by(Lap.lap_number.asc())
          .all()
    )
    if not laps:
        return (None, None)

    best = None
    last = None
    for lp in laps:
        valid = (lp.lap_number != lap_no) and bool(lp.is_valid)
        if not valid:
            continue
        # last = highest numbered valid lap we saw
        last = lp.lap_time_ms if lp.lap_time_ms is not None else last
        # best = min valid lap
        if lp.lap_time_ms is not None:
            best = lp.lap_time_ms if best is None else min(best, lp.lap_time_ms)

    return (best, last)

def _apply_penalties_preview(db: Session, sess: RaceSession, views: Dict[int, ResultView]) -> None:
    """
    Mutates 'views' in place according to penalties.
    """
    pens = (
        db.query(Penalty)
          .filter(Penalty.session_id == sess.id)
          .order_by(Penalty.created_at.asc())
          .all()
    )
    # Staging for position drops per driver
    pos_drops: Dict[int, int] = {}
    dq_drivers: set[int] = set()

    for p in pens:
        rv = views.get(p.driver_id)
        if not rv:
            continue

        if p.type == "DQ":
            dq_drivers.add(p.driver_id)
            rv.status_code = "DQ"

        elif p.type == "POSITION":
            drop = p.value_positions or 0
            pos_drops[p.driver_id] = pos_drops.get(p.driver_id, 0) + max(0, drop)

        elif p.type == "TIME":
            # Add to total time; if total_time_ms is None, still record the change
            add_ms = p.value_ms or 0
            rv.total_time_ms = (rv.total_time_ms or 0) + add_ms

        elif p.type == "LAP_INVALID" and p.lap_no:
            best, last = _recompute_best_last_after_lap_invalid(db, sess.id, p.driver_id, p.lap_no)
            rv.best_lap_ms = best
            rv.last_lap_ms = last

    # Apply DQ first (remove from normal ranking)
    for did in dq_drivers:
        rv = views.get(did)
        if rv:
            rv.position = None  # will be placed at tail as DQ with no numeric pos

    # Apply position drops only for race-type sessions
    if sess.session_type in ("Heat", "Prefinal", "Final"):
        # Build a list of race drivers w/ numeric positions, sorted by original finishing order
        race_list = [rv for rv in views.values() if rv.position is not None]
        race_list.sort(key=lambda r: (r.position if r.position is not None else 10**9,
                                      r.best_lap_ms if r.best_lap_ms is not None else 10**9))
        
        # Apply position penalties by moving drivers down in the sorted list
        # Process penalties in order of original position to avoid conflicts
        for driver_id, drop_count in sorted(pos_drops.items(), key=lambda x: next((rv.position for rv in race_list if rv.driver_id == x[0]), 10**9) or 10**9):
            if drop_count <= 0:
                continue
            # Find driver's current index in race_list
            driver_idx = next((i for i, rv in enumerate(race_list) if rv.driver_id == driver_id), None)
            if driver_idx is not None:
                # Remove driver from current position
                driver_rv = race_list.pop(driver_idx)
                # Insert at new position (bounded by list length)
                new_idx = min(driver_idx + drop_count, len(race_list))
                race_list.insert(new_idx, driver_rv)
        
        # Reassign contiguous positions starting at 1 after all penalties applied
        for i, rv in enumerate(race_list, start=1):
            rv.position = i

        # DQs to the tail, keep status_code
        dq_list = [rv for rv in views.values() if rv.driver_id in dq_drivers]
        # They keep None position, UI shows blank; that’s typical

    else:
        # Practice/Qualifying: rank by best lap
        # NOTE: Time penalties could optionally shift ranking (if event wants), but
        # SKUSA qual ranking is by best lap, so we leave it. If you want time penalties
        # to affect qual rank, switch key to (best_lap_ms + added_ms).
        qual_list = [rv for rv in views.values() if rv.status_code != "DQ"]
        qual_list.sort(key=lambda r: (r.best_lap_ms if r.best_lap_ms is not None else 10**9))
        for i, rv in enumerate(qual_list, start=1):
            rv.position = i

def _build_views_from_provisional(rows: List[Result]) -> Dict[int, ResultView]:
    views: Dict[int, ResultView] = {}
    for r in rows:
        views[r.driver_id] = _copy_to_view(r)
    return views

# ---------- Public API ----------
def compute_official_order(db: Session, session_id: int) -> List[ResultView]:
    """
    Returns a transient, penalty-applied preview list for UI.
    Does NOT persist anything.
    """
    sess = db.get(RaceSession, session_id)
    if not sess:
        return []

    prov_rows = _latest_provisional_results(db, session_id)
    if not prov_rows:
        return []

    views = _build_views_from_provisional(prov_rows)
    _apply_penalties_preview(db, sess, views)

    # Final order for display:
    if sess.session_type in ("Heat", "Prefinal", "Final"):
        # race order: numeric positions first, then non-numeric (DQ) by name
        numeric = [v for v in views.values() if v.position is not None]
        tail = [v for v in views.values() if v.position is None]  # DQs etc.
        numeric.sort(key=lambda r: r.position if r.position is not None else 10**9)
        tail.sort(key=lambda r: (r.status_code or "", r.driver_id))
        return numeric + tail
    else:
        # qual/practice already assigned contiguous positions above
        ordered = list(views.values())
        ordered.sort(key=lambda r: (r.position if r.position is not None else 10**9))
        return ordered

def write_official_and_award_points(db: Session, session_id: int, scheme_name: str = "SKUSA_SN28") -> None:
    """
    Persists an official snapshot (basis='official', version++) and awards points
    (for Heat sessions). Commit happens here.
    """
    sess = db.get(RaceSession, session_id)
    if not sess:
        return

    # 1) compute official preview
    preview = compute_official_order(db, session_id)

    # 2) figure next official version
    ver_row = (
        db.query(Result.version)
          .filter(Result.session_id == session_id, Result.basis == "official")
          .order_by(Result.version.desc())
          .first()
    )
    next_ver = 1 if not ver_row else (ver_row[0] + 1)

    # 3) persist official results
    for rv in preview:
        db.add(Result(
            session_id=session_id,
            driver_id=rv.driver_id,
            basis="official",
            version=next_ver,
            position=rv.position,
            best_lap_ms=rv.best_lap_ms,
            last_lap_ms=rv.last_lap_ms,
            total_time_ms=rv.total_time_ms,
            gap_to_p1_ms=rv.gap_to_p1_ms,
            status_code=rv.status_code,
        ))

    # Ensure official results are flushed so queries in this txn can see them
    db.flush()

    # 4) session status → official
    sess.status = "official"
    sess.ended_at = sess.ended_at or func.now()

    # 5) award points if applicable
    # Qualifying always awards according to qualifying scale.
    # Any session whose name contains "Heat" (case-insensitive) OR type == "Heat" awards heat points.
    sname = (sess.session_name or "")
    is_heat_by_name = ("heat" in sname.lower())
    if sess.session_type == "Qualifying":
        _award_points_for_session(db, sess, preview, scheme_name, next_ver, award_type="Qualifying")
    elif is_heat_by_name or sess.session_type == "Heat":
        _award_points_for_session(db, sess, preview, scheme_name, next_ver, award_type="Heat")

    db.commit()

def _award_points_for_session(db: Session, sess: RaceSession, preview: List[ResultView], scheme_name: str, version: int, award_type: str) -> None:
    """
    General point awarder for Heat or Qualifying:
      - Look up a Point row by name (scheme_name), then its PointScale entries for the given award_type.
      - Assign base points by finisher position; ignore bonus here.
      - Note: Qualifying scales are stored as integers (1..N) to fit schema; when publishing, we can render as 0.01..N*0.01.
    """
    # find the point scheme
    pt = (
        db.query(Point)
          .filter(Point.name == scheme_name)
          .first()
    )
    if not pt:
        # Lazy-seed default scheme if missing (first run on a fresh machine)
        try:
            from points_config import seed_skusa_sn28  # local import to avoid cycles
            seed_skusa_sn28()
            db.flush()
        except Exception:
            pass
        pt = (
            db.query(Point)
              .filter(Point.name == scheme_name)
              .first()
        )
        if not pt:
            # Still missing: abort awarding silently
            return

    # build a map position->points for this session type
    scales = (
        db.query(PointScale)
          .filter(PointScale.point_id == pt.id,
                  PointScale.session_type == award_type)
          .all()
    )
    pos_to_pts = {sc.position: sc.points for sc in scales}

    # Use the official results version we just wrote
    ver = int(version)

    for rv in preview:
        if rv.position is None or (rv.status_code == "DQ"):
            pts = 0
        else:
            pts = pos_to_pts.get(rv.position, 0)

        db.add(PointAward(
            session_id=sess.id,
            driver_id=rv.driver_id,
            basis="official",
            version=ver,
            position=rv.position,
            base_points=pts,
            bonus_points=0,
            total_points=pts
        ))
