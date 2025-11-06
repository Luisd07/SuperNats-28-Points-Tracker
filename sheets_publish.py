# sheets_publish.py
from __future__ import annotations
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import json
import os

import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import func

from sn28_config import CFG
from db import SessionLocal
from models import (
    Session as RaceSession,
    Result, Driver, Entry,
    PointAward, Point, PointScale,
)

# =========================
# Google Sheets plumbing
# =========================

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _load_creds() -> Credentials:
    if CFG.google.service_json_raw:
        info = json.loads(CFG.google.service_json_raw)
        return Credentials.from_service_account_info(info, scopes=_SCOPES)
    if CFG.google.service_json_path and os.path.exists(CFG.google.service_json_path):
        return Credentials.from_service_account_file(CFG.google.service_json_path, scopes=_SCOPES)
    raise RuntimeError("Google service account credentials not found. Set GS_SERVICE_JSON_PATH or GS_SERVICE_JSON_RAW.")

def _open_sheet():
    gc = gspread.authorize(_load_creds())
    if not CFG.google.spreadsheet_id:
        raise RuntimeError("Google spreadsheet_id not configured. Set GS_SPREADSHEET_ID.")
    return gc.open_by_key(CFG.google.spreadsheet_id)

def _safe_ws(sh, title: str, rows: int = 500, cols: int = 16):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

# =========================
# SHEETS HELPER: single fast write
# =========================
def _publish_rows(sh, tab_title: str, header: List[str], rows: List[List[Any]]) -> None:
    """
    Writes header + rows to a 'Raw_' tab in a single batch_update.
    Ensures the sheet exists and is sized to fit the data. Overwrites A1:... block only.
    """
    ws = _safe_ws(sh, tab_title, rows=max(1000, len(rows) + 10), cols=max(16, len(header) + 4))

    # Compute write range based on header length and row count
    total_rows = 1 + len(rows)
    total_cols = max(len(header), max((len(r) for r in rows), default=0))
    import gspread
    end_col_letter = gspread.utils.rowcol_to_a1(1, total_cols).split('1')[0]  # e.g. 'S' from 'S1'
    # Include sheet title in range to target the correct worksheet
    write_range = f"'{tab_title}'!A1:{end_col_letter}{total_rows}"

    values = [header] + rows

    body = {
        "valueInputOption": "RAW",
        "data": [
            {
                "range": write_range,
                "values": values,
            }
        ]
    }

    # Clear only the previous used area (to avoid old tail data showing)
    ws.clear()
    ws.spreadsheet.values_batch_update(body)

# =========================
# DB helpers
# =========================

def _latest_version(db, session_id: int, basis: str) -> Optional[int]:
    row = (
        db.query(Result.version)
          .filter(Result.session_id == session_id, Result.basis == basis)
          .order_by(Result.version.desc())
          .first()
    )
    return row[0] if row else None

def _get_session(db, session_id: int) -> RaceSession:
    s = db.get(RaceSession, session_id)
    if not s:
        raise RuntimeError(f"Session {session_id} not found")
    return s

def _ms(ms: Optional[int]) -> str:
    if ms is None:
        return ""
    s = ms / 1000.0
    if s >= 60:
        m = int(s // 60)
        return f"{m}:{(s - 60*m):06.3f}"
    return f"{s:.3f}"

# =========================
# Fetchers
# =========================

def _fetch_official_results(db, session_id: int) -> List[Dict]:
    v = _latest_version(db, session_id, "official")
    if v is None:
        return []
    sess = _get_session(db, session_id)

    # Preload numbers for (event_id, class_id)
    num_by_driver: Dict[int, str] = {}
    for e in (
        db.query(Entry)
          .filter(Entry.event_id == sess.event_id, Entry.class_id == sess.class_id)
          .all()
    ):
        num_by_driver[e.driver_id] = e.number or ""

    q = (
        db.query(Result, Driver)
          .join(Driver, Driver.id == Result.driver_id)
          .filter(Result.session_id == session_id,
                  Result.basis == "official",
                  Result.version == v)
          .order_by(Result.position.is_(None), Result.position.asc())
    )
    out: List[Dict] = []
    for r, d in q.all():
        out.append({
            "driver_id": d.id,
            "pos": r.position,
            "number": num_by_driver.get(d.id, ""),
            "first": d.first_name,
            "last": d.last_name,
            "team": d.team or "",
            "chassis": d.chassis or "",
            "best_ms": r.best_lap_ms,
            "last_ms": r.last_lap_ms,
            "status": r.status_code or "",
        })
    return out

def _fetch_heat_points(db, session_id: int) -> List[Dict]:
    v = _latest_version(db, session_id, "official")
    if v is None:
        return []
    sess = _get_session(db, session_id)

    # Preload numbers for (event_id, class_id)
    num_by_driver: Dict[int, str] = {}
    for e in (
        db.query(Entry)
          .filter(Entry.event_id == sess.event_id, Entry.class_id == sess.class_id)
          .all()
    ):
        num_by_driver[e.driver_id] = e.number or ""

    qa = (
        db.query(PointAward, Driver)
          .join(Driver, Driver.id == PointAward.driver_id)
          .filter(PointAward.session_id == session_id,
                  PointAward.basis == "official",
                  PointAward.version == v)
          .order_by(PointAward.position.is_(None), PointAward.position.asc())
    )
    out: List[Dict] = []
    for pa, d in qa.all():
        out.append({
            "driver_id": d.id,
            "pos": pa.position,
            "number": num_by_driver.get(d.id, ""),
            "first": d.first_name,
            "last": d.last_name,
            "base": pa.base_points or 0,
            "bonus": pa.bonus_points or 0,
            "total": pa.total_points or 0,
        })
    return out

# =========================
# Publishers
# =========================

def publish_official_results(session_id: int) -> None:
    if not CFG.app.publish_results:
        return
    sh = _open_sheet()
    ws = _safe_ws(sh, CFG.google.tab_results, rows=1000, cols=16)

    with SessionLocal() as db:
        sess = _get_session(db, session_id)
        data = _fetch_official_results(db, session_id)

    header = ["Class", "Session", "Pos", "#", "Driver", "Team", "Chassis", "Best Lap", "Last Lap", "Status", "Updated"]
    rows = [header]
    for row in data:
        rows.append([
            getattr(sess.race_class, "name", "") if sess else "",
            f"{sess.session_type} - {sess.session_name or ''}" if sess else "",
            row["pos"] or "",
            row["number"],
            f'{row["first"]} {row["last"]}'.strip(),
            row["team"],
            row["chassis"],
            _ms(row["best_ms"]),
            _ms(row["last_ms"]),
            row["status"],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ])

    ws.clear()
    if rows:
        # Use keyword args; ignore type checker complaint about literal string type
        ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")  # type: ignore[arg-type]

def publish_heat_points(session_id: int) -> None:
    if not CFG.app.publish_points:
        return
    sh = _open_sheet()
    ws = _safe_ws(sh, CFG.google.tab_heat_points, rows=1000, cols=12)

    with SessionLocal() as db:
        sess = _get_session(db, session_id)
        data = _fetch_heat_points(db, session_id)

    header = ["Class", "Heat", "Pos", "#", "Driver", "Base", "Bonus", "Total", "Updated"]
    rows = [header]
    for row in data:
        rows.append([
            getattr(sess.race_class, "name", "") if sess else "",
            sess.session_name or "",
            row["pos"] or "",
            row["number"],
            f'{row["first"]} {row["last"]}'.strip(),
            row["base"],
            row["bonus"],
            row["total"],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ])

    ws.clear()
    if rows:
        ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")  # type: ignore[arg-type]

def publish_prefinal_grid(class_id: int, event_id: int) -> None:
    """
    Build Prefinal grid by cumulative official Heat points for (event_id, class_id).
    Tie-breaker: best official Qualifying position (proxy for 'original qualifying time').
    """
    if not CFG.app.publish_prefinal_grid:
        return
    sh = _open_sheet()
    ws = _safe_ws(sh, CFG.google.tab_prefinal_grid, rows=1000, cols=12)

    with SessionLocal() as db:
        # 1) Heat session IDs
        heat_ids = [
            sid for (sid,) in db.query(RaceSession.id)
                                 .filter(RaceSession.event_id == event_id,
                                         RaceSession.class_id == class_id,
                                         RaceSession.session_type == "Heat")
                                 .all()
        ]
        if not heat_ids:
            ws.clear()
            ws.update(range_name="A1", values=[["No Heat sessions found for this class/event."]])
            return

        # 2) For each heat, use latest official PointAwards
        sub_latest = (
            db.query(PointAward.session_id, func.max(PointAward.version).label("v"))
              .filter(PointAward.session_id.in_(heat_ids), PointAward.basis == "official")
              .group_by(PointAward.session_id)
              .subquery()
        )

        qa = (
            db.query(PointAward, Driver)
              .join(Driver, Driver.id == PointAward.driver_id)
              .join(sub_latest, (PointAward.session_id == sub_latest.c.session_id) &
                               (PointAward.version == sub_latest.c.v))
              .all()
        )

        # 3) Aggregate points by driver
        agg: Dict[int, int] = {}
        name_by_driver: Dict[int, str] = {}
        num_by_driver: Dict[int, str] = {}
        # preload numbers for class/event
        for e in db.query(Entry).filter(Entry.event_id == event_id,
                                        Entry.class_id == class_id).all():
            num_by_driver[e.driver_id] = e.number or ""

        for pa, d in qa:
            agg[d.id] = agg.get(d.id, 0) + (pa.total_points or 0)
            name_by_driver[d.id] = f"{d.first_name} {d.last_name}".strip()

        # 4) Qualifying tiebreak: best official qualifying position
        q_ids = [
            sid for (sid,) in db.query(RaceSession.id)
                                 .filter(RaceSession.event_id == event_id,
                                         RaceSession.class_id == class_id,
                                         RaceSession.session_type == "Qualifying")
                                 .all()
        ]
        qual_best_pos: Dict[int, int] = {}
        if q_ids:
            sub_latest_q = (
                db.query(Result.session_id, func.max(Result.version).label("v"))
                  .filter(Result.session_id.in_(q_ids), Result.basis == "official")
                  .group_by(Result.session_id)
                  .subquery()
            )
            qres = (
                db.query(Result)
                  .join(sub_latest_q, (Result.session_id == sub_latest_q.c.session_id) &
                                     (Result.version == sub_latest_q.c.v))
                  .filter(Result.position.isnot(None))
                  .all()
            )
            for r in qres:
                cur = qual_best_pos.get(r.driver_id)
                if cur is None or (r.position or 10**6) < cur:
                    qual_best_pos[r.driver_id] = r.position or cur or 10**6

        # 5) Rank: lower points wins; tie -> better qual pos; then name
        def key_fn(drv_id: int):
            return (agg.get(drv_id, 10**6),
                    qual_best_pos.get(drv_id, 10**6),
                    name_by_driver.get(drv_id, ""))

        ranked = sorted(agg.keys(), key=key_fn)

        # 6) Write rows
        header = ["Class", "Grid Pos", "#", "Driver", "Total Heat Pts", "Qual Tiebreak", "Updated"]
        rows = [header]
        class_name = (db.query(RaceSession)
                        .filter(RaceSession.class_id == class_id,
                                RaceSession.event_id == event_id)
                        .first())
        class_name = class_name.race_class.name if class_name and class_name.race_class else ""

        for i, drv_id in enumerate(ranked, start=1):
            rows.append([
                class_name,
                str(i),
                num_by_driver.get(drv_id, ""),
                name_by_driver.get(drv_id, ""),
                str(agg.get(drv_id, 0)),
                str(qual_best_pos.get(drv_id, "")),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ])

    ws.clear()
    if rows:
        ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")  # type: ignore[arg-type]

# =========================
# Raw publishers for normalized Google Sheets tabs
# =========================
def publish_raw_results(session_id: int) -> None:
    """
    Publish latest OFFICIAL results for a session into Raw_Results
    with numeric ms fields for best/last.
    """
    if not CFG.app.publish_results:
        return

    sh = _open_sheet()
    with SessionLocal() as db:
        sess = _get_session(db, session_id)
        data = _fetch_official_results(db, session_id)

        class_name = getattr(sess.race_class, "name", "") if sess else ""
        header = [
            "EventID","ClassID","Class","SessionID","SessionType","SessionName",
            "Version","Basis","DriverID","Pos","Number","First","Last","Team","Chassis",
            "BestMs","LastMs","Status","UpdatedUTC"
        ]

        ver = _latest_version(db, session_id, "official") or 0
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        rows: List[List[Any]] = []
        for row in data:
            driver_id = row.get("driver_id") if "driver_id" in row else None
            rows.append([
                sess.event_id if sess else None,
                sess.class_id if sess else None,
                class_name,
                sess.id if sess else None,
                sess.session_type if sess else "",
                (sess.session_name or "") if sess else "",
                ver,
                "official",
                driver_id,
                row["pos"],
                row["number"],
                row["first"],
                row["last"],
                row["team"],
                row["chassis"],
                row["best_ms"],
                row["last_ms"],
                row["status"],
                now_utc,
            ])

    _publish_rows(sh, "Raw_Results", header, rows)

def publish_raw_heat_points(session_id: int) -> None:
    if not CFG.app.publish_points:
        return

    sh = _open_sheet()
    with SessionLocal() as db:
        sess = _get_session(db, session_id)
        data = _fetch_heat_points(db, session_id)

        class_name = getattr(sess.race_class, "name", "") if sess else ""
        ver = _latest_version(db, session_id, "official") or 0
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        header = [
            "EventID","ClassID","Class","SessionID","SessionName","Version","Basis",
            "DriverID","Pos","Number","First","Last","Base","Bonus","Total","UpdatedUTC"
        ]

        rows: List[List[Any]] = []
        for row in data:
            driver_id = row.get("driver_id") if "driver_id" in row else None
            rows.append([
                sess.event_id if sess else None,
                sess.class_id if sess else None,
                class_name,
                sess.id if sess else None,
                (sess.session_name or "") if sess else "",
                ver,
                "official",
                driver_id,
                row["pos"],
                row["number"],
                row["first"],
                row["last"],
                row["base"],
                row["bonus"],
                row["total"],
                now_utc,
            ])

    _publish_rows(sh, "Raw_HeatPoints", header, rows)

def publish_raw_points(session_id: int) -> None:
    """
    Publish latest OFFICIAL points (Heat or Qualifying) for a session into Raw_Points.
    Qualifying points are stored as integers in DB (1..N); render as fractional (0.01..N*0.01) in the sheet.
    """
    if not CFG.app.publish_points:
        return

    sh = _open_sheet()
    with SessionLocal() as db:
        sess = _get_session(db, session_id)
        data = _fetch_heat_points(db, session_id)  # uses PointAward table; works for Heat or Qualifying awards

        class_name = getattr(sess.race_class, "name", "") if sess else ""
        ver = _latest_version(db, session_id, "official") or 0
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        header = [
            "EventID","ClassID","Class","SessionID","SessionType","SessionName","Version","Basis",
            "DriverID","Pos","Number","First","Last","Base","Bonus","Total","UpdatedUTC"
        ]

        # Determine whether to render fractional points (Qualifying)
        s_type = getattr(sess, "session_type", "") if sess else ""
        s_name = (getattr(sess, "session_name", "") or "") if sess else ""
        is_heat = (s_type == "Heat") or ("heat" in s_name.lower())
        is_qual = (s_type == "Qualifying")

        rows: List[List[Any]] = []
        for row in data:
            driver_id = row.get("driver_id") if "driver_id" in row else None
            base = row["base"]
            bonus = row["bonus"]
            total = row["total"]
            # Render qualifying as fractional hundredths for Sheets display
            if is_qual:
                base = float(base) / 100.0 if base is not None else 0.0
                bonus = float(bonus) / 100.0 if bonus is not None else 0.0
                total = float(total) / 100.0 if total is not None else 0.0

            rows.append([
                sess.event_id if sess else None,
                sess.class_id if sess else None,
                class_name,
                sess.id if sess else None,
                s_type,
                (sess.session_name or "") if sess else "",
                ver,
                "official",
                driver_id,
                row["pos"],
                row["number"],
                row["first"],
                row["last"],
                base,
                bonus,
                total,
                now_utc,
            ])

    _publish_rows(sh, "Raw_Points", header, rows)

def ensure_heat_points_class_views(event_id: int) -> None:
    """
    Ensure per-class Heat points view tabs exist: HeatPoints_<ClassName>.
    These are Sheets tabs with FILTER formulas over Raw_Points, showing rows for the class
    and where SessionType == 'Heat' OR SessionName contains 'Heat'.
    """
    sh = _open_sheet()
    with SessionLocal() as db:
        # Get all classes for this event
        classes = db.query(RaceSession.class_id, RaceSession.event_id).filter(RaceSession.event_id == event_id).distinct().all()
        # Map class_id -> name
        names: Dict[int, str] = {}
        for cid, _ in classes:
            rc = db.query(RaceSession).filter(RaceSession.class_id == cid, RaceSession.event_id == event_id).first()
            if rc and rc.race_class and rc.race_class.name:
                names[cid] = rc.race_class.name

    def sanitize_title(title: str) -> str:
        bad = ":/\\?*[]"
        out = "".join(ch for ch in title if ch not in bad)
        return out[:95]

    for _, cname in names.items():
        tab = f"HeatPoints_{sanitize_title(cname)}"
        ws = _safe_ws(sh, tab, rows=1000, cols=18)
        # Header (match Raw_Points)
        header = [
            "EventID","ClassID","Class","SessionID","SessionType","SessionName","Version","Basis",
            "DriverID","Pos","Number","First","Last","Base","Bonus","Total","UpdatedUTC"
        ]
        # FILTER rows by Class (col C) and Heat by SessionType (col E) or SessionName (col F)
        # Use USER_ENTERED for formulas
        formula = (
            f"=FILTER('Raw_Points'!A2:Q, ('Raw_Points'!C:C=\"{cname}\") * ( (REGEXMATCH('Raw_Points'!F:F, \"Heat\")) + ('Raw_Points'!E:E=\"Heat\") ))"
        )
        ws.clear()
        ws.update(range_name="A1", values=[header, [formula]], value_input_option="USER_ENTERED")  # type: ignore[arg-type]

def ensure_simple_views(event_id: int) -> None:
    """
    Create/update compact dynamic view tabs to avoid per-class proliferation:
      - HeatPoints_View: Filter Raw_Points by class in B1 and Heat sessions.
      - HeatTotals_View: Filter Raw_HeatTotals by class in B1.
      - LCQ_View: Filter Raw_HeatTotals by class in B1 and Rank > cutoff in B2.
    """
    sh = _open_sheet()

    # HeatPoints_View
    ws_hp = _safe_ws(sh, "HeatPoints_View", rows=1000, cols=18)
    hp_header = [
        "EventID","ClassID","Class","SessionID","SessionType","SessionName","Version","Basis",
        "DriverID","Pos","Number","First","Last","Base","Bonus","Total","UpdatedUTC"
    ]
    hp_top = [["Class Filter", ""], hp_header]
    hp_formula = (
        "=FILTER('Raw_Points'!A2:Q, ('Raw_Points'!C:C=$B$1) * ((REGEXMATCH('Raw_Points'!F:F, \"Heat\")) + ('Raw_Points'!E:E=\"Heat\")))"
    )
    ws_hp.clear()
    ws_hp.update(range_name="A1", values=hp_top + [[hp_formula]], value_input_option="USER_ENTERED")  # type: ignore[arg-type]

    # HeatTotals_View
    ws_ht = _safe_ws(sh, "HeatTotals_View", rows=1000, cols=12)
    ht_header = ["EventID","ClassID","Class","DriverID","Number","Driver","TotalHeatPts","QualTiebreak","Rank","UpdatedUTC"]
    ht_top = [["Class Filter", ""], ht_header]
    ht_formula = "=FILTER('Raw_HeatTotals'!A2:J, 'Raw_HeatTotals'!C:C=$B$1)"
    ws_ht.clear()
    ws_ht.update(range_name="A1", values=ht_top + [[ht_formula]], value_input_option="USER_ENTERED")  # type: ignore[arg-type]

    # LCQ_View
    ws_lcq = _safe_ws(sh, "LCQ_View", rows=1000, cols=12)
    lcq_header = ht_header
    lcq_top = [["Class Filter", ""], ["Cutoff Rank", "28"], lcq_header]
    lcq_formula = "=FILTER('Raw_HeatTotals'!A2:J, ('Raw_HeatTotals'!C:C=$B$1) * ('Raw_HeatTotals'!I:I>$B$2))"
    ws_lcq.clear()
    ws_lcq.update(range_name="A1", values=lcq_top + [[lcq_formula]], value_input_option="USER_ENTERED")  # type: ignore[arg-type]

# =========================
# Class-focused single-tab publishers (simple outputs)
# =========================
def publish_class_results_view(session_id: int) -> None:
    """
    Publish latest OFFICIAL results for the session into a tab '<ClassName> Results'.
    Intended for Heat/Qualifying sessions. Overwrites the tab each time.
    """
    sh = _open_sheet()
    with SessionLocal() as db:
        sess = _get_session(db, session_id)
        data = _fetch_official_results(db, session_id)
        
        class_name = getattr(sess.race_class, "name", "Unknown") if sess and sess.race_class else "Unknown"
        tab_name = f"{class_name} Results"

        header = ["Class", "Session", "Pos", "#", "Driver", "Team", "Chassis", "Best Lap", "Last Lap", "Status", "Updated"]
        rows: List[List[Any]] = []

        if sess.session_type not in ("Heat", "Qualifying"):
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows = [[f"Session '{sess.session_type}' is not Heat/Qualifying", now]]
        else:
            for row in data:
                rows.append([
                    class_name,
                    f"{sess.session_type} - {sess.session_name or ''}" if sess else "",
                    row.get("pos") or "",
                    row.get("number", ""),
                    f"{row.get('first','')} {row.get('last','')}".strip(),
                    row.get("team", ""),
                    row.get("chassis", ""),
                    _ms(row.get("best_ms")),
                    _ms(row.get("last_ms")),
                    row.get("status", ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ])

    _publish_rows(sh, tab_name, header, rows)

def publish_class_heat_totals_view(class_id: int, event_id: int) -> None:
    """
    Publish cumulative totals for the class into 'Class_HeatTotals'.
    Totals = sum(Heat points) + sum(Qualifying points). Qualifying displayed as fractional (x/100).
    Overwrites the tab each time.
    """
    sh = _open_sheet()
    with SessionLocal() as db:
        # Heat sessions
        heat_ids = [sid for (sid,) in db.query(RaceSession.id)
                                .filter(RaceSession.event_id == event_id,
                                        RaceSession.class_id == class_id,
                                        RaceSession.session_type == "Heat").all()]
        # Qual sessions
        qual_ids = [sid for (sid,) in db.query(RaceSession.id)
                                .filter(RaceSession.event_id == event_id,
                                        RaceSession.class_id == class_id,
                                        RaceSession.session_type == "Qualifying").all()]

        # Map number and names
        num_by_driver: Dict[int, str] = {}
        for e in db.query(Entry).filter(Entry.event_id == event_id, Entry.class_id == class_id).all():
            num_by_driver[e.driver_id] = e.number or ""

        class_name = (db.query(RaceSession)
                        .filter(RaceSession.class_id == class_id, RaceSession.event_id == event_id)
                        .first())
        class_name = class_name.race_class.name if class_name and class_name.race_class else ""

        # Latest official awards for Heat
        heat_pts: Dict[int, int] = {}
        if heat_ids:
            sub_latest_h = (
                db.query(PointAward.session_id, func.max(PointAward.version).label("v"))
                  .filter(PointAward.session_id.in_(heat_ids), PointAward.basis == "official")
                  .group_by(PointAward.session_id)
                  .subquery()
            )
            for pa in db.query(PointAward).join(sub_latest_h, (PointAward.session_id == sub_latest_h.c.session_id) & (PointAward.version == sub_latest_h.c.v)).all():
                heat_pts[pa.driver_id] = heat_pts.get(pa.driver_id, 0) + (pa.total_points or 0)

        # Latest official awards for Qualifying
        qual_pts: Dict[int, int] = {}
        if qual_ids:
            sub_latest_q = (
                db.query(PointAward.session_id, func.max(PointAward.version).label("v"))
                  .filter(PointAward.session_id.in_(qual_ids), PointAward.basis == "official")
                  .group_by(PointAward.session_id)
                  .subquery()
            )
            for pa in db.query(PointAward).join(sub_latest_q, (PointAward.session_id == sub_latest_q.c.session_id) & (PointAward.version == sub_latest_q.c.v)).all():
                qual_pts[pa.driver_id] = qual_pts.get(pa.driver_id, 0) + (pa.total_points or 0)

        # Qualifying best position tiebreak as backup
        qual_best_pos: Dict[int, int] = {}
        if qual_ids:
            sub_latest_qr = (
                db.query(Result.session_id, func.max(Result.version).label("v"))
                  .filter(Result.session_id.in_(qual_ids), Result.basis == "official")
                  .group_by(Result.session_id)
                  .subquery()
            )
            for r in db.query(Result).join(sub_latest_qr, (Result.session_id == sub_latest_qr.c.session_id) & (Result.version == sub_latest_qr.c.v)).filter(Result.position.isnot(None)).all():
                cur = qual_best_pos.get(r.driver_id)
                if cur is None or (r.position or 10**6) < cur:
                    qual_best_pos[r.driver_id] = r.position or cur or 10**6

        # Aggregate
        drivers = set(heat_pts.keys()) | set(qual_pts.keys())
        def key_fn(did: int):
            # total used for ranking; qual as tiebreaker; then number as stable
            return (
                (heat_pts.get(did, 0) + (qual_pts.get(did, 0) / 100.0)),
                qual_best_pos.get(did, 10**6),
                num_by_driver.get(did, "")
            )
        ranked = sorted(drivers, key=key_fn)

        header = ["Class","Rank","#","Driver","HeatPts","QualPts","TotalPts","QualTiebreak","Updated"]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows: List[List[Any]] = []
        for i, did in enumerate(ranked, start=1):
            # fetch driver name
            drow = db.get(Driver, did)
            name = f"{getattr(drow, 'first_name', '')} {getattr(drow, 'last_name', '')}".strip() if drow else ""
            h = heat_pts.get(did, 0)
            q = qual_pts.get(did, 0) / 100.0
            rows.append([
                class_name,
                i,
                num_by_driver.get(did, ""),
                name,
                h,
                q,
                h + q,
                qual_best_pos.get(did, ""),
                now,
            ])

    tab_name = f"{class_name} Heat Totals"
    _publish_rows(sh, tab_name, header, rows)

def publish_class_prefinal_view(class_id: int, event_id: int) -> None:
    """
    Build Prefinal grid for the class/event and write to 'Class_Prefinal'.
    Uses cumulative Heat points with Qualifying tiebreak (same as Raw_PrefinalGrid).
    """
    sh = _open_sheet()
    with SessionLocal() as db:
        # Reuse Raw_PrefinalGrid logic to compute ordering
        # Heat session IDs
        heat_ids = [sid for (sid,) in db.query(RaceSession.id)
                               .filter(RaceSession.event_id == event_id,
                                       RaceSession.class_id == class_id,
                                       RaceSession.session_type == "Heat").all()]
        if not heat_ids:
            _publish_rows(sh, "Class_Prefinal",
                          ["Class","Grid Pos","#","Driver","Total Heat Pts","Qual Tiebreak","Updated"],
                          [["", "No Heat sessions found for this class/event.", "", "", "", "", datetime.now().strftime("%Y-%m-%d %H:%M:%S")]])
            return

        sub_latest = (
            db.query(PointAward.session_id, func.max(PointAward.version).label("v"))
              .filter(PointAward.session_id.in_(heat_ids), PointAward.basis == "official")
              .group_by(PointAward.session_id)
              .subquery()
        )
        qa = (
            db.query(PointAward, Driver)
              .join(Driver, Driver.id == PointAward.driver_id)
              .join(sub_latest, (PointAward.session_id == sub_latest.c.session_id) & (PointAward.version == sub_latest.c.v))
              .all()
        )
        agg: Dict[int, int] = {}
        name_by_driver: Dict[int, str] = {}
        num_by_driver: Dict[int, str] = {}
        for e in db.query(Entry).filter(Entry.event_id == event_id, Entry.class_id == class_id).all():
            num_by_driver[e.driver_id] = e.number or ""
        for pa, d in qa:
            agg[d.id] = agg.get(d.id, 0) + (pa.total_points or 0)
            name_by_driver[d.id] = f"{d.first_name} {d.last_name}".strip()

        # Qualifying tiebreak: best official qualifying position
        q_ids = [sid for (sid,) in db.query(RaceSession.id)
                               .filter(RaceSession.event_id == event_id,
                                       RaceSession.class_id == class_id,
                                       RaceSession.session_type == "Qualifying").all()]
        qual_best_pos: Dict[int, int] = {}
        if q_ids:
            sub_latest_q = (db.query(Result.session_id, func.max(Result.version).label("v"))
                              .filter(Result.session_id.in_(q_ids), Result.basis == "official")
                              .group_by(Result.session_id)
                              .subquery())
            for r in (db.query(Result)
                        .join(sub_latest_q, (Result.session_id == sub_latest_q.c.session_id) & (Result.version == sub_latest_q.c.v))
                        .filter(Result.position.isnot(None)).all()):
                cur = qual_best_pos.get(r.driver_id)
                if cur is None or (r.position or 10**6) < cur:
                    qual_best_pos[r.driver_id] = r.position or cur or 10**6

        def key_fn(drv_id: int):
            return (agg.get(drv_id, 10**6),
                    qual_best_pos.get(drv_id, 10**6),
                    name_by_driver.get(drv_id, ""))
        ranked = sorted(agg.keys(), key=key_fn)

        # Class name
        cls = (db.query(RaceSession)
                 .filter(RaceSession.class_id == class_id, RaceSession.event_id == event_id)
                 .first())
        class_name = cls.race_class.name if cls and cls.race_class else "Unknown"

        header = ["Class","Grid Pos","#","Driver","Total Heat Pts","Qual Tiebreak","Updated"]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows: List[List[Any]] = []
        for i, did in enumerate(ranked, start=1):
            rows.append([
                class_name,
                i,
                num_by_driver.get(did, ""),
                name_by_driver.get(did, ""),
                agg.get(did, 0),
                qual_best_pos.get(did, ""),
                now,
            ])

    tab_name = f"{class_name} Prefinal"
    _publish_rows(sh, tab_name, header, rows)

def publish_raw_prefinal_grid(class_id: int, event_id: int) -> None:
    """
    Publish normalized prefinal grid to Raw_PrefinalGrid.
    """
    if not CFG.app.publish_prefinal_grid:
        return

    sh = _open_sheet()
    with SessionLocal() as db:
        heat_ids = [
            sid for (sid,) in db.query(RaceSession.id)
                                 .filter(RaceSession.event_id == event_id,
                                         RaceSession.class_id == class_id,
                                         RaceSession.session_type == "Heat")
                                 .all()
        ]
        if not heat_ids:
            _publish_rows(sh, "Raw_PrefinalGrid",
                          ["EventID","ClassID","Class","GridPos","DriverID","Number","Driver","TotalHeatPts","QualTiebreak","UpdatedUTC"],
                          [["", "", "No Heat sessions found for this class/event.", "", "", "", "", "", "", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]])
            return

        sub_latest = (
            db.query(PointAward.session_id, func.max(PointAward.version).label("v"))
              .filter(PointAward.session_id.in_(heat_ids), PointAward.basis == "official")
              .group_by(PointAward.session_id)
              .subquery()
        )

        qa = (
            db.query(PointAward, Driver)
              .join(Driver, Driver.id == PointAward.driver_id)
              .join(sub_latest, (PointAward.session_id == sub_latest.c.session_id) &
                               (PointAward.version == sub_latest.c.v))
              .all()
        )

        agg: Dict[int, int] = {}
        name_by_driver: Dict[int, str] = {}
        num_by_driver: Dict[int, str] = {}
        for e in db.query(Entry).filter(Entry.event_id == event_id,
                                        Entry.class_id == class_id).all():
            num_by_driver[e.driver_id] = e.number or ""

        for pa, d in qa:
            agg[d.id] = agg.get(d.id, 0) + (pa.total_points or 0)
            name_by_driver[d.id] = f"{d.first_name} {d.last_name}".strip()

        q_ids = [
            sid for (sid,) in db.query(RaceSession.id)
                                 .filter(RaceSession.event_id == event_id,
                                         RaceSession.class_id == class_id,
                                         RaceSession.session_type == "Qualifying")
                                 .all()
        ]
        qual_best_pos: Dict[int, int] = {}
        if q_ids:
            sub_latest_q = (
                db.query(Result.session_id, func.max(Result.version).label("v"))
                  .filter(Result.session_id.in_(q_ids), Result.basis == "official")
                  .group_by(Result.session_id)
                  .subquery()
            )
            qres = (
                db.query(Result)
                  .join(sub_latest_q, (Result.session_id == sub_latest_q.c.session_id) &
                                     (Result.version == sub_latest_q.c.v))
                  .filter(Result.position.isnot(None))
                  .all()
            )
            for r in qres:
                cur = qual_best_pos.get(r.driver_id)
                if cur is None or (r.position or 10**6) < cur:
                    qual_best_pos[r.driver_id] = r.position or cur or 10**6

        def key_fn(drv_id: int):
            return (agg.get(drv_id, 10**6),
                    qual_best_pos.get(drv_id, 10**6),
                    name_by_driver.get(drv_id, ""))

        ranked = sorted(agg.keys(), key=key_fn)

        class_name = (db.query(RaceSession)
                        .filter(RaceSession.class_id == class_id,
                                RaceSession.event_id == event_id)
                        .first())
        class_name = class_name.race_class.name if class_name and class_name.race_class else ""

        header = ["EventID","ClassID","Class","GridPos","DriverID","Number","Driver","TotalHeatPts","QualTiebreak","UpdatedUTC"]
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        rows: List[List[Any]] = []
        for i, drv_id in enumerate(ranked, start=1):
            rows.append([
                event_id, class_id, class_name, i, drv_id,
                num_by_driver.get(drv_id, ""),
                name_by_driver.get(drv_id, ""),
                agg.get(drv_id, 0),
                qual_best_pos.get(drv_id, ""),
                now_utc
            ])

    _publish_rows(sh, "Raw_PrefinalGrid", header, rows)

def publish_raw_heat_totals(class_id: int, event_id: int) -> None:
    """
    Publish cumulative Heat points standings per class/event into Raw_HeatTotals.
    Tie-breaker: best official Qualifying position (lower is better).
    Includes Rank for convenience.
    """
    if not CFG.app.publish_prefinal_grid and not CFG.app.publish_points:
        # If either toggle is off, we still allow totals when points are being used.
        pass

    sh = _open_sheet()
    with SessionLocal() as db:
        # Heat session IDs
        heat_ids = [
            sid for (sid,) in db.query(RaceSession.id)
                                 .filter(RaceSession.event_id == event_id,
                                         RaceSession.class_id == class_id,
                                         RaceSession.session_type == "Heat")
                                 .all()
        ]
        if not heat_ids:
            _publish_rows(sh, "Raw_HeatTotals",
                          ["EventID","ClassID","Class","DriverID","Number","Driver","TotalHeatPts","QualTiebreak","Rank","UpdatedUTC"],
                          [[event_id, class_id, "No Heat sessions found for this class/event.", "", "", "", "", "", "", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]])
            return

        sub_latest = (
            db.query(PointAward.session_id, func.max(PointAward.version).label("v"))
              .filter(PointAward.session_id.in_(heat_ids), PointAward.basis == "official")
              .group_by(PointAward.session_id)
              .subquery()
        )

        qa = (
            db.query(PointAward, Driver)
              .join(Driver, Driver.id == PointAward.driver_id)
              .join(sub_latest, (PointAward.session_id == sub_latest.c.session_id) &
                               (PointAward.version == sub_latest.c.v))
              .all()
        )

        agg: Dict[int, int] = {}
        name_by_driver: Dict[int, str] = {}
        num_by_driver: Dict[int, str] = {}
        for e in db.query(Entry).filter(Entry.event_id == event_id,
                                        Entry.class_id == class_id).all():
            num_by_driver[e.driver_id] = e.number or ""

        for pa, d in qa:
            agg[d.id] = agg.get(d.id, 0) + (pa.total_points or 0)
            name_by_driver[d.id] = f"{d.first_name} {d.last_name}".strip()

        # Qualifying tiebreak: best official qualifying position
        q_ids = [
            sid for (sid,) in db.query(RaceSession.id)
                                 .filter(RaceSession.event_id == event_id,
                                         RaceSession.class_id == class_id,
                                         RaceSession.session_type == "Qualifying")
                                 .all()
        ]
        qual_best_pos: Dict[int, int] = {}
        if q_ids:
            sub_latest_q = (
                db.query(Result.session_id, func.max(Result.version).label("v"))
                  .filter(Result.session_id.in_(q_ids), Result.basis == "official")
                  .group_by(Result.session_id)
                  .subquery()
            )
            qres = (
                db.query(Result)
                  .join(sub_latest_q, (Result.session_id == sub_latest_q.c.session_id) &
                                     (Result.version == sub_latest_q.c.v))
                  .filter(Result.position.isnot(None))
                  .all()
            )
            for r in qres:
                cur = qual_best_pos.get(r.driver_id)
                if cur is None or (r.position or 10**6) < cur:
                    qual_best_pos[r.driver_id] = r.position or cur or 10**6

        def key_fn(drv_id: int):
            return (agg.get(drv_id, 10**6),
                    qual_best_pos.get(drv_id, 10**6),
                    name_by_driver.get(drv_id, ""))

        ranked = sorted(agg.keys(), key=key_fn)

        class_name = (db.query(RaceSession)
                        .filter(RaceSession.class_id == class_id,
                                RaceSession.event_id == event_id)
                        .first())
        class_name = class_name.race_class.name if class_name and class_name.race_class else ""

        header = ["EventID","ClassID","Class","DriverID","Number","Driver","TotalHeatPts","QualTiebreak","Rank","UpdatedUTC"]
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        rows: List[List[Any]] = []
        for i, drv_id in enumerate(ranked, start=1):
            rows.append([
                event_id, class_id, class_name,
                drv_id,
                num_by_driver.get(drv_id, ""),
                name_by_driver.get(drv_id, ""),
                agg.get(drv_id, 0),
                qual_best_pos.get(drv_id, ""),
                i,
                now_utc
            ])

    _publish_rows(sh, "Raw_HeatTotals", header, rows)

def ensure_heat_totals_class_views(event_id: int) -> None:
    """
    Create/update HeatTotals_<ClassName> tabs that show cumulative Heat totals for that class
    by filtering Raw_HeatTotals.
    """
    sh = _open_sheet()
    with SessionLocal() as db:
        # All classes for the event
        classes = db.query(RaceSession.class_id, RaceSession.event_id).filter(RaceSession.event_id == event_id).distinct().all()
        names: Dict[int, str] = {}
        for cid, _ in classes:
            rc = db.query(RaceSession).filter(RaceSession.class_id == cid, RaceSession.event_id == event_id).first()
            if rc and rc.race_class and rc.race_class.name:
                names[cid] = rc.race_class.name

    def sanitize_title(title: str) -> str:
        bad = ":/\\?*[]"
        out = "".join(ch for ch in title if ch not in bad)
        return out[:95]

    for _, cname in names.items():
        tab = f"HeatTotals_{sanitize_title(cname)}"
        ws = _safe_ws(sh, tab, rows=1000, cols=12)
        header = ["EventID","ClassID","Class","DriverID","Number","Driver","TotalHeatPts","QualTiebreak","Rank","UpdatedUTC"]
        formula = (
            f"=FILTER('Raw_HeatTotals'!A2:J, 'Raw_HeatTotals'!C:C=\"{cname}\")"
        )
        ws.clear()
        ws.update(range_name="A1", values=[header, [formula]], value_input_option="USER_ENTERED")  # type: ignore[arg-type]
