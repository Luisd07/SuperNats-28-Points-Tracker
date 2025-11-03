# app.py
from flask import Flask, request, jsonify
from flask.typing import ResponseReturnValue
from typing import Optional
import os
from db import SessionLocal, init_db
from official import compute_official_order, write_official_and_award_points
from models import Penalty

app = Flask(__name__)
init_db()


def _kart_to_driver_id(db, session_id: int, kart_number: str) -> Optional[int]:
    """Resolve a kart number to a driver_id within the session's event/class."""
    from models import Session as RaceSession, Entry
    sess = db.get(RaceSession, session_id)
    if not sess:
        return None
    e = (
        db.query(Entry)
          .filter(Entry.event_id == sess.event_id,
                  Entry.class_id == sess.class_id,
                  Entry.number == kart_number)
          .first()
    )
    return e.driver_id if e else None

@app.post("/sessions/<int:sid>/penalties")
def add_penalties(sid: int) -> ResponseReturnValue:
    """
    Accepts either:
    - JSON: [{"driver_id":7, "type":"POSITION", "value_positions":3, "note":"start infraction"}, ...]
    - text/plain bulk: '541 +5s | 077 DQ | 119 -3pos'
    """
    with SessionLocal() as db:
        if request.is_json:
            payload = request.get_json(force=True)
            for p in payload:
                db.add(Penalty(
                    session_id=sid,
                    driver_id=p["driver_id"],
                    type=p["type"],
                    value_ms=p.get("value_ms"),
                    value_positions=p.get("value_positions"),
                    lap_no=p.get("lap_no"),
                    note=p.get("note"),
                    source=p.get("source","Stewards")
                ))
            db.commit()
            return jsonify({"ok": True})
        else:
            # minimal bulk parser (extend as needed)
            bulk = request.data.decode("utf-8")
            items = [x.strip() for x in bulk.replace("\n","|").split("|") if x.strip()]
            not_found: list[str] = []
            for it in items:
                # "541 +5s" or "077 DQ" or "119 -3pos"
                parts = it.split()
                if len(parts) < 2:
                    continue
                num = parts[0]
                code = parts[1].lower()
                driver_id = _kart_to_driver_id(db, sid, num)
                if not driver_id:
                    not_found.append(num)
                    continue
                if "dq" in code:
                    db.add(Penalty(session_id=sid, driver_id=driver_id, type="DQ"))
                elif "pos" in code:
                    try:
                        drop = int(code.replace("pos","" ).replace("-",""))
                    except ValueError:
                        continue
                    db.add(Penalty(session_id=sid, driver_id=driver_id, type="POSITION", value_positions=drop))
                elif code.endswith("s"):
                    try:
                        ms = int(float(code[:-1]) * 1000)
                    except ValueError:
                        continue
                    db.add(Penalty(session_id=sid, driver_id=driver_id, type="TIME", value_ms=ms))
            db.commit()
        return jsonify({"ok": True, "missing_numbers": not_found})
    # Fallback (should not hit)
    return jsonify({"ok": True})

@app.post("/sessions/<int:sid>/preview_official")
def preview_official(sid: int) -> ResponseReturnValue:
    with SessionLocal() as db:
        officials = compute_official_order(db, sid)
        return jsonify([{
            "driver_id": r.driver_id,
            "position": r.position,
            "status_code": r.status_code,
            "best_lap_ms": r.best_lap_ms
        } for r in officials])
    # Fallback
    return jsonify([])

@app.post("/sessions/<int:sid>/publish_official")
def publish_official(sid: int) -> ResponseReturnValue:
    with SessionLocal() as db:
        write_official_and_award_points(db, sid, scheme_name="SKUSA_SN28")
        return jsonify({"ok": True})
    # Fallback
    return jsonify({"ok": False})


def main():
    host = os.getenv("API_HOST", "127.0.0.1")
    port = int(os.getenv("API_PORT", "5000"))
    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
