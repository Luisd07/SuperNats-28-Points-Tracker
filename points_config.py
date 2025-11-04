# points_config.py
from __future__ import annotations
import argparse
from typing import Dict
from sqlalchemy import delete
from db import SessionLocal, init_db
from models import Point, PointScale

SCHEME_NAME = "SKUSA_SN28"

# SKUSA SuperNats heat scoring: 0 (P1), 2 (P2), 3 (P3), 4 (P4), ..., 120 (P120)
def build_skusa_heat_scale(field_size: int = 120) -> Dict[int, int]:
    scale: Dict[int, int] = {1: 0}
    for pos in range(2, field_size + 1):
        scale[pos] = pos  # 2->2, 3->3, 4->4, ...
    return scale

# SKUSA SuperNats qualifying scoring: fractional points for tiebreaker
# 0.01 (P1), 0.02 (P2), 0.03 (P3), ..., 1.20 (P120)
def build_skusa_qualifying_scale(field_size: int = 120) -> Dict[int, float]:
    scale: Dict[int, float] = {}
    for pos in range(1, field_size + 1):
        scale[pos] = pos * 0.01  # 1->0.01, 2->0.02, ..., 120->1.20
    return scale

def seed_skusa_sn28(field_size: int = 120, bonus_fast_lap: int = 0, bonus_pole: int = 0) -> None:
    init_db()
    with SessionLocal() as db:
        # Upsert the Point scheme row
        pt = db.query(Point).filter(Point.name == SCHEME_NAME).one_or_none()
        if not pt:
            pt = Point(
                name=SCHEME_NAME,
                bonus_lap=bool(bonus_fast_lap),
                bonus_pole=bool(bonus_pole),
            )
            db.add(pt); db.flush()
        else:
            changed = False
            if pt.bonus_lap != bool(bonus_fast_lap):
                pt.bonus_lap = bool(bonus_fast_lap); changed = True
            if pt.bonus_pole != bool(bonus_pole):
                pt.bonus_pole = bool(bonus_pole); changed = True
            if changed:
                db.flush()

        # Replace all Heat scales for this scheme
        db.execute(
            delete(PointScale).where(
                PointScale.point_id == pt.id,
                PointScale.session_type == "Heat",
            )
        )
        heat_scale = build_skusa_heat_scale(field_size)
        for position, points in sorted(heat_scale.items()):
            db.add(PointScale(
                point_id=pt.id,
                session_type="Heat",
                position=position,
                points=points
            ))

        # Replace all Qualifying scales for this scheme
        db.execute(
            delete(PointScale).where(
                PointScale.point_id == pt.id,
                PointScale.session_type == "Qualifying",
            )
        )
        qual_scale = build_skusa_qualifying_scale(field_size)
        for position, points in sorted(qual_scale.items()):
            db.add(PointScale(
                point_id=pt.id,
                session_type="Qualifying",
                position=position,
                points=points
            ))

        db.commit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Seed SKUSA SuperNats 28 Heat and Qualifying points")
    ap.add_argument("--field", type=int, default=120, help="Max positions to seed")
    ap.add_argument("--bonus-fast-lap", type=int, default=0)
    ap.add_argument("--bonus-pole", type=int, default=0)
    args = ap.parse_args()
    seed_skusa_sn28(field_size=args.field,
                    bonus_fast_lap=args.bonus_fast_lap,
                    bonus_pole=args.bonus_pole)
    print(f"Seeded {SCHEME_NAME} Heat and Qualifying points for {args.field} positions")
