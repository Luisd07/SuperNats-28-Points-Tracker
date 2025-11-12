import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def _data_dir() -> Path:
    r"""Return per-user writable data directory.

    On Windows, use %APPDATA%\SuperNats28. Else, ~/.supernats28
    Allow override via SN28_DATA_DIR env var.
    """
    override = os.getenv("SN28_DATA_DIR")
    if override:
        p = Path(override)
    else:
        appdata = os.getenv("APPDATA")
        if appdata:
            p = Path(appdata) / "SuperNats28"
        else:
            p = Path.home() / ".supernats28"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _default_db_path() -> Path:
    return _data_dir() / "supernats28.db"


_DB_PATH = _default_db_path()
_DB_URL = "sqlite:///" + str(_DB_PATH).replace("\\", "/")

engine = create_engine(_DB_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def init_db():
    """Initialize database schema if not present.

    On first run, this creates the SQLite file in the per-user data dir and
    creates tables from metadata. If you later add a pre-seeded DB file to the
    bundle, you can copy it here when not exists before creating tables.
    """
    from models import Base
    # Ensure parent exists
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Track whether the DB file existed before we called create_all. If it did
    # not, this is a fresh DB and seeding is appropriate. If it did exist, we
    # will still check whether the points scheme is present and only seed when
    # missing — we must not overwrite an existing DB.
    db_file_existed = _DB_PATH.exists()

    # Create tables if missing (this will not overwrite an existing DB file)
    Base.metadata.create_all(bind=engine)

    # Auto-seed default points scheme if not present. Only insert rows when
    # the Point scheme is absent to avoid duplicates. This covers the case
    # where a DB file exists but the points were not yet seeded.
    try:
        from models import Point, PointScale
        import logging

        logger = logging.getLogger(__name__)

        with SessionLocal() as db:
            existing = db.query(Point).filter(Point.name == "SKUSA_SN28").first()
            if existing:
                logger.debug("Points scheme SKUSA_SN28 already present; skipping auto-seed")
                return

            logger.info("Auto-seeding default points scheme (SKUSA_SN28) into DB: %s", _DB_PATH)

            # create default scheme (defaults: no bonus lap/pole)
            pt = Point(name="SKUSA_SN28", bonus_lap=False, bonus_pole=False)
            db.add(pt)
            db.flush()

            # Heat scale: position 1 -> 0 points, others map to their position
            for position in range(1, 121):
                heat_points = 0 if position == 1 else position
                db.add(PointScale(point_id=pt.id, session_type="Heat", position=position, points=heat_points))

            # Qualifying scale: store 1..N (rendered elsewhere as fractional hundredths)
            for position in range(1, 121):
                db.add(PointScale(point_id=pt.id, session_type="Qualifying", position=position, points=position))

            db.commit()
            logger.info("Seeding complete: SKUSA_SN28 inserted with %d heat entries", 120)
    except Exception:
        # Non-fatal: if models/DB unavailable or commit fails, ignore and continue.
        # The app can still function; user can run the explicit seeder later.
        try:
            import logging
            logging.getLogger(__name__).exception("Auto-seed failed")
        except Exception:
            pass


def get_db_path() -> Path:
    """Expose the resolved DB path for diagnostics/UI."""
    return _DB_PATH