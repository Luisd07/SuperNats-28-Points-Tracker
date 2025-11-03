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
    # Create tables if missing
    Base.metadata.create_all(bind=engine)


def get_db_path() -> Path:
    """Expose the resolved DB path for diagnostics/UI."""
    return _DB_PATH