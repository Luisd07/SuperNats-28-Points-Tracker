from __future__ import annotations
import os
import sys
from dataclasses import dataclass
from pathlib import Path

# Determine the directory where the executable/script is located
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    APP_DIR = Path(sys.executable).parent
else:
    # Running as script
    APP_DIR = Path(__file__).parent

# Optional: load a .env file if present (no hard dependency)
try:
    from dotenv import load_dotenv  # type: ignore
    # Look for .env next to the executable/script
    env_path = APP_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()  # Try current directory as fallback
except Exception:
    pass


@dataclass
class GoogleCfg:
    spreadsheet_id: str | None
    service_json_path: str | None
    service_json_raw: str | None
    tab_results: str
    tab_heat_points: str
    tab_prefinal_grid: str


@dataclass
class AppCfg:
    points_scheme: str
    publish_results: bool
    publish_points: bool
    publish_prefinal_grid: bool
    publish_raw_tabs: bool


@dataclass
class Config:
    google: GoogleCfg
    app: AppCfg


def load_config() -> Config:
    spreadsheet_id = os.getenv("GS_SPREADSHEET_ID")
    service_path = os.getenv("GS_SERVICE_JSON_PATH")
    service_raw = os.getenv("GS_SERVICE_JSON_RAW")

    sheets_ready = bool(spreadsheet_id and (service_path or service_raw))

    return Config(
        google=GoogleCfg(
            spreadsheet_id=spreadsheet_id,
            service_json_path=service_path,
            service_json_raw=service_raw,
            tab_results=os.getenv("GS_TAB_RESULTS", "Official Results"),
            tab_heat_points=os.getenv("GS_TAB_HEAT_POINTS", "Heat Points"),
            tab_prefinal_grid=os.getenv("GS_TAB_PREFINAL_GRID", "Prefinal Grid"),
        ),
        app=AppCfg(
            points_scheme=os.getenv("POINTS_SCHEME", "SKUSA_SN28"),
            publish_results=(os.getenv("PUBLISH_RESULTS", "1") == "1") and sheets_ready,
            publish_points=(os.getenv("PUBLISH_POINTS", "1") == "1") and sheets_ready,
            publish_prefinal_grid=(os.getenv("PUBLISH_PREFINAL_GRID", "1") == "1") and sheets_ready,
            publish_raw_tabs=(os.getenv("PUBLISH_RAW_TABS", "0") == "1") and sheets_ready,
        ),
    )


CFG = load_config()
