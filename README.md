# SuperNats-28-Points-Tracker

Lightweight toolkit to ingest live timing (Orbits TCP feed), maintain provisional results, apply steward penalties, publish official results, and compute heat points for SKUSA SuperNats 28.

## What’s inside

- `socket_listener.py` — TCP client that parses Orbits packets and persists laps + live results.
- `ui.py` — Steward-facing penalty pad to add DQ / position / time / lap invalid penalties and preview the official order.
- `official.py` — Applies penalties and writes an official, versioned snapshot; awards Heat points.
- `sheets_publish.py` — Pushes official results, heat points, and Prefinal grid to Google Sheets.
- `points_config.py` — Seeds the SKUSA SN28 Heat point scale.
- `app.py` — Minimal Flask API to add penalties and publish sessions programmatically.
- `sn28_config.py` — App configuration loaded from environment/.env.

## Quick start

1. Create and activate a virtual environment (Windows PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Configure environment (optional .env):

- Copy `.env.example` to `.env` and fill in values, or set as real environment variables.

Key variables:

- `GS_SPREADSHEET_ID` — target Google Sheet ID
- `GS_SERVICE_JSON_PATH` or `GS_SERVICE_JSON_RAW` — service account credentials
- `GS_TAB_RESULTS` / `GS_TAB_HEAT_POINTS` / `GS_TAB_PREFINAL_GRID` — tab names
- `POINTS_SCHEME` — defaults to `SKUSA_SN28`
- `PUBLISH_RESULTS` / `PUBLISH_POINTS` / `PUBLISH_PREFINAL_GRID` — `1` to enable

3. Install as an editable package (adds the `sn28` CLI):

```powershell
pip install -e .
```

4. Seed the Heat points scale (once):

```powershell
python points_config.py --field 50
```

5. Start the TCP ingest (reads from Orbits):

```powershell
python socket_listener.py --host 127.0.0.1 --port 50000
```

6. Open the Penalty Pad UI in another terminal:

```powershell
python ui.py
```

7. (Optional) Use the API for bulk operations:

- Add penalties (JSON or bulk text). See `app.py` for routes:
  - `POST /sessions/<sid>/penalties`
  - `POST /sessions/<sid>/preview_official`
  - `POST /sessions/<sid>/publish_official`

## Notes

## One-command runs (CLI)

With the package installed, you can run multiple components together:

```powershell
# Double-click the EXE or run without args to start Listener + UI by default
# (uses ORBITS_HOST/ORBITS_PORT env if set)
sn28

# Explicit: Start Listener + UI
sn28 run --listen --ui --listen-host 127.0.0.1 --listen-port 50000

# Start API server only
sn28 run --api --api-host 127.0.0.1 --api-port 5000

# Seed SKUSA points scale
sn28 seed --field 50

# Convenience subcommands
sn28 ui
sn28 listen --host 127.0.0.1 --port 50000
sn28 api --host 127.0.0.1 --port 5000
```

- Official result versions: each publish creates `basis=official, version=N`. Heat points are written using the SAME version for consistency.
- Prefinal grid builder sums official Heat points and tie‑breaks by official Qualifying position.

## Troubleshooting

- Google Sheets: ensure service account has access to the spreadsheet; set `GS_SERVICE_JSON_PATH` or `GS_SERVICE_JSON_RAW`.
- SQLite file `supernats28.db` is created in the repo root. Delete it to reset, or use SQLite browser to inspect.
- If UI shows no sessions, confirm the socket listener is running and Orbits is sending packets.

## License

For event operations use. No warranty.
