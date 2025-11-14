from __future__ import annotations
import os
import json
from pathlib import Path
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import font as tkfont
from typing import List, Dict, Optional, cast
from datetime import datetime
from sqlalchemy.orm import joinedload


# === Your project modules ===
from db import SessionLocal, init_db
from models import (
    Session as RaceSession, Result, Driver, Entry, Penalty, Lap
)
# official.py should expose:
#   - compute_official_order(db, session_id) -> List[Result] (transient, not persisted)
#   - write_official_and_award_points(db, session_id, scheme_name="SKUSA_SN28") -> None
from official import compute_official_order, write_official_and_award_points

# sheets_publish.py should expose:
#   - publish_official_results(session_id) or publish_raw_results(session_id)
#   - publish_heat_points(session_id) or publish_raw_heat_points(session_id)
#   - publish_prefinal_grid(class_id, event_id) or publish_raw_prefinal_grid(class_id, event_id)
from sheets_publish import (
    publish_class_results_view, publish_class_heat_totals_view, publish_class_prefinal_view,
    publish_raw_results, publish_raw_points, publish_raw_prefinal_grid, publish_raw_heat_totals,
)

# config.py must define CFG (with points_scheme, publish toggles, etc.)
from sn28_config import CFG
from socket_listener import OrbitsParser, DBIngestor, OrbitsTCPReader

init_db()

# ---------- Data helpers ----------
def list_sessions() -> List[RaceSession]:
    with SessionLocal() as db:
        return (
            db.query(RaceSession)
              .options(joinedload(RaceSession.race_class))  # <-- eager load
              .order_by(RaceSession.id.desc())
              .all()
        )

def get_session(sid: int) -> Optional[RaceSession]:
    with SessionLocal() as db:
        return db.get(RaceSession, sid)

def kart_to_driver_id(db, sess: Optional[RaceSession], kart_number: str) -> Optional[int]:
    """Resolve a kart number to a driver_id within the current session's event/class."""
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

def get_latest_provisional(db, session_id: int) -> List[Dict]:
    """Return the latest provisional results for a session, joined with driver info and kart numbers."""
    ver = (
        db.query(Result.version)
          .filter(Result.session_id == session_id, Result.basis == "provisional")
          .order_by(Result.version.desc())
          .first()
    )
    if not ver:
        return []

    v = ver[0]
    sess = db.get(RaceSession, session_id)
    class_id = getattr(sess, "class_id", None)
    event_id = getattr(sess, "event_id", None)

    rows = (
        db.query(Result, Driver)
          .join(Driver, Driver.id == Result.driver_id)
          .filter(Result.session_id == session_id,
                  Result.basis == "provisional",
                  Result.version == v)
          .order_by(Result.position.is_(None), Result.position.asc())
          .all()
    )

    out: List[Dict] = []
    for r, d in rows:
        # Resolve kart number for this class/event
        num = ""
        if class_id is not None and event_id is not None:
            for e in d.entries:
                if e.class_id == class_id and e.event_id == event_id:
                    num = e.number or ""
                    break
        out.append({
            "driver_id": d.id,
            "pos": r.position,
            "num": num,
            "name": f"{(d.first_name or '')} {(d.last_name or '')}".strip(),
            "status": r.status_code or "",
            "best_ms": r.best_lap_ms,
            "last_ms": r.last_lap_ms,
        })
    return out

def ms_fmt(ms: Optional[int]) -> str:
    if ms is None:
        return ""
    s = ms / 1000.0
    if s >= 60:
        m = int(s // 60)
        return f"{m}:{(s - 60*m):06.3f}"
    return f"{s:.3f}"

# --- UI helpers ---
def autosize_treeview_columns(tv: ttk.Treeview, columns: List[str], padding: int = 12,
                              min_widths: Optional[Dict[str, int]] = None,
                              max_widths: Optional[Dict[str, int]] = None) -> None:
    """Resize specified columns to fit the widest cell value or header.
    - padding: extra pixels to add for readability
    - min_widths/max_widths: optional per-column constraints
    """
    try:
        fnt = tkfont.nametofont("TkDefaultFont")
    except Exception:
        fnt = None
    for col in columns:
        # Start with header text width
        header_text = col.upper()
        max_w = fnt.measure(header_text) if fnt else len(header_text) * 7
        for iid in tv.get_children():
            val = tv.set(iid, col)
            txt = str(val)
            w = fnt.measure(txt) if fnt else len(txt) * 7
            if w > max_w:
                max_w = w
        if min_widths and col in min_widths:
            max_w = max(max_w, int(min_widths[col]))
        if max_widths and col in max_widths:
            max_w = min(max_w, int(max_widths[col]))
        tv.column(col, width=int(max_w + padding))

# ---------- Tkinter App ----------
class PenaltyApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("KC Points Tracker")
        self.geometry("1150x700")
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Listener runtime state
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_reader: Optional[OrbitsTCPReader] = None
        self.listener_status = tk.StringVar(value="Listener: stopped")
        self._settings: Dict[str, str] = self._load_settings()
        # Top: session picker + info + publish
        # Auto-refresh state (UI polling for latest results)
        # Default from persisted settings (string stored), else True
        try:
            ar = self._settings.get("auto_refresh", "True")
            ar_val = True if str(ar).lower() in ("1", "true", "yes") else False
        except Exception:
            ar_val = True
        self.auto_refresh_var = tk.BooleanVar(value=ar_val)
        self._auto_refresh_job: Optional[object] = None

        # Top: session picker + info + publish
        top = ttk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        ttk.Label(top, text="Session:").pack(side="left")
        self.session_cb = ttk.Combobox(top, state="readonly", width=60)
        self.session_cb.pack(side="left", padx=6)
        self.session_cb.bind("<<ComboboxSelected>>", self.on_session_change)
        # Manual refresh control + Auto-refresh checkbox
        ttk.Checkbutton(top, text="Auto-refresh", variable=self.auto_refresh_var,
                        command=self._on_auto_refresh_toggle).pack(side="left", padx=(6,0))
        ttk.Button(top, text="Refresh", command=lambda: (self.refresh_all(), self.status.set("Manual refresh"))).pack(side="left", padx=(6,6))
        ttk.Button(top, text="Publish Session Results", command=self.publish_official).pack(side="right")
        ttk.Button(top, text="Publish Heat Totals (Class)", command=self.publish_heat_totals_for_class).pack(side="right", padx=(0,6))
        ttk.Button(top, text="Publish Prefinal Grid (Class)", command=self.publish_prefinal_for_class).pack(side="right", padx=(0,6))

        # Notebook hosts Penalties and Listener tabs
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        penalties_tab = ttk.Frame(nb)
        listener_tab = ttk.Frame(nb)
        nb.add(penalties_tab, text="Penalties")
        nb.add(listener_tab, text="Listener")

        # Middle: split into three panes (inside Penalties tab)
        mid = ttk.PanedWindow(penalties_tab, orient=tk.HORIZONTAL)
        mid.pack(fill="both", expand=True)

        # Left: provisional table
        self.prov_frame = ttk.Frame(mid)
        ttk.Label(self.prov_frame, text="Provisional (latest)").pack(anchor="w")
        self.prov_tv = ttk.Treeview(self.prov_frame, columns=("pos","num","name","best","last"),
                                    show="headings", height=20)
        for c, w in [("pos",50),("num",70),("name",150),("best",90),("last",90)]:
            self.prov_tv.heading(c, text=c.upper())
            # Default widths; we'll autosize after filling data
            self.prov_tv.column(c, width=w, anchor="w", stretch=(c == "name"))
        self.prov_tv.pack(fill="both", expand=True)
        # Horizontal scrollbar for provisional table
        self.prov_xscroll = ttk.Scrollbar(self.prov_frame, orient="horizontal", command=self.prov_tv.xview)
        self.prov_tv.configure(xscrollcommand=self.prov_xscroll.set)
        self.prov_xscroll.pack(fill="x")
        mid.add(self.prov_frame, weight=3)

        # Center: penalties (form + table)
        self.pen_frame = ttk.Frame(mid)
        ttk.Label(self.pen_frame, text="Penalties").pack(anchor="w")

        form = ttk.Frame(self.pen_frame); form.pack(fill="x", pady=4)
        ttk.Label(form, text="Kart #").grid(row=0, column=0, sticky="w")
        self.kart_entry = ttk.Entry(form, width=8)
        self.kart_entry.grid(row=0, column=1, padx=4)

        ttk.Label(form, text="Type").grid(row=0, column=2, sticky="w")
        self.type_cb = ttk.Combobox(form, values=["POSITION","TIME","DQ","LAP_INVALID"],
                                    state="readonly", width=12)
        self.type_cb.set("POSITION")
        self.type_cb.grid(row=0, column=3, padx=4)

        ttk.Label(form, text="Value").grid(row=0, column=4, sticky="w")
        self.value_entry = ttk.Entry(form, width=10)
        self.value_entry.grid(row=0, column=5, padx=4)
        ttk.Label(form, text="(e.g. 3 / 5s / lap# or 'best')").grid(row=0, column=6, sticky="w")

        ttk.Label(form, text="Note").grid(row=1, column=0, sticky="w", pady=(4,0))
        self.note_entry = ttk.Entry(form, width=40)
        self.note_entry.grid(row=1, column=1, columnspan=5, sticky="we", padx=4, pady=(4,0))
        ttk.Button(form, text="Add", command=self.add_penalty).grid(row=1, column=6, padx=4, pady=(4,0))

        # quick preset buttons (common SKUSA penalties)
        presets = ttk.Frame(self.pen_frame); presets.pack(fill="x", pady=(0,4))
        quick_items = [
            ("+3s", "time:3"),      # PBB one side, jump start, pushing, scrubbing, 2 wheels out, blocking, track limits
            ("+5s", "time:5"),      # 4 wheels out, cut track advantage, careless IR
            ("+6s", "time:6"),      # PBB both sides
            ("+10s", "time:10"),    # advancing after cone, manipulating start, passing under yellow, unsafe re-entry, reckless IR
            ("Loss Fast Lap", "lap:best"),  # Qualifying common
            ("DQ", "dq"),
            ("-1 pos", "pos:1"),    # keep position drops as needed by stewards
            ("-3 pos", "pos:3"),
        ]
        # Place buttons in two rows using grid so they fit well without shifting side panes
        ttk.Label(presets, text="Quick:").grid(row=0, column=0, padx=(0,6), pady=(0,2), sticky="w")
        for idx, (label, payload) in enumerate(quick_items):
            r, c = divmod(idx, 4)  # 4 buttons per row
            ttk.Button(presets, text=label, command=lambda p=payload: self.apply_preset(p)).grid(row=r, column=c+1, padx=3, pady=2, sticky="w")

        self.pen_tv = ttk.Treeview(self.pen_frame, columns=("driver_id","type","value","note","created"),
                                   show="headings", height=12)
        for c, w in [("driver_id",80),("type",100),("value",100),("note",240),("created",140)]:
            self.pen_tv.heading(c, text=c.upper())
            self.pen_tv.column(c, width=w, anchor="w")
        self.pen_tv.pack(fill="both", expand=True, pady=(4,0))

        btns = ttk.Frame(self.pen_frame); btns.pack(fill="x", pady=4)
        ttk.Button(btns, text="Delete Selected", command=self.delete_selected_penalty).pack(side="left")
        ttk.Button(btns, text="Clear All (this session)", command=self.clear_all_penalties).pack(side="left", padx=6)
        ttk.Button(btns, text="Preview Official (recompute)", command=self.refresh_preview).pack(side="right")

        mid.add(self.pen_frame, weight=3)

        # Right: preview official
        self.prev_frame = ttk.Frame(mid)
        ttk.Label(self.prev_frame, text="Preview: Official (after penalties)").pack(anchor="w")
        self.prev_tv = ttk.Treeview(self.prev_frame, columns=("pos","num","name","best","last"),
                                    show="headings", height=20)
        for c, w in [("pos",50),("num",70),("name",150),("best",90),("last",90)]:
            self.prev_tv.heading(c, text=c.upper())
            self.prev_tv.column(c, width=w, anchor="w", stretch=(c == "name"))
        self.prev_tv.pack(fill="both", expand=True)
        # Horizontal scrollbar for official preview table
        self.prev_xscroll = ttk.Scrollbar(self.prev_frame, orient="horizontal", command=self.prev_tv.xview)
        self.prev_tv.configure(xscrollcommand=self.prev_xscroll.set)
        self.prev_xscroll.pack(fill="x")
        mid.add(self.prev_frame, weight=3)

        # Listener tab content
        self._build_listener_tab(listener_tab)

        # Status bar
        self.status = tk.StringVar(value="Ready.")
        ttk.Label(self, textvariable=self.status, anchor="w").pack(fill="x", padx=8, pady=(0,6))

        # Start auto-refresh if the checkbox was set from persisted settings
        try:
            if getattr(self, "auto_refresh_var", None) and self.auto_refresh_var.get():
                self._start_auto_refresh()
                self.status.set("Auto-refresh: ON")
            else:
                self.status.set("Auto-refresh: OFF")
        except Exception:
            pass

        # Shortcuts
        self.bind("<Return>", lambda e: self.add_penalty())
        self.bind("<F5>", lambda e: self.refresh_all())

        # Initial data
        self._sessions: List[RaceSession] = []
        self.refresh_sessions()

    # ----- Listener tab UI -----
    def _build_listener_tab(self, parent: tk.Widget) -> None:
        frm = ttk.Frame(parent)
        frm.pack(fill="x", padx=8, pady=8)

        ttk.Label(frm, text="Orbits Host").grid(row=0, column=0, sticky="w")
        default_host = self._settings.get("orbits_host") or os.getenv("ORBITS_HOST", "127.0.0.1")
        self.listen_host_var = tk.StringVar(value=default_host)
        ttk.Entry(frm, textvariable=self.listen_host_var, width=20).grid(row=0, column=1, padx=6, pady=4)

        ttk.Label(frm, text="Orbits Port").grid(row=0, column=2, sticky="w")
        default_port = self._settings.get("orbits_port") or os.getenv("ORBITS_PORT", "50000")
        self.listen_port_var = tk.StringVar(value=str(default_port))
        ttk.Entry(frm, textvariable=self.listen_port_var, width=10).grid(row=0, column=3, padx=6, pady=4)

        ttk.Button(frm, text="Start Listener", command=self.start_listener).grid(row=0, column=4, padx=8)
        ttk.Button(frm, text="Stop Listener", command=self.stop_listener).grid(row=0, column=5, padx=4)

        ttk.Label(parent, textvariable=self.listener_status).pack(anchor="w", padx=8)

        # If the listener was started by the CLI launcher in this same process,
        # attach to the running reader so the UI can control it (and edit host/port).
        try:
            import socket_listener as _sl
            launched_reader = getattr(_sl, "_launched_reader", None)
            launched_thread = getattr(_sl, "_launched_thread", None)
            if launched_reader is not None:
                # reflect current host/port in UI fields
                try:
                    self.listen_host_var.set(getattr(launched_reader, "host", self.listen_host_var.get()))
                    self.listen_port_var.set(str(getattr(launched_reader, "port", self.listen_port_var.get())))
                except Exception:
                    pass
                self._listener_reader = launched_reader
                # attach thread if available (may be None)
                self._listener_thread = launched_thread
                lh = getattr(launched_reader, "host", os.getenv("SN28_LISTENER_HOST", self.listen_host_var.get()))
                lp = getattr(launched_reader, "port", os.getenv("SN28_LISTENER_PORT", self.listen_port_var.get()))
                self.listener_status.set(f"Listener: running (launcher) on {lh}:{lp}")
        except Exception:
            # fall back to env var only when module not accessible
            if os.getenv("SN28_LISTENER_RUNNING") == "1":
                lh = os.getenv("SN28_LISTENER_HOST", self.listen_host_var.get())
                lp = os.getenv("SN28_LISTENER_PORT", self.listen_port_var.get())
                self.listener_status.set(f"Listener: running (launcher) on {lh}:{lp}")

    # ----- small helper -----
    def _require_session(self) -> Optional[RaceSession]:
        s = self.current_session()
        if s is None:
            self.status.set("No session selected.")
        return s

    # ----- Listener control -----
    def start_listener(self):
        # If the launcher already started a listener in another process, avoid double-starts.
        if os.getenv("SN28_LISTENER_RUNNING") == "1":
            # If the listener was launched in this same process and exposed, allow control.
            try:
                import socket_listener as _sl
                if not getattr(_sl, "_launched_reader", None):
                    messagebox.showinfo(
                        "Listener",
                        "A listener is already running (started by the launcher).\n\n"
                        "If you want to control it from here, start the UI via 'sn28 ui' or stop the external listener first."
                    )
                    return
            except Exception:
                messagebox.showinfo(
                    "Listener",
                    "A listener is already running (started by the launcher).\n\n"
                    "If you want to control it from here, start the UI via 'sn28 ui' or stop the external listener first."
                )
                return
        if self._listener_thread and self._listener_thread.is_alive():
            messagebox.showinfo("Listener", "Listener already running.")
            return
        host = (self.listen_host_var.get() or "127.0.0.1").strip()
        try:
            port = int(self.listen_port_var.get() or "50000")
        except ValueError:
            messagebox.showerror("Port", "Port must be an integer.")
            return
        # save settings immediately on start
        try:
            self._save_settings(host, str(port))
        except Exception:
            pass

        parser = OrbitsParser()
        ingestor = DBIngestor(SessionLocal)
        reader = OrbitsTCPReader(
            host=host,
            port=port,
            parser=parser,
            ingestor=ingestor,
            connect_timeout=5.0,
            read_timeout=5.0,
            max_backoff=10.0,
        )
        self._listener_reader = reader

        def _run():
            try:
                # Run the reader loop (blocking) in background thread.
                reader.run()
            finally:
                # Update UI from the Tk main thread to avoid thread-safety issues.
                try:
                    self.after(0, lambda: self.listener_status.set("Listener: stopped"))
                except Exception:
                    pass

        th = threading.Thread(target=_run, daemon=True)
        th.start()
        self._listener_thread = th
        # Set status from the UI thread only.
        self.listener_status.set(f"Listener: running on {host}:{port}")

    def stop_listener(self):
        if self._listener_reader:
            try:
                self._listener_reader.stop()
            except Exception:
                pass
        if self._listener_thread:
            try:
                self._listener_thread.join(timeout=1.0)
            except Exception:
                pass
        self._listener_thread = None
        self._listener_reader = None
        self.listener_status.set("Listener: stopped")

    # ----- Auto-refresh (UI polling) -----
    def _on_auto_refresh_toggle(self) -> None:
        """Called when the Auto Refresh checkbutton is toggled."""
        if self.auto_refresh_var.get():
            self._start_auto_refresh()
            try:
                self.status.set("Auto-refresh: ON")
            except Exception:
                pass
        else:
            self._stop_auto_refresh()
            try:
                self.status.set("Auto-refresh: OFF")
            except Exception:
                pass

    def _start_auto_refresh(self) -> None:
        """Begin scheduling periodic refreshes every 500 ms."""
        if getattr(self, "_auto_refresh_job", None) is not None:
            return
        # schedule immediately
        self._schedule_auto_refresh()

    def _schedule_auto_refresh(self) -> None:
        """Invoke refresh_all and schedule next invocation in 500ms."""
        try:
            # Keep this resilient to exceptions in refresh
            self.refresh_all()
        except Exception:
            pass
        try:
            self._auto_refresh_job = self.after(500, self._schedule_auto_refresh)
        except Exception:
            self._auto_refresh_job = None

    def _stop_auto_refresh(self) -> None:
        """Cancel the scheduled auto-refresh if present."""
        if getattr(self, "_auto_refresh_job", None) is not None:
            try:
                # after_cancel expects the id returned by `after`; cast to satisfy static checkers
                self.after_cancel(cast(str, self._auto_refresh_job))
            except Exception:
                pass
            self._auto_refresh_job = None

    def on_close(self):
        try:
            # persist latest settings
            try:
                self._save_settings(self.listen_host_var.get(), self.listen_port_var.get())
            except Exception:
                pass
            self.stop_listener()
            # ensure background publishers drain before exit
            try:
                from socket_listener import stop_publish_worker
                stop_publish_worker(timeout=3.0)
            except Exception:
                pass
        except Exception:
            pass
        self.destroy()

    # ----- Settings persistence -----
    def _settings_file(self) -> Path:
        base = os.getenv("APPDATA")
        if base:
            base_path = Path(base) / "SuperNats28"
        else:
            base_path = Path.home() / ".supernats28"
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path / "settings.json"

    def _load_settings(self) -> Dict[str, str]:
        try:
            p = self._settings_file()
            if p.exists():
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return {k: str(v) for k, v in data.items()}
        except Exception:
            pass
        return {}

    def _save_settings(self, host: str, port: str) -> None:
        data = {
            "orbits_host": (host or "127.0.0.1").strip(),
            "orbits_port": (port or "50000").strip(),
            # persist whether the user wants auto-refresh enabled
            "auto_refresh": str(bool(getattr(self, "auto_refresh_var", tk.BooleanVar(value=True)).get())),
        }
        p = self._settings_file()
        with p.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    # ----- UI actions -----
    def refresh_sessions(self):
        sessions = list_sessions()
        self._sessions = sessions
        labels = [
            f"{s.id} | {getattr(s.race_class, 'name', '?')} | {s.session_type} | {s.session_name or ''}"
            for s in sessions
        ]
        self.session_cb["values"] = labels
        if labels:
            self.session_cb.current(0)
            self.on_session_change(None)
        else:
            self.status.set("No sessions found. Start the socket listener to ingest data.")

    def current_session(self) -> Optional[RaceSession]:
        i = self.session_cb.current()
        if i < 0:
            return None
        # guard if sessions changed and index is stale
        if i >= len(self._sessions):
            return None
        return self._sessions[i]

    def on_session_change(self, _):
        self.refresh_all()

    def refresh_all(self):
        self.refresh_provisional()
        self.refresh_penalties()
        self.refresh_preview()

    def refresh_provisional(self):
        for i in self.prov_tv.get_children():
            self.prov_tv.delete(i)

        sess = self._require_session()
        if not sess:
            return
        sid = sess.id

        with SessionLocal() as db:
            rows = get_latest_provisional(db, sid)

        for r in rows:
            self.prov_tv.insert(
                "", "end",
                values=(
                    r["pos"] or "",
                    r["num"] or "",
                    r["name"] or "",
                    ms_fmt(r["best_ms"]),
                    ms_fmt(r["last_ms"]),
                )
            )

        # (auto-size disabled per request)

    def refresh_penalties(self):
        for i in self.pen_tv.get_children():
            self.pen_tv.delete(i)

        sess = self._require_session()
        if not sess:
            return
        sid = sess.id

        with SessionLocal() as db:
            pens = (
                db.query(Penalty)
                  .filter(Penalty.session_id == sid)
                  .order_by(Penalty.created_at.asc())
                  .all()
            )
            for p in pens:
                if p.type == "POSITION":
                    val = f"-{p.value_positions or 0} pos"
                elif p.type == "TIME":
                    sec = (p.value_ms or 0) / 1000.0
                    val = f"+{sec:.0f}s"
                elif p.type == "LAP_INVALID":
                    val = f"Lap {p.lap_no}"
                else:
                    val = "DQ" if p.type == "DQ" else ""
                self.pen_tv.insert(
                    "", "end", iid=str(p.id),
                    values=(
                        p.driver_id,
                        p.type,
                        val,
                        p.note or "",
                        p.created_at.strftime("%H:%M:%S") if p.created_at else ""
                    )
                )

    def refresh_preview(self):
        for i in self.prev_tv.get_children():
            self.prev_tv.delete(i)

        sess = self._require_session()
        if not sess:
            return
        sid = sess.id

        with SessionLocal() as db:
            officials = compute_official_order(db, sid)
            # Need kart numbers and names
            num_cache: Dict[int, str] = {}
            name_cache: Dict[int, str] = {}
            for r in officials:
                d = db.get(Driver, r.driver_id)
                if not d:
                    name_cache[r.driver_id] = ""
                    num_cache[r.driver_id] = ""
                    continue
                name_cache[r.driver_id] = f"{(d.first_name or '')} {(d.last_name or '')}".strip()
                num_cache[r.driver_id] = next(
                    (e.number or "" for e in d.entries
                     if e.class_id == sess.class_id and e.event_id == sess.event_id),
                    ""
                )
            for r in officials:
                self.prev_tv.insert(
                    "", "end",
                    values=(
                        r.position or "",
                        num_cache.get(r.driver_id, ""),
                        name_cache.get(r.driver_id, ""),
                        ms_fmt(r.best_lap_ms),
                        ms_fmt(r.last_lap_ms),
                    )
                )

        # (auto-size disabled per request)

    def apply_preset(self, payload: str):
        # payload shapes: "pos:3" | "time:5" | "dq" | "lap:1"
        t = payload.split(":")[0]
        v = payload.split(":")[1] if ":" in payload else ""
        self.type_cb.set("POSITION" if t == "pos" else
                         "TIME" if t == "time" else
                         "DQ" if t == "dq" else
                         "LAP_INVALID")
        self.value_entry.delete(0, tk.END)
        self.value_entry.insert(0, v)
        self.kart_entry.focus_set()

    def add_penalty(self):
        sess = self._require_session()
        if not sess:
            messagebox.showwarning("No session", "Select a session first.")
            return
        sid = sess.id

        number = self.kart_entry.get().strip()
        ptype  = self.type_cb.get().strip()
        val    = (self.value_entry.get() or "").strip().lower()
        note   = (self.note_entry.get() or "").strip()

        if not number:
            messagebox.showwarning("Missing", "Enter a kart number.")
            return

        with SessionLocal() as db:
            drv_id = kart_to_driver_id(db, sess, number)
            if not drv_id:
                messagebox.showerror("Unknown kart", f"No entry found for #{number} in this class/event.")
                return

            pen = Penalty(session_id=sid, driver_id=drv_id, type=ptype, note=note, source="Stewards")
            if ptype == "POSITION":
                try:
                    pen.value_positions = int(val or "0")
                except Exception:
                    messagebox.showerror("Value", "Position drop must be an integer.")
                    return
            elif ptype == "TIME":
                # accept 5 or 5s
                if val.endswith("s"):
                    val = val[:-1]
                try:
                    pen.value_ms = int(float(val or "0") * 1000)
                except Exception:
                    messagebox.showerror("Value", "Time must be a number (seconds).")
                    return
            elif ptype == "LAP_INVALID":
                # Support special keyword 'best' to invalidate driver's best lap
                if val in ("best", "fast", "bestlap", "fastlap"):
                    best_lap = (
                        db.query(Lap)
                          .filter(Lap.session_id == sid,
                                  Lap.driver_id == drv_id,
                                  Lap.is_valid == True,
                                  Lap.lap_time_ms.isnot(None))
                          .order_by(Lap.lap_time_ms.asc(), Lap.lap_number.asc())
                          .first()
                    )
                    if not best_lap:
                        messagebox.showerror("Value", "No valid laps found for this driver to invalidate.")
                        return
                    pen.lap_no = int(best_lap.lap_number)
                else:
                    try:
                        pen.lap_no = int(val or "0")
                    except Exception:
                        messagebox.showerror("Value", "Lap # must be an integer.")
                        return

            db.add(pen)
            db.commit()

        # clear inputs and refresh
        self.kart_entry.delete(0, tk.END)
        self.value_entry.delete(0, tk.END)
        self.note_entry.delete(0, tk.END)
        self.refresh_penalties()
        self.refresh_preview()
        self.status.set(f"Added {ptype} to kart #{number}")

    def delete_selected_penalty(self):
        sel = self.pen_tv.selection()
        if not sel:
            return
        try:
            pid = int(sel[0])
        except Exception:
            return
        with SessionLocal() as db:
            p = db.get(Penalty, pid)
            if p:
                db.delete(p)
                db.commit()
        self.refresh_penalties()
        self.refresh_preview()
        self.status.set("Penalty removed.")

    def clear_all_penalties(self):
        if not messagebox.askyesno("Confirm", "Delete ALL penalties for this session?"):
            return
        sess = self._require_session()
        if not sess:
            return
        sid = sess.id
        with SessionLocal() as db:
            db.query(Penalty).filter(Penalty.session_id == sid).delete()
            db.commit()
        self.refresh_penalties()
        self.refresh_preview()
        self.status.set("All penalties cleared for this session.")

    def publish_official(self):
        sess = self._require_session()
        if not sess:
            messagebox.showwarning("No session", "Select a session first.")
            return
        sid = sess.id
        
        # Check if Google Sheets is configured
        if not CFG.google.spreadsheet_id:
            messagebox.showerror("Config Error", 
                "Google Sheets not configured!\n\n"
                "Set GS_SPREADSHEET_ID in .env file or environment variables.")
            return
        
        if not (CFG.google.service_json_path or CFG.google.service_json_raw):
            messagebox.showerror("Config Error", 
                "Google service account not configured!\n\n"
                "Set GS_SERVICE_JSON_PATH in .env file or environment variables.")
            return
        
        try:
            # Write official results + award points (transaction inside)
            with SessionLocal() as db:
                write_official_and_award_points(db, sid, scheme_name=CFG.app.points_scheme)

            # Push to Sheets (guard with CFG publishing toggles if you like)
            msg_parts = []
            if CFG.app.publish_results:
                # Single-tab results for the current class (Heat/Qual only)
                publish_class_results_view(sid)
                msg_parts.append("results")
                # Optionally maintain normalized Raw tabs if enabled
                if getattr(CFG.app, "publish_raw_tabs", False):
                    try:
                        publish_raw_results(sid)
                        if CFG.app.publish_points:
                            publish_raw_points(sid)
                    except Exception:
                        pass

            # Optional: publish prefinal grid when ready (usually after all heats):
            # publish_raw_prefinal_grid(sess.class_id, sess.event_id)

            self.refresh_all()
            published = " and ".join(msg_parts) if msg_parts else "nothing (all toggles off)"
            messagebox.showinfo("Published", f"Official {published} pushed to Sheets.")
            self.status.set(f"Published official + pushed {published} to Sheets.")
        except Exception as e:
            import traceback
            messagebox.showerror("Error", f"Publish failed:\n{e}\n\n{traceback.format_exc()}")

    def publish_heat_totals_for_class(self):
        sess = self._require_session()
        if not sess:
            messagebox.showwarning("No session", "Select a session first.")
            return
        if not CFG.google.spreadsheet_id:
            messagebox.showerror("Config Error", "Google Sheets not configured! Set GS_SPREADSHEET_ID.")
            return
        if not (CFG.google.service_json_path or CFG.google.service_json_raw):
            messagebox.showerror("Config Error", "Google service account not configured! Set GS_SERVICE_JSON_PATH or GS_SERVICE_JSON_RAW.")
            return
        try:
            from sheets_publish import publish_raw_heat_totals
            publish_class_heat_totals_view(sess.class_id, sess.event_id)
            if getattr(CFG.app, "publish_raw_tabs", False):
                try:
                    publish_raw_heat_totals(sess.class_id, sess.event_id)
                except Exception:
                    pass
            messagebox.showinfo("Published", "Heat totals published and class views updated.")
            self.status.set("Heat totals published for class.")
        except Exception as e:
            import traceback
            messagebox.showerror("Error", f"Publish Heat Totals failed:\n{e}\n\n{traceback.format_exc()}")

    def publish_prefinal_for_class(self):
        sess = self._require_session()
        if not sess:
            messagebox.showwarning("No session", "Select a session first.")
            return
        if not CFG.google.spreadsheet_id:
            messagebox.showerror("Config Error", "Google Sheets not configured! Set GS_SPREADSHEET_ID.")
            return
        if not (CFG.google.service_json_path or CFG.google.service_json_raw):
            messagebox.showerror("Config Error", "Google service account not configured! Set GS_SERVICE_JSON_PATH or GS_SERVICE_JSON_RAW.")
            return
        try:
            publish_class_prefinal_view(sess.class_id, sess.event_id)
            if getattr(CFG.app, "publish_raw_tabs", False):
                try:
                    publish_raw_prefinal_grid(sess.class_id, sess.event_id)
                except Exception:
                    pass
            messagebox.showinfo("Published", "Prefinal grid published for this class.")
            self.status.set("Prefinal grid published for class.")
        except Exception as e:
            import traceback
            messagebox.showerror("Error", f"Publish Prefinal Grid failed:\n{e}\n\n{traceback.format_exc()}")

if __name__ == "__main__":
    app = PenaltyApp()
    app.mainloop()

def main():
    app = PenaltyApp()
    app.mainloop()