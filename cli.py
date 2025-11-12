from __future__ import annotations
import argparse
import os
import threading
import logging


def run_socket_listener(host: str, port: int):
    from socket_listener import OrbitsTCPReader, OrbitsParser, DBIngestor
    from db import SessionLocal, init_db
    init_db()
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
    # If the UI is running in the same process, expose the reader so the UI can control it
    try:
        import socket_listener as _sl
        _sl._launched_reader = reader
    except Exception:
        pass
    reader.run()


def run_api(host: str, port: int):
    # app imports Flask app at module import time
    from app import app  # type: ignore
    app.run(host=host, port=port, debug=False, use_reloader=False)


def run_ui():
    from ui import PenaltyApp
    app = PenaltyApp()
    app.mainloop()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="sn28", description="SuperNats 28 Points Toolkit")
    # Do not require subcommand; we'll default to 'run' with sensible defaults when absent
    sub = ap.add_subparsers(dest="cmd")

    run = sub.add_parser("run", help="Run components")
    run.add_argument("--listen", action="store_true", help="Start Orbits TCP listener")
    run.add_argument("--api", action="store_true", help="Start Flask API server")
    run.add_argument("--ui", action="store_true", help="Start Penalty Pad UI")
    run.add_argument("--listen-host", default=os.getenv("ORBITS_HOST", "127.0.0.1"))
    run.add_argument("--listen-port", type=int, default=int(os.getenv("ORBITS_PORT", "50000")))
    run.add_argument("--api-host", default=os.getenv("API_HOST", "127.0.0.1"))
    run.add_argument("--api-port", type=int, default=int(os.getenv("API_PORT", "5000")))

    # Convenience subcommands
    sub.add_parser("ui", help="Launch Penalty Pad UI only")

    listen = sub.add_parser("listen", help="Start Orbits TCP listener only")
    listen.add_argument("--host", default=os.getenv("ORBITS_HOST", "127.0.0.1"))
    listen.add_argument("--port", type=int, default=int(os.getenv("ORBITS_PORT", "50000")))

    api = sub.add_parser("api", help="Start Flask API server only")
    api.add_argument("--host", default=os.getenv("API_HOST", "127.0.0.1"))
    api.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "5000")))

    seed = sub.add_parser("seed", help="Seed points scale")
    seed.add_argument("--field", type=int, default=50)
    seed.add_argument("--bonus-fast-lap", type=int, default=0)
    seed.add_argument("--bonus-pole", type=int, default=0)

    args = ap.parse_args()

    # Default behavior: if no subcommand provided, behave like: sn28 run --listen --ui
    if not getattr(args, "cmd", None):
        args.cmd = "run"
        # synthesize attributes used by 'run'
        
        args.listen = False
        args.ui = True
        args.api = False
        args.listen_host = os.getenv("ORBITS_HOST", "127.0.0.1")
        args.listen_port = int(os.getenv("ORBITS_PORT", "50000"))
        args.api_host = os.getenv("API_HOST", "127.0.0.1")
        args.api_port = int(os.getenv("API_PORT", "5000"))

    if args.cmd == "seed":
        from points_config import seed_skusa_sn28
        seed_skusa_sn28(field_size=args.field, bonus_fast_lap=args.bonus_fast_lap, bonus_pole=args.bonus_pole)
        print("Seeded SKUSA_SN28 scale")
        return

    if args.cmd == "ui":
        run_ui(); return

    if args.cmd == "listen":
        run_socket_listener(host=args.host, port=args.port); return

    if args.cmd == "api":
        run_api(host=args.host, port=args.port); return

    if args.cmd == "run":
        threads: list[threading.Thread] = []

        # If user specified 'run' but no components toggled, default to UI + listener
        if not (args.listen or args.api or args.ui):
            args.listen = True
            args.ui = True

        if args.listen:
            # Mark environment so the UI (running in the same process) knows a listener is already active
            os.environ["SN28_LISTENER_RUNNING"] = "1"
            os.environ["SN28_LISTENER_HOST"] = str(args.listen_host)
            os.environ["SN28_LISTENER_PORT"] = str(args.listen_port)
            t = threading.Thread(target=run_socket_listener, args=(args.listen_host, args.listen_port), daemon=True)
            t.start(); threads.append(t)
            # Expose the launcher thread to the socket_listener module so UI can attach
            try:
                import socket_listener as _sl
                _sl._launched_thread = t
            except Exception:
                pass
            logging.info("Listener thread started on %s:%s", args.listen_host, args.listen_port)

        if args.api:
            t = threading.Thread(target=run_api, args=(args.api_host, args.api_port), daemon=True)
            t.start(); threads.append(t)
            logging.info("API server starting on http://%s:%s", args.api_host, args.api_port)

        # UI should run on main thread if requested
        if args.ui:
            run_ui()
        else:
            # If no UI, keep process alive if any threads running
            try:
                for t in threads:
                    t.join()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
