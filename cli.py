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
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run components")
    run.add_argument("--listen", action="store_true", help="Start Orbits TCP listener")
    run.add_argument("--api", action="store_true", help="Start Flask API server")
    run.add_argument("--ui", action="store_true", help="Start Penalty Pad UI")
    run.add_argument("--listen-host", default=os.getenv("ORBITS_HOST", "127.0.0.1"))
    run.add_argument("--listen-port", type=int, default=int(os.getenv("ORBITS_PORT", "50000")))
    run.add_argument("--api-host", default=os.getenv("API_HOST", "127.0.0.1"))
    run.add_argument("--api-port", type=int, default=int(os.getenv("API_PORT", "5000")))

    seed = sub.add_parser("seed", help="Seed points scale")
    seed.add_argument("--field", type=int, default=50)
    seed.add_argument("--bonus-fast-lap", type=int, default=0)
    seed.add_argument("--bonus-pole", type=int, default=0)

    args = ap.parse_args()

    if args.cmd == "seed":
        from points_config import seed_skusa_sn28
        seed_skusa_sn28(field_size=args.field, bonus_fast_lap=args.bonus_fast_lap, bonus_pole=args.bonus_pole)
        print("Seeded SKUSA_SN28 scale")
        return

    if args.cmd == "run":
        threads: list[threading.Thread] = []

        if args.listen:
            t = threading.Thread(target=run_socket_listener, args=(args.listen_host, args.listen_port), daemon=True)
            t.start(); threads.append(t)
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
