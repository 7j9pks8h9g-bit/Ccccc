"""
BTC UP/DOWN 5-MIN PREDICTOR v3 — Polymarket Style

Entry point. Run with: python app.py
Dashboard: http://localhost:5000
"""

import os
import threading
import time


def _keep_alive():
    """Render free tier sleeps after 15 min without inbound traffic.
    Ping our own public URL every 10 min so the bot keeps running."""
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        return
    import requests
    while True:
        time.sleep(600)
        try:
            requests.get(url.rstrip("/") + "/healthz", timeout=20)
        except Exception:
            pass

from btc5min.logging_setup import install_terminal_logging
from btc5min.routes import create_app, get_engine


def main():
    install_terminal_logging()
    print(r"""
    ╔══════════════════════════════════════════════════════════════╗
    ║         BTC UP/DOWN 5-MIN PREDICTOR v3                       ║
    ║         Chainlink BTC/USD -> Polymarket RTDS WebSocket       ║
    ║         AI: Agnóstico Multi-Algoritmo                        ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Dashboard:  http://localhost:5000                           ║
    ║  API State:  http://localhost:5000/api/state                 ║
    ║  Data feed:  wss://ws-live-data.polymarket.com               ║
    ║  Source:     Chainlink BTC/USD Data Stream (same as PM)      ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    print("  📡 Conectando a Chainlink BTC/USD vía Polymarket RTDS...\n")

    engine = get_engine()
    engine.start()
    app = create_app()
    threading.Thread(target=_keep_alive, daemon=True).start()
    # Default: only this computer. Set BTC5MIN_HOST=0.0.0.0 only behind a firewall.
    host = os.environ.get("BTC5MIN_HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "5000"))
    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
