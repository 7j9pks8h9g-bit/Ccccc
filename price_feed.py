import json
import time
import threading
from typing import Optional
from collections import deque

import websocket

from ..config import POLYMARKET_WS_URL, BINANCE_WS_URL, log
from ..models import PriceData


class ChainlinkPriceFeed:
    """
    Connects to Polymarket RTDS WebSocket for real-time
    Chainlink BTC/USD prices (same source Polymarket uses for resolution).

    - current_price / price_history: ONLY Chainlink (Binance is used as
      current_price only if Chainlink is silent for >30s).
    - chainlink_ticks: raw Chainlink ticks with exchange timestamps, used to
      get the exact price at a window boundary (= Polymarket "Price to beat").

    Also runs a secondary Binance trade stream for 1s-resolution
    tick data used by feature engineering (stored in hf_history).
    """

    CHAINLINK_STALE_SEC = 30.0

    def __init__(self):
        self.current_price: float = 0.0
        self.current_source: str = "connecting..."
        self.price_history: deque = deque(maxlen=600)
        # High-frequency price history from Binance trades (~1s resolution)
        self.hf_history: deque = deque(maxlen=3600)
        self._last_hf_append: float = 0
        self.last_update: float = 0.0
        self.connected: bool = False
        self.ws: Optional[websocket.WebSocketApp] = None
        self._lock = threading.Lock()
        self._reconnect_delay = 2
        # Only Chainlink ticks: (exchange_timestamp_seconds, price)
        self.chainlink_ticks: deque = deque(maxlen=2000)
        self.last_chainlink_update: float = 0.0

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True).start()
        threading.Thread(target=self._binance_hf_loop, daemon=True).start()

    def _connect_loop(self):
        while True:
            try:
                log.info(f"Connecting to Polymarket RTDS: {POLYMARKET_WS_URL}")
                self.ws = websocket.WebSocketApp(
                    POLYMARKET_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=5, ping_timeout=3)
            except Exception as e:
                log.error(f"WebSocket error: {e}")
            finally:
                # Close previous WS to release the socket before looping.
                try:
                    if self.ws is not None:
                        self.ws.close()
                except Exception:
                    pass
                self.ws = None
            self.connected = False
            log.info(f"Reconnecting in {self._reconnect_delay}s...")
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(30, self._reconnect_delay * 1.5)

    def _on_open(self, ws):
        self.connected = True
        self._reconnect_delay = 2
        log.info("Connected to Polymarket RTDS WebSocket")
        chainlink_sub = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*",
                 "filters": '{"symbol":"btc/usd"}'}
            ],
        }
        ws.send(json.dumps(chainlink_sub))
        log.info("Subscribed to Chainlink BTC/USD")
        binance_sub = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices", "type": "update", "filters": "btcusdt"}
            ],
        }
        ws.send(json.dumps(binance_sub))
        log.info("Subscribed to Binance BTC/USDT as backup")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except (json.JSONDecodeError, ValueError):
            return  # Non-JSON frame (handshake, ack) — ignore silently
        try:
            topic = data.get("topic", "")
            payload = data.get("payload", {})
            if not payload or not isinstance(payload, dict):
                return
            price = 0.0
            source = ""
            if topic == "crypto_prices_chainlink":
                # Accept any message type (update / snapshot) for btc/usd
                if payload.get("symbol", "") == "btc/usd":
                    price = float(payload.get("value", 0) or 0)
                    source = "chainlink"
            elif topic == "crypto_prices":
                if payload.get("symbol", "") == "btcusdt":
                    price = float(payload.get("value", 0) or 0)
                    source = "binance"

            if price <= 0:
                return

            ts = float(payload.get("timestamp", time.time() * 1000)) / 1000.0
            now = time.time()
            with self._lock:
                updated_current = False
                if source == "chainlink":
                    # Keep ticks ordered by exchange timestamp
                    if not self.chainlink_ticks or ts >= self.chainlink_ticks[-1][0]:
                        self.chainlink_ticks.append((ts, price))
                    self.last_chainlink_update = now
                    self.current_price = price
                    self.current_source = "chainlink"
                    updated_current = True
                    # Chart/indicator history: ONLY Chainlink
                    self.price_history.append(
                        PriceData(price=price, timestamp=ts, source=source)
                    )
                elif source == "binance":
                    # Binance only as emergency fallback when Chainlink is silent
                    if now - self.last_chainlink_update > self.CHAINLINK_STALE_SEC:
                        self.current_price = price
                        self.current_source = "binance (fallback)"
                        updated_current = True

                # last_update only moves when current_price actually changed
                if updated_current:
                    self.last_update = now
        except Exception as e:
            log.error(f"Message parse error: {e}")

    def _on_error(self, ws, error):
        log.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        log.warning(f"WebSocket closed: {close_status_code} {close_msg}")

    # ── Public getters ────────────────────────────────────────

    def get_price(self) -> Optional[PriceData]:
        """Current price (Chainlink, or Binance fallback if Chainlink silent >30s)."""
        with self._lock:
            if self.current_price > 0:
                return PriceData(
                    price=self.current_price,
                    timestamp=self.last_update,
                    source=self.current_source,
                )
            return None

    def get_chainlink_price(self) -> Optional[PriceData]:
        """Latest pure Chainlink price (never Binance). timestamp = exchange time."""
        with self._lock:
            if self.chainlink_ticks:
                ts, price = self.chainlink_ticks[-1]
                return PriceData(price=price, timestamp=ts, source="chainlink")
            return None

    def get_chainlink_price_at(self, target_ts: float,
                               max_gap: float = 5.0) -> Optional[PriceData]:
        """Chainlink price at target_ts (how Polymarket sets Price to beat / close).

        Returns the first tick with exchange timestamp >= target_ts.
        None if that tick has not arrived yet, or is more than max_gap
        seconds after target_ts.
        """
        with self._lock:
            for ts, price in self.chainlink_ticks:
                if ts >= target_ts:
                    if ts - target_ts <= max_gap:
                        return PriceData(price=price, timestamp=ts, source="chainlink")
                    return None
            return None

    def get_recent_prices(self, n: int = 60) -> list[float]:
        with self._lock:
            prices = list(self.price_history)
            return [p.price for p in prices[-n:]]

    def get_binance_price(self) -> Optional[PriceData]:
        """Latest Binance spot price (features only — NOT the Polymarket oracle).

        Uses the HF trade stream (~1s resolution) as primary source.
        Falls back to the main WS Binance price if HF is empty.
        Returns None if no Binance data is available.
        """
        with self._lock:
            if self.hf_history:
                return PriceData(
                    price=self.hf_history[-1],
                    timestamp=self._last_hf_append,
                    source="binance_spot",
                )
            if "binance" in self.current_source.lower() and self.current_price > 0:
                return PriceData(
                    price=self.current_price,
                    timestamp=self.last_update,
                    source="binance_ws",
                )
            return None

    def get_hf_prices(self, n: int = 3600) -> list[float]:
        """Get high-frequency prices from Binance trade stream (~1s intervals)."""
        with self._lock:
            return list(self.hf_history)[-n:]

    def get_status(self) -> dict:
        with self._lock:
            now = time.time()
            age = now - self.last_update if self.last_update else 999
            cl_age = (now - self.last_chainlink_update
                      if self.last_chainlink_update else 999)
            return {
                "connected": self.connected,
                "source": self.current_source,
                "price": self.current_price,
                "last_update_age": round(age, 1),
                "chainlink_age": round(cl_age, 1),
                "history_size": len(self.price_history),
                "chainlink_ticks": len(self.chainlink_ticks),
                "hf_history_size": len(self.hf_history),
            }

    # ── Binance high-frequency trade stream ──────────────────

    def _binance_hf_loop(self):
        """Connect to Binance trade stream for ~1s tick resolution."""
        while True:
            ws = None
            try:
                log.info(f"Connecting to Binance HF stream: {BINANCE_WS_URL}")
                ws = websocket.WebSocketApp(
                    BINANCE_WS_URL,
                    on_message=self._on_hf_message,
                    on_error=lambda ws, e: log.debug(f"Binance HF error: {e}"),
                    on_close=lambda ws, c, m: log.debug("Binance HF closed"),
                    on_open=lambda ws: log.info("Binance HF stream connected"),
                )
                ws.run_forever(ping_interval=10, ping_timeout=5)
            except Exception as e:
                log.debug(f"Binance HF connection error: {e}")
            finally:
                try:
                    if ws is not None:
                        ws.close()
                except Exception:
                    pass
            time.sleep(3)

    def _on_hf_message(self, ws, message):
        """Handle Binance trade messages. Throttle to ~1s."""
        try:
            data = json.loads(message)
            # Combined streams wrap the payload in "data"
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
                data = data["data"]
            price = float(data.get("p", 0) or 0)
            if price <= 0:
                return
            now = time.time()
            with self._lock:
                if now - self._last_hf_append >= 1.0:
                    self.hf_history.append(price)
                    self._last_hf_append = now
        except Exception:
            pass
