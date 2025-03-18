from datetime import datetime, timezone
import json
from typing import List

import pandas as pd
import websocket

from observability import get_logger

logger = get_logger(__name__)


class FileWriteDataReturnValue:
    """Custom return object mimicking"""

    def __init__(self, paths: List[str], rows_written: int):
        self.paths = paths  # List of written file paths
        self.rows_written = rows_written  # Number of rows written

    def __repr__(self):
        return f"FileWriteDataReturnValue(paths={self.paths}, rows_written={self.rows_written})"


class BinanceWebSocketClient:
    """Class to handle Binance WebSocket connections for real-time OHLCV data streaming."""

    def __init__(self, symbols: List[str], interval: str = "1m"):
        """
        Initialize the Binance WebSocket client.

        :param symbols: List of trading pairs to subscribe (e.g., ["btcusdt", "ethusdt"])
        :param interval: Time interval for kline data (e.g., "1m", "5m", "1h", etc.)
        """
        self.symbols = [f"{symbol.lower()}@kline_{interval}" for symbol in symbols]
        self.ws = None

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        data = json.loads(message)
        kline = data.get("k", {})

        # Extract relevant fields from the kline data
        timestamp = datetime.fromtimestamp(kline["t"] / 1000, timezone.utc)

        open_price = float(kline["o"])
        high_price = float(kline["h"])
        low_price = float(kline["l"])
        close_price = float(kline["c"])
        volume = float(kline["v"])
        trades = int(kline["n"])
        symbol = data.get("s", "UNKNOWN")

        # Create a DataFrame with the incoming data
        df = pd.DataFrame(
            [
                [
                    symbol,
                    timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    trades,
                ]
            ],
            columns=[
                "symbol",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trades",
            ],
        )

        # Log and process the data
        logger.info(f"New data received for {symbol}: {df.iloc[0].to_dict()}")
        print(df)

        # Further processing (e.g., save to file, database, etc.) can be added here

    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket closure."""
        logger.info(f"WebSocket closed with code {close_status_code}: {close_msg}")

    def on_open(self, ws):
        """Send a subscription message when WebSocket opens."""
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": self.symbols,  # Subscribe to multiple symbols dynamically
            "id": 1,
        }
        ws.send(json.dumps(subscribe_message))
        logger.info(f"Subscribed to {', '.join(self.symbols)}")

    def start_stream(self):
        """Start the Binance WebSocket stream."""
        url = "wss://stream.binance.com:9443/ws"
        self.ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.on_open = self.on_open
        self.ws.run_forever()
