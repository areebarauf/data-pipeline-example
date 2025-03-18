import json
import logging
import datetime
import requests
import pandas as pd
import websocket
from typing import List, Optional
from dataclasses import dataclass

# Configure Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
STREAM_URL = "wss://stream.binance.com:9443/ws"


@dataclass
class OHLCVData:
    timestamp: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    taker_buy_base: float
    taker_buy_quote: float
    symbol: str


class BinanceETL:
    def __init__(self, symbols: List[str], interval: str = "1d", days: int = 30):
        """
        Args:
            interval: define the granuality between data values
        """
        self.symbols = symbols
        self.interval = interval
        self.days = days

    def fetch_historical_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch historical OHLCV data for a given symbol from Binance API."""
        try:
            end_time = int(datetime.now(datetime.timezone.utc) * 1000)
            start_time = end_time - (self.days * 24 * 60 * 60 * 1000)

            params = {
                "symbol": symbol,
                "interval": self.interval,
                "startTime": start_time,
                "endTime": end_time,
                "limit": 1000,
            }

            response = requests.get(BINANCE_API_URL, params=params)
            response.raise_for_status()

            data = response.json()
            df = pd.DataFrame(
                data,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "trades",
                    "taker_buy_base",
                    "taker_buy_quote",
                    "ignore",
                ],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df.drop(
                columns=["close_time", "quote_asset_volume", "ignore"], inplace=True
            )
            df["symbol"] = symbol

            return self.clean_data(df)

        except requests.RequestException as e:
            logger.error(f"API request failed for {symbol}: {e}")
            return None

    def start_stream(self):
        """Start real-time data stream from Binance WebSocket."""

        def on_message(ws, message):
            data = json.loads(message)
            kline = data.get("k", {})
            df = pd.DataFrame(
                [
                    [
                        pd.to_datetime(kline.get("t"), unit="ms", utc=True),
                        kline.get("o"),
                        kline.get("h"),
                        kline.get("l"),
                        kline.get("c"),
                        kline.get("v"),
                        kline.get("n"),
                        kline.get("V"),
                        kline.get("Q"),
                        data.get("s"),
                    ]
                ],
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "trades",
                    "taker_buy_base",
                    "taker_buy_quote",
                    "symbol",
                ],
            )
            df = self.clean_data(df)
            df = self.anonymize_data(df)

        def on_error(ws, error):
            logger.error(f"WebSocket Error: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.info("WebSocket Closed")

        def on_open(ws):
            msg = {"method": "SUBSCRIBE", "params": ["btcusdt@kline_1m"], "id": 1}
            ws.send(json.dumps(msg))
            logger.info("Subscribed to BTCUSDT stream.")

        ws = websocket.WebSocketApp(
            f"{STREAM_URL}/btcusdt@kline_1m",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.on_open = on_open
        ws.run_forever()


if __name__ == "__main__":
    etl = BinanceETL(symbols=["BTCUSDT", "ETHUSDT"], interval="1d", days=30)
    for symbol in etl.symbols:
        df = etl.fetch_historical_data(symbol)
        if df is not None:
            df = etl.anonymize_data(df)
            etl.save_to_parquet(df, f"{symbol}_ohlcv.parquet")
    etl.start_stream()
