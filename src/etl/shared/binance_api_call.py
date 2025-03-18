# import asyncio
# import json
# import logging
# import datetime
# import requests
# import pandas as pd
# import websocket
# from typing import List, Optional
# from dataclasses import dataclass

# # Configure Logging
# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)

# BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
# STREAM_URL = "wss://stream.binance.com:9443/ws"


# @dataclass
# class OHLCVData:
#     timestamp: datetime.datetime
#     open: float
#     high: float
#     low: float
#     close: float
#     volume: float
#     trades: int
#     taker_buy_base: float
#     taker_buy_quote: float
#     symbol: str


# class BinanceAPI:
#     def __init__(self, symbols: List[str], interval: str = "1d", days: int = 30):
#         self.symbols = symbols
#         self.interval = interval
#         self.days = days

#     def fetch_historical_data(self, symbol: str) -> Optional[List[OHLCVData]]:
#         """Fetch historical OHLCV data for a given symbol from Binance API."""
#         try:
#             end_time = int(datetime.datetime.utcnow().timestamp() * 1000)
#             start_time = end_time - (self.days * 24 * 60 * 60 * 1000)

#             params = {
#                 "symbol": symbol,
#                 "interval": self.interval,
#                 "startTime": start_time,
#                 "endTime": end_time,
#                 "limit": 1000,
#             }

#             response = requests.get(BINANCE_API_URL, params=params)
#             response.raise_for_status()

#             data = response.json()
#             ohlcv_list = [
#                 OHLCVData(
#                     timestamp=pd.to_datetime(entry[0], unit="ms", utc=True),
#                     open=float(entry[1]),
#                     high=float(entry[2]),
#                     low=float(entry[3]),
#                     close=float(entry[4]),
#                     volume=float(entry[5]),
#                     trades=int(entry[8]),
#                     taker_buy_base=float(entry[9]),
#                     taker_buy_quote=float(entry[10]),
#                     symbol=symbol,
#                 )
#                 for entry in data
#             ]

#             return ohlcv_list

#         except requests.RequestException as e:
#             logger.error(f"API request failed for {symbol}: {e}")
#             return None

#     def start_stream(self):
#         """Start real-time data stream from Binance WebSocket."""

#         async def connect_websocket(self, symbol: str):
#             """Connects to Binance WebSocket for a given symbol."""
#             uri = f"{STREAM_URL}/{symbol.lower()}@kline_1h"
#             async with ws.connect(uri) as websocket:
#                 logger.info(f"Connected to {uri}")
#                 while True:
#                     try:
#                         message = await websocket.recv()
#                         await self.on_message(symbol, message)
#                     except ws.exceptions.ConnectionClosed:
#                         logger.warning(f"WebSocket closed for {symbol}, reconnecting...")
#                         await asyncio.sleep(5)  # Wait before retrying
#                         return await self

#         def on_message(ws, message):
#             data = json.loads(message)
#             kline = data.get("k", {})
#             ohlcv_data = OHLCVData(
#                 timestamp=pd.to_datetime(kline.get("t"), unit="ms", utc=True),
#                 open=float(kline.get("o")),
#                 high=float(kline.get("h")),
#                 low=float(kline.get("l")),
#                 close=float(kline.get("c")),
#                 volume=float(kline.get("v")),
#                 trades=int(kline.get("n")),
#                 taker_buy_base=float(kline.get("V")),
#                 taker_buy_quote=float(kline.get("Q")),
#                 symbol=data.get("s"),
#             )
#             print(ohlcv_data)  # This can be replaced with database storage

#         def on_error(ws, error):
#             logger.error(f"WebSocket Error: {error}")

#         def on_close(ws, close_status_code, close_msg):
#             logger.info("WebSocket Closed")

#         def on_open(ws):
#             msg = {"method": "SUBSCRIBE", "params": ["btcusdt@kline_1m"], "id": 1}
#             ws.send(json.dumps(msg))
#             logger.info("Subscribed to BTCUSDT stream.")

#         ws = websocket.WebSocketApp(
#             f"{STREAM_URL}/btcusdt@kline_1m",
#             on_message=on_message,
#             on_error=on_error,
#             on_close=on_close,
#         )
#         ws.on_open = on_open
#         ws.run_forever()


# if __name__ == "__main__":
#     binance_api = BinanceAPI(symbols=["BTCUSDT", "ETHUSDT"], interval="1d", days=30)
#     for symbol in binance_api.symbols:
#         historical_data = binance_api.fetch_historical_data(symbol)
#         if historical_data is not None:
#             print(historical_data[:5])  # Display sample data
#     binance_api.start_stream()


import asyncio
import json
import logging
import datetime
import requests
import pandas as pd
import websockets
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


class BinanceAPI:
    def __init__(self, symbols: List[str], interval: str = "1d", days: int = 30):
        self.symbols = symbols
        self.interval = interval
        self.days = days

    def fetch_historical_data(self, symbol: str) -> Optional[List[OHLCVData]]:
        """Fetch historical OHLCV data for a given symbol from Binance API."""
        try:
            end_time = int(datetime.datetime.utcnow().timestamp() * 1000)
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
            ohlcv_list = [
                OHLCVData(
                    timestamp=pd.to_datetime(entry[0], unit="ms", utc=True),
                    open=float(entry[1]),
                    high=float(entry[2]),
                    low=float(entry[3]),
                    close=float(entry[4]),
                    volume=float(entry[5]),
                    trades=int(entry[8]),
                    taker_buy_base=float(entry[9]),
                    taker_buy_quote=float(entry[10]),
                    symbol=symbol,
                )
                for entry in data
            ]

            return ohlcv_list

        except requests.RequestException as e:
            logger.error(f"API request failed for {symbol}: {e}")
            return None

    async def on_message(self, symbol: str, message: str):
        """Handle the incoming WebSocket message."""
        data = json.loads(message)
        kline = data.get("k", {})
        ohlcv_data = OHLCVData(
            timestamp=pd.to_datetime(kline.get("t"), unit="ms", utc=True),
            open=float(kline.get("o")),
            high=float(kline.get("h")),
            low=float(kline.get("l")),
            close=float(kline.get("c")),
            volume=float(kline.get("v")),
            trades=int(kline.get("n")),
            taker_buy_base=float(kline.get("V")),
            taker_buy_quote=float(kline.get("Q")),
            symbol=data.get("s"),
        )
        print(ohlcv_data)  # This can be replaced with database storage

    async def connect_websocket(self, symbol: str):
        """Connects to Binance WebSocket for a given symbol."""
        uri = f"{STREAM_URL}/{symbol.lower()}@kline_1h"
        async with websockets.connect(uri) as websocket:
            logger.info(f"Connected to {uri}")
            while True:
                try:
                    message = await websocket.recv()
                    await self.on_message(symbol, message)
                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"WebSocket closed for {symbol}, reconnecting...")
                    await asyncio.sleep(5)  # Wait before retrying
                    return await self.connect_websocket(symbol)

    async def start_stream(self):
        """Start real-time data stream from Binance WebSocket for all symbols."""
        tasks = []
        for symbol in self.symbols:
            tasks.append(self.connect_websocket(symbol))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    binance_api = BinanceAPI(symbols=["BTCUSDT", "ETHUSDT"], interval="1d", days=30)

    # Fetch historical data
    for symbol in binance_api.symbols:
        historical_data = binance_api.fetch_historical_data(symbol)
        if historical_data is not None:
            print(historical_data[:5])  # Display sample data

    # Start WebSocket stream
    asyncio.run(binance_api.start_stream())
