from typing import List
import pandas as pd
import requests
from datetime import datetime, timezone
from src.etl.shared.observability import get_logger

logger = get_logger(__name__)

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
TRADE_API_URL = "https://api.binance.com/api/v3/trades"


def fetch_historical_ohlcv(
    symbols: List[str], interval: str = "1h", days: int = 30
) -> pd.DataFrame:
    """
    This method fetch historical OHLCV (Open, High, Low, Close, Volume) data for multiple symbols from Binance API.

    :param symbols: List of trading pairs (e.g., ["BTCUSDT", "ETHUSDT"]).
    :param interval: Timeframe for candles (e.g., "1m", "1h", "1d").
    :param days: Number of days of historical data to fetch.
    :return: Pandas DataFrame containing OHLCV data for all symbols.
    """
    end_time = int(datetime.now(timezone.utc) * 1000)  # Current time in ms
    start_time = end_time - (days * 24 * 60 * 60 * 1000)  # Days before

    all_data = []

    for symbol in symbols:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 10000,  # Max records per request
        }

        try:
            response = requests.get(BINANCE_API_URL, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                logger.warning(f"No data returned for {symbol}")
                continue

            # Convert to DataFrame
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

            # Convert timestamp to readable datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

            # retaining only relevant columns
            df = df[["timestamp", "open", "high", "low", "close", "volume", "trades"]]
            df["symbol"] = symbol  # Add symbol(digital currency) for identification

            all_data.append(df)
            logger.info(f"Successful data fetch for {symbol}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            continue

    # Concatenate all symbols' data into a single DataFrame
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data was fetched

