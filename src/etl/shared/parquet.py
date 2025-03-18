import pyarrow.parquet as pq
import pyarrow as pa
import requests
import pandas as pd
from datetime import datetime
from observability import get_logger


logger = get_logger(__name__)

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"


def fetch_historical_ohlcv(symbol="BTCUSDT", interval="1d", days=30):
    """Fetch historical OHLCV data for a given symbol."""

    end_time = int(datetime.utcnow().timestamp() * 1000)  # Current time in ms
    start_time = end_time - (days * 24 * 60 * 60 * 1000)  # 30 days before

    params = {
        "symbol": symbol,
        "interval": interval,  # 1d = daily candles
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000,  # Max records per request
    }

    response = requests.get(BINANCE_API_URL, params=params)

    if response.status_code != 200:
        logger.error(f"Error fetching Binance OHLCV: {response.text}")
        return None

    data = response.json()

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

    # Convert timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df[
        [
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
        ]
    ]
    df["symbol"] = symbol

    return df


def fetch_and_store_binance_data(
    symbol: str,
    interval: str,
    destination_path: str,
    limit: int = 100,
    partition_cols: List[str] = None,
) -> FileWriteDataReturnValue:
    """
    Fetches Binance OHLCV data and stores it as a Parquet file.

    :param symbol: Trading pair (e.g., "BTCUSDT").
    :param interval: Timeframe (e.g., "1d").
    :param destination_path: Where to store the Parquet file.
    :param limit: Number of records to fetch.
    :param partition_cols: List of partition columns (optional).
    :return: FileWriteDataReturnValue object.
    """
    df = fetch_binance_ohlcv(symbol, interval, limit)
    return write_parquet(df, destination_path, partition_cols)


def fetch_historical_trades(symbol="BTCUSDT", limit=1000):
    """Fetch historical trades for a given symbol."""

    TRADE_API_URL = "https://api.binance.com/api/v3/trades"
    params = {"symbol": symbol, "limit": limit}

    response = requests.get(TRADE_API_URL, params=params)

    if response.status_code != 200:
        print(f"Error fetching Binance trades: {response.text}")
        return None

    trades = response.json()

    df = pd.DataFrame(trades)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["symbol"] = symbol

    return df[["time", "price", "qty", "symbol"]]


def save_to_parquet(df, filename="binance_data.parquet"):
    """Save DataFrame to Parquet format."""
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    print(f"Data saved to {filename}")


# Fetch BTC & ETH historical data
btc_data = fetch_historical_ohlcv("BTCUSDT", "1d", 120)
eth_data = fetch_historical_ohlcv("ETHUSDT", "1d", 120)

# Merge both
historical_df = pd.concat([btc_data, eth_data], ignore_index=True)

# Fetch recent trades for BTC
btc_trades = fetch_historical_trades("BTCUSDT")
# Save OHLCV historical data
save_to_parquet(historical_df, "binance_ohlcv.parquet")

# Save trades
save_to_parquet(btc_trades, "binance_trades.parquet")
