from file_writer import BinanceWebSocketClient


symbols_to_track = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

# Initialize WebSocket client
binance_ws = BinanceWebSocketClient(symbols=symbols_to_track, interval="1m")

# Start the stream
binance_ws.start_stream()


def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
    """Perform data cleaning steps."""
    df.fillna(method="ffill", inplace=True)
    df.dropna(inplace=True)
    df.drop_duplicates(subset=["symbol", "timestamp"], keep="last", inplace=True)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "taker_buy_base",
        "taker_buy_quote",
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df = df[
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["volume"] > 0)
    ]
    return df


def anonymize_data(self, df: pd.DataFrame) -> pd.DataFrame:
    """Mask volume data to anonymize trade sizes."""
    df["volume"] = df["volume"].apply(lambda x: round(x, 2))  # Mask small variations
    df["taker_buy_base"] = df["taker_buy_base"].apply(lambda x: round(x, 2))
    df["taker_buy_quote"] = df["taker_buy_quote"].apply(lambda x: round(x, 2))
    return df


def save_to_parquet(self, df: pd.DataFrame, filename: str):
    """Save DataFrame to Parquet format."""
    df.to_parquet(filename, index=False)
    logger.info(f"Saved {filename}")


class DataTransformation:
    """Handles data cleaning, EMA addition, and anonymization tasks."""

    @staticmethod
    def add_ema(df: pd.DataFrame, periods=[20, 50, 100, 200]) -> pd.DataFrame:
        """
        Add Exponential Moving Averages (EMAs) to the DataFrame.

        :param df: DataFrame with price data
        :param periods: List of periods for EMAs
        :return: DataFrame with added EMA columns
        """
        for period in periods:
            df[f"EMA_{period}"] = df["close"].ewm(span=period, adjust=False).mean()
        return df

    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Perform data cleaning steps."""
        df.fillna(method="ffill", inplace=True)
        df.dropna(inplace=True)
        df.drop_duplicates(subset=["symbol", "timestamp"], keep="last", inplace=True)

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "taker_buy_base",
            "taker_buy_quote",
        ]
        df[numeric_cols] = df[numeric_cols].astype(float)
        df = df[
            (df["open"] > 0)
            & (df["high"] > 0)
            & (df["low"] > 0)
            & (df["close"] > 0)
            & (df["volume"] > 0)
        ]
        return df

    @staticmethod
    def anonymize_data(df: pd.DataFrame) -> pd.DataFrame:
        """Mask volume data to anonymize trade sizes."""
        df["volume"] = df["volume"].apply(
            lambda x: round(x, 2)
        )  # Mask small variations
        df["taker_buy_base"] = df["taker_buy_base"].apply(lambda x: round(x, 2))
        df["taker_buy_quote"] = df["taker_buy_quote"].apply(lambda x: round(x, 2))
        return df
