from binance_api_call import OHLCVData
import pandas as pd
from typing import List


class DataTransformation:
    """Handles data cleaning, EMA addition, and anonymization tasks."""

    @staticmethod
    def add_ema(
        ohlcv_data: List[OHLCVData], periods=[20, 50, 100, 200]
    ) -> List[OHLCVData]:
        """
        Add Exponential Moving Averages (EMAs) to a list of OHLCVData objects.

        :param ohlcv_data: List of OHLCVData objects
        :param periods: List of periods for EMAs
        :return: List of OHLCVData objects with added EMA attributes
        """
        df = pd.DataFrame([data.__dict__ for data in ohlcv_data])
        for period in periods:
            df[f"EMA_{period}"] = df["close"].ewm(span=period, adjust=False).mean()

        # Update OHLCVData objects with the new EMA values
        for i, data in enumerate(ohlcv_data):
            for period in periods:
                setattr(data, f"EMA_{period}", df[f"EMA_{period}"].iloc[i])

        return ohlcv_data

    @staticmethod
    def clean_data(ohlcv_data: List[OHLCVData]) -> List[OHLCVData]:
        """Perform data cleaning steps on OHLCVData objects."""
        cleaned_data = []
        seen = set()

        for data in ohlcv_data:
            if (data.symbol, data.timestamp) not in seen:
                # Apply cleaning conditions (e.g., positive values check)
                if all(
                    [
                        x > 0
                        for x in [
                            data.open,
                            data.high,
                            data.low,
                            data.close,
                            data.volume,
                        ]
                    ]
                ):
                    seen.add((data.symbol, data.timestamp))
                    cleaned_data.append(data)

        return cleaned_data

    @staticmethod
    def anonymize_data(ohlcv_data: List[OHLCVData]) -> List[OHLCVData]:
        """Mask volume data to anonymize trade sizes."""
        for data in ohlcv_data:
            data.volume = round(data.volume, 2)
            data.taker_buy_base = round(data.taker_buy_base, 2)
            data.taker_buy_quote = round(data.taker_buy_quote, 2)
        return ohlcv_data


class DataSaver:
    """Handles saving data to Parquet format."""

    @staticmethod
    def save_to_parquet(ohlcv_data: List[OHLCVData], filename: str):
        """Save a list of OHLCVData to Parquet format."""
        df = pd.DataFrame([data.__dict__ for data in ohlcv_data])
        df.to_parquet(filename, index=False)
        logger.info(f"Saved {filename}")


class ReportGenerator:
    """Handles the generation of reports (e.g., CSV)."""

    @staticmethod
    def generate_report(ohlcv_data: List[OHLCVData], filename: str):
        """Generate and save the report as a CSV file."""
        df = pd.DataFrame([data.__dict__ for data in ohlcv_data])
        df.to_csv(filename, index=False)
        logger.info(f"Report saved to {filename}")
