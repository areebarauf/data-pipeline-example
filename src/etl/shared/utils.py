import os
from typing import List
import pandas as pd
from observability import get_logger
from src.etl.shared.file_writer import FileWriteDataReturnValue

logger = get_logger(__name__)


def write_parquet(
    df: pd.DataFrame,
    destination_path: str,
    partition_cols: List[str] = None,
    mode: str = "overwrite",
) -> FileWriteDataReturnValue:
    """
    Writes a DataFrame to a Parquet file and returns metadata.

    :param df: Pandas DataFrame to write.
    :param destination_path: Path where the Parquet file should be saved.
    :param partition_cols: Optional list of columns to partition data.
    :param mode: "overwrite" (default) or "append".
    :return: FileWriteDataReturnValue object.
    """

    os.makedirs(
        os.path.dirname(destination_path), exist_ok=True
    )  # Ensure directory exists

    if partition_cols:
        df.to_parquet(
            destination_path,
            partition_cols=partition_cols,
            engine="pyarrow",
            index=False,
        )
    else:
        df.to_parquet(destination_path, engine="pyarrow", index=False)

    return FileWriteDataReturnValue(paths=[destination_path], rows_written=len(df))


# def write_json_to_df():
