import pandas as pd
from pandas import DataFrame


def replace_header_with_top_row(df: DataFrame) -> DataFrame:
    """Replace the header with the top row of a DataFrame."""
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    return df


def drop_duplicate_columns(df: DataFrame) -> DataFrame:
    """Drop duplicate columns from a DataFrame."""
    duplicates = df.T.duplicated()
    return df.T[~duplicates].T


def rename_duplicate_columns(df: DataFrame) -> DataFrame:
    """Rename duplicate columns in a DataFrame."""
    cols = pd.Series(df.columns)
    for dup in df.columns[df.columns.duplicated(keep=False)]:
        cols[df.columns.get_loc(dup)] = [
            f"{dup}_{i}" if i != 0 else dup
            for i in range(df.columns.get_loc(dup).sum())
        ]
    df.columns = cols
    return df
