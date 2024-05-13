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
