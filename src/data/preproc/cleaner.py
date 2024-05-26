import re

import pandas as pd


def drop_uninformative_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns with more than 50% missing values."""
    return df.drop(columns=df.columns[df.isna().mean() > 0.5])


def drop_superfluous_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that are not needed for the model."""
    cols_to_drop = [
        "currency",
        "name_x",
        "name_y",
        "isin",
    ]
    return df.drop(columns=cols_to_drop)


def fix_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    """Fix percentage change column.

    For some reason, the values coming from the WSE seem to sometimes be incorrect.
    """
    df = df.sort_values(["ticker", "date"])
    df["pct_change"] = df.groupby("ticker")["close"].pct_change() * 100
    return df[df["pct_change"].notnull()].reset_index(drop=True)


def clean_percentage_string(input_str: str) -> str | float:
    """Clean a string representing a percentage: 7.61% -> 0.0761."""
    pattern = re.compile(r"^-?(\d+(\.\d+)?)%$")
    match = pattern.match(input_str)
    if match:
        number = float(match.group(1))
        return number / 100
    return input_str


def clean_percentage_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply string percentage cleanup to all obejct columns and cast to floats."""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: clean_percentage_string(x)
            if pd.notnull(x) and type(x) == str
            else x
        )
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            pass
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean joined data."""
    df = drop_uninformative_columns(df)
    df = drop_superfluous_columns(df)
    df = fix_pct_change(df)
    df = clean_percentage_string_columns(df)
    return df
