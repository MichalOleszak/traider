import numpy as np
import pandas as pd
import talib


def get_currency_volume_and_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Get stock liquidity (21 day rolling average of traded volume) and daily per-stock
    liquidity rank,"""
    df["pln_vol"] = df.loc[:, "close"].mul(df.loc[:, "volume_units"], axis=0)
    df["pln_vol"] = (
        df.groupby("ticker", group_keys=False, as_index=False)
        .pln_vol.rolling(window=21)
        .mean()
        .pln_vol
    )
    df["pln_vol_rank"] = df.groupby("date").pln_vol.rank(ascending=False)
    return df


def get_historical_returns(
    df: pd.DataFrame,
    periods: list = [1, 2, 3, 5, 10],
    outlier_cutoff: float = 0.01,
) -> pd.DataFrame:
    """Get returns for multipl historical perdiods.

    Winsorize returns at 1% and 99% levels and normalize with geometric average to get
    compounded daily returns.
    """
    for lag in periods:
        df[f"return_{lag}d"] = (
            df.groupby("ticker")
            .close.pct_change(lag)
            .pipe(
                lambda x: x.clip(
                    lower=x.quantile(outlier_cutoff),
                    upper=x.quantile(1 - outlier_cutoff),
                )
            )
            .add(1)
            .pow(1 / lag)
            .sub(1)
        )
    return df


def get_lagged_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Get lagged returns to use as features."""
    for t in range(1, 10):
        df[f"return_1d_t-{t}"] = df.groupby("ticker").return_1d.shift(t)
    return df


def get_lagged_market_ops_data(df: pd.DataFrame) -> pd.DataFrame:
    """Get lagged market operations data."""
    for t in range(1, 10):
        df[f"num_transactions_t-{t}"] = df.groupby("ticker").num_transactions.shift(t)
        df[f"volume_units_t-{t}"] = df.groupby("ticker").volume_units.shift(t)
        df[f"pln_vol_t-{t}"] = df.groupby("ticker").pln_vol.shift(t)
    return df


def get_next_day_return(df: pd.DataFrame, outlier_cutoff: float = 0.01) -> pd.DataFrame:
    """Get next-day returns, winsorized at 1% and 99% levels."""
    df[f"target_next_day_return"] = (
        df.groupby("ticker")
        .close.pct_change(1)
        .pipe(
            lambda x: x.clip(
                lower=x.quantile(outlier_cutoff), upper=x.quantile(1 - outlier_cutoff)
            )
        )
        .shift(-1)
    )
    return df


def get_momentum_factors(
    df: pd.DataFrame, periods: list = [2, 3, 5, 10]
) -> pd.DataFrame:
    """Get momentum factors for multiple historical periods.

    Based on:
    - Difference between returns over longer periods and the most recent return.
    - Difference between 10-day and 2-day return.
    """
    for lag in periods:
        df[f"momentum_{lag}d"] = df[f"return_{lag}d"].sub(df.return_1d)
    df["momentum_2_10d"] = df[f"return_10d"].sub(df.return_2d)
    return df


def add_date_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add features extracted from date."""
    df["weekday"] = df.date.dt.day_name()
    df["month"] = df.date.dt.month_name()
    df["day_in_month"] = df.date.dt.day
    return df


def remove_rows_with_nans(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where returns, momentum factors or aplha factors are NaN."""
    relevant_cols = [
        col
        for col in df.columns
        if "return" in col or "momentum" in col or "alpha" in col or "target" in col
    ]
    return df.dropna(subset=relevant_cols, how="any")


def get_relative_strength_index(df: pd.DataFrame) -> pd.DataFrame:
    df["alpha_rsi"] = (
        df.groupby("ticker")
        .close.apply(talib.RSI, timeperiod=14)
        .reset_index(drop=True)
    )
    return df


def get_bollinger_bands(df: pd.DataFrame) -> pd.DataFrame:
    """Get Bollinger Bands.

    Compute % difference between close price and each band and compress distribution
    with a log. Output: current stock value reflective to recent volatility trend.
    """

    def compute_bb(close):
        high, _, low = talib.BBANDS(close)
        return pd.DataFrame(
            {"alpha_bb_hi": high, "alpha_bb_lo": low}, index=close.index
        )

    df = pd.concat(
        [df, df.groupby("ticker").close.apply(compute_bb).reset_index(drop=True)],
        axis=1,
    )
    df["alpha_bb_hi"] = df.alpha_bb_hi.sub(df.close).div(df.alpha_bb_hi).apply(np.log1p)
    df["alpha_bb_lo"] = df.close.sub(df.alpha_bb_lo).div(df.close).apply(np.log1p)
    return df


def get_average_true_range(df: pd.DataFrame) -> pd.DataFrame:
    """Get Average True Range, standardized for comparability between stocks."""

    def compute_atr(high, low, close):
        atr_df = talib.ATR(high, low, close, timeperiod=14)
        return atr_df.sub(atr_df.mean()).div(atr_df.std())

    df["alpha_atr"] = df.groupby("ticker", group_keys=False).apply(
        lambda x: compute_atr(x["max"], x["min"], x["close"])
    )
    return df


def get_moving_average_convergence_divergence(df: pd.DataFrame) -> pd.DataFrame:
    """Get Moving Average Convergence/Divergence, standardized.

    Reflects difference between shorter and longer term exponential moving average.
    """

    def compute_macd(close):
        macd = talib.MACD(close)[0]
        return (macd / np.mean(macd)) / np.std(macd)

    df["alpha_macd"] = df.groupby("ticker", group_keys=False).close.apply(compute_macd)
    return df


def get_alpha_factors(df: pd.DataFrame) -> pd.DataFrame:
    """Get technical alpha factors."""
    df = get_relative_strength_index(df)
    df = get_bollinger_bands(df)
    df = get_average_true_range(df)
    df = get_moving_average_convergence_divergence(df)
    return df


def remove_superfluous_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove columns not needed for model training."""
    cols_to_drop = [
        "open",
        "max",
        "min",
        "close",
        "pct_change",
        "num_transactions",
        "volume_units",
        "trade_value_thousands",
        "pln_vol",
        "pln_vol_rank",
        "return_1d",
        "return_2d",
        "return_3d",
        "return_5d",
        "return_10d",
    ]
    return df.drop(columns=cols_to_drop)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = get_currency_volume_and_rank(df)
    df = get_historical_returns(df)
    df = get_lagged_returns(df)
    df = get_lagged_market_ops_data(df)
    df = get_next_day_return(df)
    df = get_momentum_factors(df)
    df = get_alpha_factors(df)
    df = add_date_features(df)
    df = remove_rows_with_nans(df)
    df = remove_superfluous_columns(df)
    return df
