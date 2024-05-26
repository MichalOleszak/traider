import os

import pandas as pd

from src.data import config


def merge_data() -> pd.DataFrame:
    """Merge all data into one DataFrame."""
    prices = pd.concat(
        [
            pd.read_csv(
                f"{config.DATA_PATHS['PRICES_DP']}/{file}", parse_dates=["date"]
            )
            for file in [
                fn
                for fn in os.listdir(config.DATA_PATHS["PRICES_DP"])
                if fn.endswith(".csv")
            ]
        ]
    )
    info = pd.read_json(config.DATA_PATHS["INFO_FP"])
    balance_sheets = pd.read_csv(
        config.DATA_PATHS["BALANCE_SHEETS_FP"], parse_dates=["Data publikacji"]
    )
    cash_flows = pd.read_csv(
        config.DATA_PATHS["CASH_FLOWS_FP"], parse_dates=["Data publikacji"]
    )
    profit_and_loss = pd.read_csv(
        config.DATA_PATHS["PROFIT_AND_LOSS_FP"], parse_dates=["Data publikacji"]
    )
    market_value_ind = pd.read_csv(
        config.DATA_PATHS["MARKET_VALUE_IND_FP"], parse_dates=["date"]
    )
    profitability_ind = pd.read_csv(
        config.DATA_PATHS["PROFITABILITY_IND_FP"], parse_dates=["date"]
    )
    cash_flow_ind = pd.read_csv(
        config.DATA_PATHS["CASH_FLOW_IND_FP"], parse_dates=["date"]
    )
    debt_ind = pd.read_csv(config.DATA_PATHS["DEBT_IND_FP"], parse_dates=["date"])
    liquidity_ind = pd.read_csv(
        config.DATA_PATHS["LIQUIDITY_IND_FP"], parse_dates=["date"]
    )
    activity_ind = pd.read_csv(
        config.DATA_PATHS["ACTIVITY_IND_FP"], parse_dates=["date"]
    )

    df = pd.DataFrame(
        pd.date_range(prices.date.min(), prices.date.max(), freq="D", name="date")
    )
    df = df.merge(prices, on="date", how="left")
    df = df.merge(info, on="isin", how="left")
    df = df.merge(
        balance_sheets.rename(columns={"Data publikacji": "date"}),
        on=["date", "ticker"],
        how="left",
    )
    df = df.merge(
        cash_flows.rename(columns={"Data publikacji": "date"}),
        on=["date", "ticker"],
        how="left",
    )
    df = df.merge(
        profit_and_loss.rename(columns={"Data publikacji": "date"}),
        on=["date", "ticker"],
        how="left",
    )
    df = df.merge(market_value_ind, on=["date", "ticker"], how="left")
    df = df.merge(profitability_ind, on=["date", "ticker"], how="left")
    df = df.merge(cash_flow_ind, on=["date", "ticker"], how="left")
    df = df.merge(debt_ind, on=["date", "ticker"], how="left")
    df = df.merge(liquidity_ind, on=["date", "ticker"], how="left")
    df = df.merge(activity_ind, on=["date", "ticker"], how="left")

    assert all(
        df.groupby(["date", "ticker"]).count() == 1
    ), "Found multiple entries for date-ticker pair!"
    return df


def remove_non_trading_days(df: pd.DataFrame) -> pd.DataFrame:
    """Remove non-trading days from the DataFrame."""
    return df[df["close"].notnull()].reset_index(drop=True)


def remove_tickers_with_no_info(df: pd.DataFrame) -> pd.DataFrame:
    """Remove tickers with no information.

    These are tickers for which the get_info method of the WSEScraper returned Nones.
    Some of them are not traded anymore, for other the reason for scraping failure is
    has not been investigated. This filtering is a known source of selection bias.
    """
    return df[df["ticker"].notnull()].reset_index(drop=True)


def propagate_non_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    """Propagate data published at non-daily frequency to fill in missing values."""
    df = df.sort_values(["ticker", "date"])
    df = df.groupby("ticker").apply(lambda group: group.fillna(method="ffill"))
    return df.reset_index(drop=True)


def join_data() -> pd.DataFrame:
    df = merge_data()
    df = remove_non_trading_days(df)
    df = remove_tickers_with_no_info(df)
    df = propagate_non_daily_data(df)
    return df
