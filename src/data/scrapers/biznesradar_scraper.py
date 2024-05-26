import re
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from pandas import DataFrame
from src.utils import pandas as pandas_utils

from src.data.scrapers.base_table_scraper import BaseTableScraper


class BiznesradarScraper(BaseTableScraper):
    def __init__(self):
        self.BASE_URL = "https://www.biznesradar.pl/"

    def _preprocess_url(self, **kwargs) -> str:
        return f"{self.BASE_URL}{kwargs['resource']}/{kwargs['ticker']}"

    @staticmethod
    def _find_table(soup):
        return soup.find("table", class_="report-table")

    def _postprocess(self, table, **kwargs) -> DataFrame:
        headers = [
            th.get_text(strip=True)
            for th in table.find_all("th")
            if th.get_text(strip=True)
        ]
        headers = [""] + headers + [""]
        data = []
        for tr in table.find_all("tr")[1:]:  # skip the first row (header)
            row_data = [td.get_text(strip=True) for td in tr.find_all("td")]
            if row_data:
                data.append(row_data)
        df = pd.DataFrame(data, columns=headers)
        if "wskazniki-" in kwargs["resource"]:
            dates = [date for date in df.columns if date]
        for col in range(1, len(df.columns)):  # skip first column with row names
            df.iloc[:, col] = df.iloc[:, col].apply(self._clean_string)
        df = df.transpose().reset_index(drop=True)
        df = pandas_utils.replace_header_with_top_row(df)
        df = df.replace("", np.nan)
        df = df.dropna(how="all")
        if "wskazniki-" in kwargs["resource"]:
            df["date"] = [self._extract_date(date) for date in dates]
        df["ticker"] = kwargs["ticker"]
        return df

    @staticmethod
    def _clean_string(input_string) -> str:
        """Clean a string from the profit and loss statement data."""
        # Do not change date pattern YYYY-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", input_string):
            return input_string
        # Missing values get filled-in with strings like "r/r-100.00%~sektor-1.14%"
        if input_string.startswith("r/r") or input_string.startswith("k/k"):
            return ""
        # Apply only to strings starting with a digit or a hyphen (negative numbers)
        if input_string and (input_string[0].isdigit() or input_string[0] == "-"):
            # Find all characters that are digits, spaces or a starting hyphen until the first alphabetic character
            match = re.match(r"^-?[0-9 .]*[^\dA-Za-z~]*", input_string)
            if match:
                return match.group(0).replace(" ", "")
        return input_string.replace(" ", "")

    @staticmethod
    def _extract_date(date_str):
        """Extract date from the string in the format '2017/Q4(gru 17)'."""
        month_map = {
            "sty": 1,
            "lut": 2,
            "mar": 3,
            "kwi": 4,
            "maj": 5,
            "cze": 6,
            "lip": 7,
            "sie": 8,
            "wrz": 9,
            "paź": 10,
            "lis": 11,
            "gru": 12,
        }
        year = int(date_str[:4])
        month_abbr = date_str.split("(")[1][:3]
        month = month_map[month_abbr]
        last_day = pd.Period(f"{year}-{month:02d}").end_time.day
        return datetime(year, month, last_day)

    def get_profit_and_loss_statement_for_ticker(
        self, ticker: str
    ) -> Optional[DataFrame]:
        """Get profit and loss statement for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(
            ticker=ticker, resource="raporty-finansowe-rachunek-zyskow-i-strat"
        )

    def get_balance_sheet_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get balance sheeet for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="raporty-finansowe-bilans")

    def get_cash_flow_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get cash flow for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(
            ticker=ticker, resource="raporty-finansowe-przeplywy-pieniezne"
        )

    def get_market_value_indicators_for_ticker(
        self, ticker: str
    ) -> Optional[DataFrame]:
        """Get market value indicators for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="wskazniki-wartosci-rynkowej")

    def get_profitability_indicators_for_ticker(
        self, ticker: str
    ) -> Optional[DataFrame]:
        """Get profitability indicators for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="wskazniki-rentownosci")

    def get_cash_flow_indicators_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get cash flow indicators for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="wskazniki-przeplywow-pienieznych")

    def get_debt_indicators_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get debt indicators for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="wskazniki-zadluzenia")

    def get_liquidity_indicators_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get liquidity indicators for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="wskazniki-plynnosci")

    def get_activity_indicators_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get activity indicators for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="wskazniki-aktywnosci")
