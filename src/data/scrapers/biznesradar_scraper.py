import re
from typing import Optional

import numpy as np
import pandas as pd
from pandas import DataFrame

from src.data.scrapers.base_table_scraper import BaseTableScraper
from src.utils import pandas as pandas_utils


class BiznesradarScraper(BaseTableScraper):
    def __init__(self):
        self.BASE_URL = "https://www.biznesradar.pl/"

    def _preprocess_url(self, **kwargs) -> str:
        return (
            f"{self.BASE_URL}raporty-finansowe-{kwargs['resource']}/{kwargs['ticker']}"
        )

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
        for col in range(1, len(df.columns)):  # skip first column with row names
            df.iloc[:, col] = df.iloc[:, col].apply(self._clean_pl_string)
        df = df.transpose().reset_index(drop=True)
        df = pandas_utils.replace_header_with_top_row(df)
        df = df.replace("", np.nan)
        df = df.dropna()
        df["ticker"] = kwargs["ticker"]
        return df

    @staticmethod
    def _clean_pl_string(input_string) -> str:
        """Clean a string from the profit and loss statement data."""
        # Do not change date pattern YYYY-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", input_string):
            return input_string
        # Apply only to strings starting with a digit or a hyphen (negative numbers)
        if input_string and (input_string[0].isdigit() or input_string[0] == "-"):
            # Find all characters that are digits, spaces or a starting hyphen until the first alphabetic character
            match = re.match(r"^-?[0-9 ]*[^\dA-Za-z]*", input_string)
            if match:
                return match.group(0).replace(" ", "")
        return input_string.replace(" ", "")

    def get_profit_and_loss_statement_for_ticker(
        self, ticker: str
    ) -> Optional[DataFrame]:
        """Get profit and loss statement for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="rachunek-zyskow-i-strat")

    def get_balance_sheet_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get balance sheeet for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="bilans")

    def get_cash_flow_for_ticker(self, ticker: str) -> Optional[DataFrame]:
        """Get cash flow for a given ticker.

        Args:
            ticker (str): Stock ticker.
        """
        return self._scrape(ticker=ticker, resource="przeplywy-pieniezne")
