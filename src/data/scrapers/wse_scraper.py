import re
from io import StringIO
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup
from pandas import DataFrame
from tqdm import tqdm

from src.data.scrapers.base_table_scraper import BaseTableScraper


class WSEPriceScraper(BaseTableScraper):
    def __init__(self):
        self.BASE_URL = "https://www.gpw.pl/"
        self.PRICES_ARCHIVE_URL = (
            f"{self.BASE_URL}archiwum-notowan-full?type=10&instrument=&"
        )
        self.COLUMN_NAMES = [
            "name",
            "isin",
            "currency",
            "open",
            "max",
            "min",
            "close",
            "pct_change",
            "volume_units",
            "num_transactions",
            "trade_value_thousands",
        ]

    def _preprocess_url(self, **kwargs) -> str:
        date = pd.to_datetime(kwargs["date"]).strftime("%d-%m-%Y")
        return f"{self.PRICES_ARCHIVE_URL}date={date}"

    @staticmethod
    def _find_table(soup):
        return soup.find(
            "table", class_="table footable", attrs={"data-sorting": "true"}
        )

    def _postprocess(self, table, **kwargs) -> DataFrame:
        table_html = StringIO(
            re.sub(r"(\d)\s+(\d)", r"\1\2", str(table).replace(",", "."))
        )
        df = pd.read_html(table_html)[0]
        df.columns = self.COLUMN_NAMES
        df["date"] = pd.to_datetime(kwargs["date"], format="%d-%m-%Y")
        return df

    def get_prices_for_date(self, date: str) -> Optional[DataFrame]:
        """Get stock prices for a given date.

        Args:
            date (str): Date in format YYYY-MM-DD.
        """
        return self._scrape(date=pd.to_datetime(date))

    def get_prices_for_date_range(self, date_start: str, date_end: str) -> DataFrame:
        """Get stock prices for a given date range.

        Args:
            date_start (str): Start date in format YYYY-MM-DD.
            date_end (str): End date in format YYYY-MM-DD.
        """
        dates = [
            date
            for date in pd.date_range(start=date_start, end=date_end, freq="D")
            if date.day_name() not in ["Saturday", "Sunday"]
        ]
        df_list = [self.get_prices_for_date(date) for date in tqdm(dates)]
        return pd.concat(df_list, ignore_index=True)

    def get_prices_for_month(self, year: int, month: int) -> DataFrame:
        """Get stock prices for a given month.

        Args:
            year (int): Year.
            month (int): Month.
        """
        date_start = f"{year}-{month:02d}-01"
        date_end = f"{year}-{month:02d}-{pd.Period(year=year, month=month, freq='M').days_in_month}"
        return self.get_prices_for_date_range(date_start, date_end)

    def get_info_for_isin(self, isin_number: str) -> dict:
        """Get company name and ticker for a given ISIN."""
        target_url = f"{self.BASE_URL}/spolka?isin={isin_number}#infoTab"
        html_content = self._get_html(target_url)
        soup = BeautifulSoup(html_content, "html.parser")
        return {
            "isin": isin_number,
            "name": soup.find("small", {"id": "getH1"})
            .get_text(strip=True)
            .split("(")[0]
            .strip(),
            "ticker": soup.find_all("input", {"id": "glsSkrot"})[0].get("value"),
        }
