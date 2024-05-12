from io import StringIO
import re
from typing import Optional

from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from src.data.scrapers.utils import get_html


BASE_URL = "https://www.gpw.pl/"
PRICES_ARCHIVE_URL = f"{BASE_URL}/archiwum-notowan-full?type=10&instrument=&"
COLUMN_NAMES = [
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


def get_prices_for_date(date: str) -> Optional[DataFrame]:
    """Get stock prices for a given date.

    Args:
        date (str): Date in format YYYY-MM-DD.
    """
    date = pd.to_datetime(date).strftime("%d-%m-%Y")
    target_url = f"{PRICES_ARCHIVE_URL}date={date}"
    html_content = get_html(target_url)
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", class_="table footable", attrs={"data-sorting": "true"})
    if table:
        # Remove spaces between digits and convert commas to dots
        table_html = StringIO(re.sub(r'(\d)\s+(\d)', r'\1\2', str(table).replace(",", ".")))
        df = pd.read_html(table_html)[0]
        df.columns = COLUMN_NAMES
        df["date"] = pd.to_datetime(date, format="%d-%m-%Y")
        return df
    else:
        print("No table found with the specified class and attributes.")
        return None


def get_prices_for_date_range(date_start: str, date_end: str) -> DataFrame:
    """Get stock prices for a given date range.

    Args:
        date_start (str): Start date in format YYYY-MM-DD.
        date_end (str): End date in format YYYY-MM-DD.
    """
    dates = [
        date for date in pd.date_range(start=date_start, end=date_end, freq="D")
        if date.day_name() not in ["Saturday", "Sunday"]
    ]
    df_list = [get_prices_for_date(date) for date in tqdm(dates)]
    return pd.concat(df_list, ignore_index=True)


def get_prices_for_month(year: int, month: int) -> DataFrame:
    """Get stock prices for a given month.

    Args:
        year (int): Year.
        month (int): Month.
    """
    date_start = f"{year}-{month:02d}-01"
    date_end = f"{year}-{month:02d}-{pd.Period(year=year, month=month, freq='M').days_in_month}"
    return get_prices_for_date_range(date_start, date_end)


def get_info_for_isin(isin: str) -> dict:
    """Get company name and ticker for a given ISIN."""
    target_url = f"{BASE_URL}/spolka?isin={isin}#infoTab"
    html_content = get_html(target_url)
    soup = BeautifulSoup(html_content, "html.parser")
    return {
        "isin": isin,
        "name": soup.find("small", {"id": "getH1"}).get_text(strip=True).split("(")[0].strip(),
        "ticker": soup.find_all('input', {'id': 'glsSkrot'})[0].get('value'),
    }