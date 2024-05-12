import re
from typing import Optional

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pandas import DataFrame

from src.data.scrapers.utils import get_html
from src.utils import pandas as pandas_utils

BASE_URL = "https://www.biznesradar.pl/"


def clean_pl_string(input_string):
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


def get_profit_and_loss_statement_for_ticker(ticker: str) -> Optional[DataFrame]:
    """Get profit and loss statement for a given ticker.

    Args:
        ticker (str): Stock ticker.
    """
    target_url = f"{BASE_URL}raporty-finansowe-rachunek-zyskow-i-strat/{ticker}"
    html_content = get_html(target_url)
    soup = BeautifulSoup(html_content, "lxml")
    table = soup.find("table", class_="report-table")
    if table:
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
            df.iloc[:, col] = df.iloc[:, col].apply(clean_pl_string)
        df = df.transpose().reset_index(drop=True)
        df = pandas_utils.replace_header_with_top_row(df)
        df["ticker"] = ticker
        df = df.replace("", np.nan)
        df = df.dropna()
        return df
    else:
        print("No table found with the specified class and attributes.")
        return None
