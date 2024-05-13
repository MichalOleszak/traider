from abc import ABC, abstractmethod
from typing import Optional

import requests
from bs4 import BeautifulSoup
from pandas import DataFrame


class BaseTableScraper(ABC):
    @staticmethod
    def _get_html(url):
        """Get the HTML content of a webpage."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            return soup.prettify()
        except requests.RequestException as e:
            return f"An error occurred: {e}"

    def _scrape(self, **kwargs) -> Optional[DataFrame]:
        """Scrape the webpage and return the table as a DataFrame."""
        url_processed = self._preprocess_url(**kwargs)
        html_content = self._get_html(url_processed)
        soup = BeautifulSoup(html_content, "html.parser")
        table = self._find_table(soup)
        if table:
            return self._postprocess(table, **kwargs)
        else:
            print("No table found with the specified class and attributes.")
            return None

    @abstractmethod
    def _preprocess_url(self, **kwargs) -> str:
        pass

    @abstractmethod
    def _find_table(self, soup):
        pass

    @abstractmethod
    def _postprocess(self, table, **kwargs) -> DataFrame:
        pass
