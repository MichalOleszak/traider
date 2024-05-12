import requests
from bs4 import BeautifulSoup


def get_html(url):
    """Get the HTML content of a webpage."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        return soup.prettify()
    except requests.RequestException as e:
        return f"An error occurred: {e}"
