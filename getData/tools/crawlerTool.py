import requests
from bs4 import BeautifulSoup

class CrawlerTool:
    """Web scraping class for crawling websites and extracting information."""

    def __init__(self, url, headers):
        self.url = url
        self.headers = headers

    def fetch_data(self, limit=None):
        """
        Fetch data from a website
        :param limit: Limit for the number of results fetched
        :return: List of data extracted from the website
        """
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            return self.parse_data(soup, limit)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return []

    def parse_data(self, soup, limit):
        """
        Parse the fetched web page to extract useful data.
        :param soup: Parsed HTML content
        :param limit: Limit the number of items
        :return: List of extracted data
        """
        raise NotImplementedError("Subclasses should implement this method to parse specific data")
