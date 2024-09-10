__version__ = "0.1.2"

from io import BytesIO
from bs4 import BeautifulSoup
from collections import namedtuple
from functools import lru_cache
import requests
import re
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor


class Immobiliare:
    def __init__(self, url: str, *,
                 verbose: bool = True,
                 min_house_cost: int = 100,
                 browse_all_pages: bool = True,
                 area_not_found: int = 0,
                 price_not_found: float = float('nan'),
                 floor_not_found: int = 0,
                 car_not_found: int = 0,
                 energy_not_found: str = "n/a",
                 invalid_price_per_area: int = 0,
                 wait: int = 100):
        """ Initialize the Immobiliare object with configuration parameters. """
        self.url = url
        self.verbose = verbose
        self.min_house_cost = min_house_cost
        self.browse_all_pages = browse_all_pages
        self.wait = wait / 1000
        
        self.area_not_found = area_not_found
        self.price_not_found = price_not_found
        self.floor_not_found = floor_not_found
        self.car_not_found = car_not_found
        self.energy_not_found = energy_not_found
        self.invalid_price_per_area = invalid_price_per_area

        # Configure logging
        logging.basicConfig(level=logging.INFO if verbose else logging.WARNING)

    def _say(self, message: str):
        """ Log messages if verbosity is enabled. """
        logging.info(message)

    def _get_page(self, url: str, timeout: int = 10) -> BytesIO:
        """ Fetch and return a page as a BytesIO object. """
        try:
            req = requests.get(url, allow_redirects=False, timeout=timeout)
            page = BytesIO(req.content)
            return page
        except requests.exceptions.Timeout:
            self._say(f"Timeout while trying to reach {url}")
            return BytesIO()

    def _get_text(self, url: str) -> str:
        """ Extract text content from a URL. """
        page = self._get_page(url)
        page.seek(0)
        soup = BeautifulSoup(page, "html.parser")
        text = ' '.join(soup.get_text().split())
        return text

    def _extract_pattern(self, text: str, patterns: tuple) -> str:
        """ Extract the first matching pattern from text. """
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None

    def _get_data(self, sub_url: str) -> namedtuple:
        """ Extract data from a single sub-URL. """
        t = self._get_text(sub_url).lower()

        # Extract cost
        cost_patterns = (
            "€ (\d+\.\d+\.\d+)",
            "€ (\d+\.\d+)"
        )
        cost = self._extract_pattern(t, cost_patterns)
        if cost:
            cost = cost.replace(".", "")
            if int(cost) < self.min_house_cost:
                self._say(f"Too low house price: {int(cost)} for {sub_url}")
                cost = None
        else:
            if "prezzo su richiesta" in t:
                self._say(f"Price available upon request for {sub_url}")
                cost = self.price_not_found
            else:
                self._say(f"Can't get price for {sub_url}")
                cost = self.price_not_found

        # Extract floor
        floor_patterns = (
            "piano (\d{1,2})",
            "(\d{1,2}) piano",
            "(\d{1,2}) piani",
        )
        floor = self._extract_pattern(t, floor_patterns)
        if "piano terra" in t:
            floor = 1
        ultimo = "ultimo" in t

        # Extract area
        area_pattern = "superficie (\d{1,4}) m"
        area = self._extract_pattern(t, (area_pattern,))
        if area is None:
            area = self.area_not_found

        # Extract energy class
        energy_patterns = (
            "energetica (\D{1,2}) ",
            "energetica(\S{1,2})",
        )
        energy = self._extract_pattern(t, energy_patterns)
        if energy and energy[0] in "ABCDEF" and energy[-1] in "0123456789+":
            energy = energy.upper()
        else:
            if "in attesa di certificazione" in t:
                self._say(f"Energy efficiency still pending for {sub_url}")
                energy = self.energy_not_found
            else:
                self._say(f"Can't get energy efficiency from {sub_url}")
                energy = self.energy_not_found

        # Extract parking spots
        car_patterns = ("post\S auto (\d{1,2})",)
        car = self._extract_pattern(t, car_patterns)
        if car is None:
            if re.search("possibilit\S.{0,10}auto", t):
                self._say(f"Car spot/box available upon request for {sub_url}")
                car = 0
            else:
                car = self.car_not_found

        # Calculate price per area
        try:
            price_per_area = round(int(cost) / int(area), 1)
        except:
            price_per_area = self.invalid_price_per_area

        # Pack the results
        House = namedtuple(
            "House", [
                "cost",
                "price_per_area",
                "floor",
                "area",
                "ultimo",
                "url",
                "energy",
                "posto_auto"
            ]
        )
        return House(cost, price_per_area, floor, area, ultimo, sub_url, energy, car)

    def get_all_urls(self):
        """ Retrieve all URLs for house listings. """
        pattern = re.compile(r"\d+/$")
        urls_ = []

        # Process first page
        self._say("Processing page 1")
        page = self._get_page(self.url)
        soup = BeautifulSoup(page, "html.parser")
        for link in soup.find_all("a"):
            time.sleep(self.wait)
            l = link.get("href")
            if l and "https" in l and "annunci" in l and pattern.search(l):
                urls_.append(l)

        if self.browse_all_pages:
            for i in range(2, 10_000):
                self._say(f"Processing page {i}")
                curr_url = f"{self.url}&pag={i}"
                t = self._get_text(curr_url).lower()
                if "404 not found" in t or "non è presente" in t:
                    break
                else:
                    page = self._get_page(curr_url)
                    soup = BeautifulSoup(page, "html.parser")
                    for link in soup.find_all("a"):
                        l = link.get("href")
                        if l and "https" in l and "annunci" in l and pattern.search(l):
                            urls_.append(l)

        self.urls_ = urls_
        self._say("All retrieved URLs stored in attribute 'urls_'")
        self._say(f"Found {len(urls_)} houses matching criteria.")

    def find_all_houses(self):
        """ Find and store data for all houses. """
        if not hasattr(self, "urls_"):
            self.get_all_urls()

        with ThreadPoolExecutor(max_workers=10) as executor:
            all_results = list(executor.map(self._get_data, self.urls_))

        self.df_ = pd.DataFrame(all_results)
        self._say("Results stored in attribute 'df_'")
        print(f"Numero di case trovate: {len(all_results)}")
