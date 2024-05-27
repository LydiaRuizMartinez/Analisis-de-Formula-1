"""
formula1_spider.py

Adquisición de datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Lydia Ruiz
    - David Tarrasa
    - Jorge Vančo
    - Alberto Velasco

Descripción:
El objetivo del crawler es hacer web scraping de la página web de Wikipedia de las distintas temporadas de Formula 1 (desde 2012 hasta la actualidad)"""

from numpy import isin
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import re
import os


class F1Scraper(scrapy.Spider):
    """
    Class that implements Scrapy's spider interface to crawl Formula One race data from Wikipedia.
    """

    name = "formula1"
    # Generate start URLs for the different Formula 1 seasons
    start_urls = [
        f"https://en.wikipedia.org/wiki/{year}_Formula_One_World_Championship"
        for year in range(2012, 2024)
    ]

    def parse(self, response):
        """
        Method that parses the response from the page and extracts links to race reports.
        """
        # Find the table containing Grand Prix information
        grands_prix_table = self.find_grand_prix_table(response)
        i = 1
        for row in grands_prix_table.xpath(".//tr[position()>1]"):
            # Extract link to race report
            report_link = row.xpath(".//td[6]/a/@href").extract_first()
            if report_link:
                # Request each race report page for further parsing
                yield scrapy.Request(
                    response.urljoin(report_link),
                    callback=self.parse_report,
                    meta={"race_number": i},
                )
                i += 1

    def find_grand_prix_table(self, response):
        """
        Method that searches for the grand prix table in the HTML response.
        """

        for table in response.xpath('//table[@class="wikitable sortable"]'):
            if "Report" in table.xpath(".//th//text()").extract():
                return table

    def parse_report(self, response):
        """
        Method that analyzes the response from the race report page and extracts the information.
        """

        # Extract year and Grand Prix name from the report title
        year, prix = self.extract_year_and_prix(response)
        if not year:
            year = "2020"  # 70th Anniversary Grand Prix (2020) doesn't match any year

        # Create a directory for each year if it doesn't exist
        path = self.create_directory(year, prix)

        # Depending on the year and the prix, the race classification table has a different format.
        race_table = self.get_race_table(response, year, prix)

        # Extract race data from the table
        rows = self.extract_race_data(race_table)

        column_names = [
            "Pos",
            "DriverNumber",
            "Driver",
            "Constructor",
            "Laps",
            "Time/Retired",
            "Grid",
            "Points",
        ]

        # Create a DataFrame with the extracted data
        df = pd.DataFrame(rows, columns=column_names)

        race_number = response.meta.get("race_number")
        df["RaceNumber"] = race_number
        df["Season"] = year
        df["RaceName"] = prix
        df.to_csv(path, index=False)

    def extract_year_and_prix(self, response):
        """
        Method that extracts the year and Grand Prix name from the race report page's title.
        """

        report_title = response.xpath("//title/text()").extract_first()
        year_match = re.search(r"\d{4}", report_title)
        year = year_match.group() if year_match else "2020"
        prix = report_title.lstrip(f"{year} ").rstrip(" - Wikipedia")
        return year, prix

    def create_directory(self, year, prix):
        """
        Method that creates a directory for each year and Grand Prix if it doesn't exist.
        """
        year = os.path.join("cache", "spider", year)
        os.makedirs(year, exist_ok=True)
        return os.path.join(year, f"{prix}.csv")

    def get_race_table(self, response, year, prix):
        """
        Method that determines the correct race table based on the year and Grand Prix name.
        """

        # Conditions for different years and Grands Prix
        if year in ["2015", "2016"]:
            race_table = response.xpath('//table[@class="wikitable"]')

        elif year == "2012":
            if prix == "Spanish Grand Prix":
                race_table = response.xpath('//table[@class="wikitable sortable"]')[-1]
            else:
                race_table = response.xpath('//table[@class="wikitable"]')

        elif year == "2013":
            if prix in ["Malaysian Grand Prix", "British Grand Prix"]:
                race_table = response.xpath('//table[@class="wikitable sortable"]')[-1]
            else:
                race_table = response.xpath('//table[@class="wikitable"]')

        elif year == "2014":
            if prix in [
                "Abu Dhabi Grand Prix",
                "Australian Grand Prix",
                "Austrian Grand Prix",
                "Chinese Grand Prix",
                "German Grand Prix",
                "Hungarian Grand Prix",
                "Malaysian Grand Prix",
                "Russian Grand Prix",
            ]:
                race_table = response.xpath('//table[@class="wikitable"]')
            else:
                race_table = response.xpath('//table[@class="wikitable sortable"]')[-1]

        elif year == "2017":
            if prix in [
                "Chinese Grand Prix",
                "Bahrain Grand Prix",
                "Spanish Grand Prix",
                "Monaco Grand Prix",
                "Canadian Grand Prix",
                "Azerbaijan Grand Prix",
                "Austrian Grand Prix",
                "British Grand Prix",
            ]:
                race_table = response.xpath('//table[@class="wikitable sortable"]')[-1]
            else:
                race_table = response.xpath('//table[@class="wikitable"]')
        else:
            race_table = response.xpath('//table[@class="wikitable sortable"]')[-1]

        return race_table

    def extract_race_data(self, race_table):
        """
        Method that extracts race data from the race classification table.
        """

        rows = []
        for row in race_table.xpath(".//tr[position()>1]"):
            data = []
            for cell in row.xpath(".//td|th"):
                cell_text = " ".join(cell.xpath(".//text()").extract()).strip()
                data.append(cell_text)
            rows.append(data)
        return rows


def setup_crawler():
    """
    Setups Scrapy crawler
    """
    process = CrawlerProcess()
    process.crawl(F1Scraper)
    return process
