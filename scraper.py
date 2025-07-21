import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import os
from io import StringIO
import logging
import json

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/scraper.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)


class Scraper:

    def __init__(self):
        self.base_url: str = "https://crs.cookcountyclerkil.gov"
        self.url_templates: list = [
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateRecorded&direction=desc",
            "https://crs.cookcountyclerkil.gov/Search/SortResultByPin?id1={pin}&column=DateRecorded&direction=asc",
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=AlphaDocNumber&direction=desc",
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=AlphaDocNumber&direction=asc",
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateExecuted&direction=desc",
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateExecuted&direction=asc",
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DocTypeDescription&direction=desc",
            "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DocTypeDescription&direction=asc",
        ]
        self.data: dict = {}
        self.data_dir: str = os.getcwd() + "/data"

    def make_snake_case(self, s: str) -> str:
        """Converts a given string to snake_case

        Parameters:
            s (str): An input string

        Returns:
            A string in snake_case
        """
        s = str(s)
        s = [char if char != " " else "_" for char in s]
        return "".join(s).lower()

    def initialize_data_directory(self):
        """
        Ensures that the data directory exists within the project
        """
        if not os.path.exists(self.data_dir):
            logger.info("Initializing data directory")
            os.makedirs(self.data_dir)

    def remove_duplicates(self, urls: list[str]) -> list[str]:
        """Removes duplicate values from a list object.

        Parameters:
            urls (List[str]): A list of strings.

        Returns:
            A list of unique strings.
        """
        urls = list(set(urls))
        return urls

    def clean_pin(self, pin: str) -> str:
        """
        Cleans a Property Identification Number (PIN) value

        Parameters:
            pin (str): A formatted PIN

        Returns:
            A PIN without hyphens.
        """
        assert isinstance(pin, str), "pin must be of type str"
        pin = "".join(filter(str.isdigit, pin))
        assert (
            len(pin) == 14
        ), f"pin value must evaluate to a string of  digits of length 14, instead have {pin}"
        return pin

    def initialize_pin(self, pin: str):
        """
        Create an entry in the data structure for a given pin
        """
        # TODO: Add validation of PIN by checking website
        clean_pin = self.clean_pin(pin)
        self.data[clean_pin] = {"docs": {}, "properties": {}}

    def collect_doc_metadata(self, pin: str) -> list[dict[str]]:
        """
        Collects all of the available URls to document pages associated with a given PIN
        """
        def process_records(row: list) -> list:
            # I'm not sure why the records returned from read_html are
            # structured like this...
            row = [val[0] if val[0] != "View" else self.base_url + val[1]
                   for val in row]
            return row

        doc_metadata = pd.DataFrame()
        expected_nrows = 0  # TODO: This may not be necessary
        for url in self.url_templates:
            logger.info("Querying " + url.format(pin=pin))
            response = requests.get(url.format(pin=pin))
            df = pd.read_html(StringIO(response.text), extract_links="body")[0]
            df = df.apply(lambda x: process_records(x))
            df.rename(lambda x:
                      self.make_snake_case(x),
                      inplace=True, axis='columns')
            expected_nrows += df.shape[0]
            doc_metadata = pd.concat([doc_metadata, df])

        actual_nrows = doc_metadata.shape[0]
        assert actual_nrows == expected_nrows, f"Actual pre-deduplication  \
        records collected ({actual_nrows}) differs from \
        expected ({expected_nrows})."

        doc_metadata = doc_metadata.drop_duplicates()
        doc_metadata = doc_metadata.drop("unnamed:_0", axis=1)
        doc_metadata = doc_metadata.rename({'view_doc': 'url'},
                                           axis='columns')
        doc_metadata = doc_metadata.to_dict(orient='records')
        return doc_metadata

    def extract_from_doc_urls(self):
        '''
        Iterate through the doc_urls collected and download the documents and
        other relevant metadata.
        '''
        for pin in self.data:
            for doc in self.data[pin]['docs']:
                response = requests.get(self.data[pin]['docs'][doc]['url'])
                soup = BeautifulSoup(response.text, features='lxml')
                tag = soup.find("a", href=re.compile("/Document/DisplayPdf"))
                pdf_url = self.base_url + tag.get("href")
                self.data[pin]['docs'][doc]['pdf_url'] = pdf_url
                self.download_pdf(pdf_url, pin, doc)

                info_table = soup.fieldset.table.find_all('tr')
                self.extract_info_table(info_table, pin, doc)

                entities = self.extract_grantor_grantee_tables(soup)
                self.data[pin]['docs'][doc]['entities'] = entities

    def extract_grantor_grantee_tables(self, soup: BeautifulSoup):
        """
        Extracts Grantor and Grantee table data from Cook County Clerk's
        document HTML.

        Returns:
            dict: Dictionary containing 'grantors' and 'grantees' lists
            with extracted data
        """
        def extract_table_data(section_title):
            """Helper function to extract data from a specific table section"""
            # Find the span with the section title
            section_span = soup.find(
                'span', string=section_title, class_='fs-5')
            if not section_span:
                return []

            # Find the table that follows this span
            table = section_span.find_next('table', class_='table')
            if not table:
                return []

            # Extract data from table rows (skip header row)
            data = []
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:  # Ensure we have at least Name and Trust# columns
                        # Extract name (could be a link or plain text)
                        name_cell = cells[0]
                        name_link = name_cell.find('a')
                        name = name_link.get_text(
                            strip=True) if name_link else name_cell.get_text(strip=True)

                        # Extract trust number
                        trust_num = cells[1].get_text(strip=True)

                        data.append({
                            'name': name,
                            'trust_number': trust_num if trust_num else None
                        })

            return data

        # Extract both tables
        grantors = extract_table_data('Grantors')
        grantees = extract_table_data('Grantees')

        return {
            'grantors': grantors,
            'grantees': grantees
        }

    def download_pdf(self, url: str, pin: str, doc_num: str):
        """
        Downloads the document PDF to the data directory

        Parameters:
            url (str): The URL containing the document.
            pin (str): The PIN associated with the document.
            doc_num (str): The document's ID number.

        Returns:
            None
        """
        path = self.data_dir + "/" + pin + "/" + doc_num + '.pdf'
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
            logger.info(f"Created new directory for pin at {
                        os.path.dirname(path)}")
        if os.path.exists(path):
            logger.info(f"{path} already exists, skipping")
            self.data[pin]['docs'][doc_num]['pdf_path'] = path
            return

        logger.info(f"Downloading file at {url}...")
        response = requests.get(url)
        with open(path, "wb") as file:
            file.write(response.content)
            self.data[pin]['docs'][doc_num]['pdf_path'] = path
        assert os.path.exists(path), logger.error(
            f"Download from {url} was not successful to {path}"
        )
        logger.info(f"Document saved to {path}")

    def extract_info_table(self,
                           info_table: BeautifulSoup,
                           pin: str,
                           doc: str):
        '''
        Traverse across info_table and extract any metadata that
        wasn't extracted during the metadata pull.
        '''
        for record in info_table:
            key = record.th.label.string.strip(":")
            key = self.make_snake_case(key)
            value = record.td.string
            if not self.data[pin]['docs'][doc].get(key):
                key = 'doc_page_' + key
                self.data[pin]['docs'][doc][key] = value
                # TODO: This creates duplicate data entire

    def main(self):
        self.initialize_data_directory()

        # pin = "16-10-421-053-0000" # Hotel Guyon
        # pin = "17-05-115-085-0000"  # Starsiak Clothing
        pin = "17-29-304-001-0000"
        pin = self.clean_pin(pin)
        self.initialize_pin(pin)
        pin_doc_metadata = self.collect_doc_metadata(pin)

        for doc in pin_doc_metadata:
            doc_num = doc.pop('doc_number', None)
            self.data[pin]['docs'][doc_num] = doc

        self.extract_from_doc_urls()

        out_path = self.data_dir + '/metadata.json'
        with open(out_path, 'w') as file:
            json.dump(self.data, file, indent=4, sort_keys=True)


if __name__ == "__main__":
    s = Scraper()
    s.main()
