import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import os
from io import StringIO
import time
import logging
import csv
from typing import List, Dict

logger = logging.getLogger(__name__)
logging.basicConfig(filename=os.path.curdir + "/deed_scraper.log", format='%(asctime)s %(message)s', encoding='utf-8', level=logging.INFO)

class Scraper:
    def __init__(self):
        self.base_url = 'https://crs.cookcountyclerkil.gov'
        self.url_templates = {
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateRecorded&direction=desc",
        "https://crs.cookcountyclerkil.gov/Search/SortResultByPin?id1={pin}&column=DateRecorded&direction=asc",
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=AlphaDocNumber&direction=desc",
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=AlphaDocNumber&direction=asc",
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateExecuted&direction=desc",
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateExecuted&direction=asc",
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DocTypeDescription&direction=desc",
        "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DocTypeDescription&direction=asc"
        }
        self.data = {}
        self.data_dir = os.getcwd() + '/data'
        self.metadata_dir = self.data_dir + '/doc_metadata.csv'

        # initialize data directories
        if not os.path.exists(self.data_dir):
            logger.info("Initializing data directory")
            os.makedirs(self.data_dir)

        if not os.path.exists(self.metadata_dir):
            headers = ['document_number','document_type','date_recorded','date_executed','#_of_pages','address','consideration_amount', 'pin', 'download_url','path']
            with open(self.metadata_dir, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)


    def _make_snake_case(self, s: str) -> str:
        '''Converts a given string to snake_case
        
        Parameters: 
            s (str): An input string
        
        Returns: 
            A string in snake_case
        '''
        s = [char if char != " " else "_" for char in s]
        return "".join(s).lower()

    def _clean_pin(self, pin: str) -> str:
        '''Cleans a Property Identification Number (PIN) value so that it can be used in the base URLs.

        Parameters:
            pin (str): An input PIN

        Returns:
            A PIN without hyphens.
        '''
        assert isinstance(pin, str), "pin must be of type str"
        pin = "".join(filter(str.isdigit, pin))
        assert len(pin) == 14, "pin value must evaluate to a string of numeric digits of length 14"
        return pin
    
    def _remove_duplicates(self, urls: List[str]) -> List[str]:
        '''Removes duplicate values from a list object.

        Parameters:
            urls (List[str]): A list of strings.

        Returns:
            A list of unique strings.
        '''
        urls = list(set(urls))
        return urls

    def get_pin_docs(self, pin: str):
        '''Runs the scraping process for a given PIN.

        Parameters:
            pin (str): A PIN to scrape data for.

        Returns:
            None
        '''
        clean_pin = self._clean_pin(pin)
        logger.info(f"Scraping for {clean_pin}...")
        self.initialize_pin(clean_pin)
        self.get_doc_view_links(clean_pin)
        self.get_documents(clean_pin)
    
    def initialize_pin(self, pin: str):
        '''Initializes the PIN in the Scraper object, adding the given PIN value to the data.

        Parameters:
            pin (str): The PIN to initialize.

        Returns:
            None
        '''
        # TODO: Verify PIN exists on website
        self.data[pin] = {'doc_urls': [], 'documents': {}}

    def get_doc_view_links(self, pin: str) -> List[str]:
        '''Collects and saves a collection of all URLs related to a given PIN on the Cook County Recorder web portal.

        Parameters:
            pin (str): A PIN value.

        Returns:
            collected_urls (List[str]): A List of URLs.
        '''
        collected_urls = []
        for url in self.url_templates:
            logger.info("Querying " + url.format(pin = pin))
            response = requests.get(url.format(pin = pin))
            df = pd.read_html(StringIO(response.text), extract_links='body')[0] # TODO: Why is this a list?
            view_urls = df['View Doc'].apply(lambda x: self.base_url + x[1]).to_list()
            collected_urls.append(view_urls)
        
        collected_urls = [item for sublist in collected_urls for item in sublist]
        collected_urls = self._remove_duplicates(collected_urls)
        self.data[pin]['doc_urls'] = collected_urls
        logger.info(str(len(collected_urls)) + " document urls collected")
        return collected_urls

    def extract_doc_metadata(self, soup: BeautifulSoup, pin: str = "", url: str = "") -> Dict:
        '''Extracts and normalizes document metadata for a given webpage on the Recorder web portal.

        Parameters:
            soup (BeautifulSoup): A BeautifulSoup object
            pin (str): A PIN value
            url (str): The URL to a document page on the Recorder web portal

        Returns:
            A Dict containing metadata for the document found at the given URL 
        '''
        if "Error" in soup.title.string:
            logger.error(f"bad doc url for {pin}, {url}")
            return None
        metadata = {}
        doc_info_table = soup.fieldset.table.find_all("tr")
        for record in doc_info_table:
            key = record.th.label.string.strip(":")
            key = self._make_snake_case(key)
            value = record.td.string
            metadata[key] = value
        if not metadata.get('consideration_amount'): # some docs don't have this field
            metadata['consideration_amount'] = ''
        metadata['pin'] = pin
        # TODO: metadata missing grantor and grantee information found on same page at given url
        logger.info(f"Successfully extracted metadata for {pin}, document {metadata['document_number']}")
        return metadata

    def _save_metadata(self, metadata: Dict):
        '''A helper function to save the metadata to a CSV file.
        
        Parameters:
            metadata (Dict): A dictionary of metadata for a PIN.

        Returns:
            None
        '''
        df = pd.DataFrame(metadata, index=[metadata['document_number']])
        df.to_csv(self.metadata_dir, mode='a', header=False, index=False)

    def get_documents(self, pin: str):
        '''Accesses each URL in self.data[pin]['doc_urls'], extracts all metadata from the page, downloads the document PDF, and saves all of the data to the data directory.

        Parameters:
            pin (str): A PIN string.

        Returns:
            None
        '''
        assert self.data.get(pin).get('doc_urls'), "pin must have pulled view links to pull pdf links. run get_doc_view_links(pin) first."
        
        for url in self.data[pin]['doc_urls']:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, features="lxml")
            tag = soup.find('a', href=re.compile("/Document/DisplayPdf"))
            pdf_url = self.base_url + tag.get('href')
            # Collect all metadata
            metadata = self.extract_doc_metadata(soup, pin, url)
            metadata['download_url'] = pdf_url
            metadata['path'] = self.data_dir + f"/{pin}/{metadata['document_number']}.pdf"
            # Save metadata and download the PDF of the document
            if not self.data[pin]['documents'].get(metadata['document_number']):
                self.data[pin]['documents'][metadata['document_number']] = metadata
                self.download_pdf(metadata['download_url'], metadata['path'])
            else:
                logger.info(f"Duplicate document at {url}, skipping...")
            self._save_metadata(metadata)

    def download_pdf(self, url: str, path: os.PathLike):
        '''Downloads the document PDF from the given URL and saves it to the given path.

        Parameters:
            url (str): The URL containing the document. 
            path (PathLike): The path to save the document to.

        Returns:
            None
        '''
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
            logger.info(f"Created new directory for pin at {os.path.dirname(path)}")
        if os.path.exists(path):
            logger.info(f"{path} already exists, skipping")
            return None
        
        logger.info(f"Downloading file at {url}...")
        response = requests.get(url)
        with open(path, 'wb') as file:
            file.write(response.content)
        assert os.path.exists(path), logger.error(f"Download was not successful to {path}")
            
if __name__ == "__main__":
    pin_to_pull = "16-10-421-053-0000"
    scraper = Scraper()
    scraper.get_pin_docs(pin_to_pull)