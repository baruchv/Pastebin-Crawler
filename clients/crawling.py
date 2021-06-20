import datetime
from time import sleep
from typing import List, Tuple, Iterator

import requests
from concurrent.futures import ThreadPoolExecutor
from arrow import Arrow
from lxml import html
import arrow
from lxml.html import HtmlElement
import sys

from src.controllers.paste_controller import PastesController
from src.entities.paste import Paste

LINK_PATH = '//tr/td[1]/a/@href'
DATE_PATH = '//div[@class="date"]/span/@title'
BASIC_URL = 'https://pastebin.com'
ARCHIVE_URL = 'https://pastebin.com/archive'
AUTHOR_PATH = '//div[@class="username"]/a/text()'
CONTENT_PATH = '//textarea/text()'
TITLE_PATH = '//h1/text()'


class Crawler:

    @property
    def controller(self) -> PastesController:
        return PastesController.get_instance()

    @staticmethod
    def extract_date(date_str: str) -> Arrow:
        """
        :param date_str: str represents the paste posting date
        :return: An Arrow objects represent humanized time record
        """
        date_str = date_str.replace('of', '')
        date_str = date_str.replace('CDT', '+0500')
        paste_date = arrow.get(date_str, 'dddd Do  MMMM YYYY hh:mm:ss A Z')
        return paste_date

    @classmethod
    def generate_paste(cls, href: str) -> Paste:
        """
        This method parses one needed paste, generates a matching Paste object
        :param href: A link to specific paste
        :return: Paste object
        """
        try:
            print("Generating Paste")
            paste_url = BASIC_URL + href
            print(f"Loading page: {paste_url}")
            paste_response = requests.get(paste_url)
            assert paste_response.status_code == 200, f"Error: Page loading process was failed for url: {paste_url} \n message: {paste_response.reason}"
            paste_source = html.fromstring(paste_response.content)
            paste_author = paste_source.xpath(AUTHOR_PATH)[0]
            paste_title = paste_source.xpath(TITLE_PATH)[0]
            paste_content = paste_source.xpath(CONTENT_PATH)[0]
            date_str = paste_source.xpath(DATE_PATH)[0]
            paste_date = cls.extract_date(date_str)
            paste = Paste(author=paste_author, title=paste_title, content=paste_content, paste_id=href[1:],
                          date=paste_date)
            print(f"Paste generated: \n {paste}")
            return paste
        except:

            print("Error, ", sys.exc_info(),
                  f"\n occurred during generating paste with paste_id: {href[1:]}")

    def get_relevant_links(self, href_list: List[str]) -> List[str]:
        relevant_list = []
        for href in href_list:
            paste_id = href[1:]
            if self.controller.is_relevant_pate(paste_id):
                relevant_list.append(href)
        return relevant_list

    def crawl(self):
        """
        This method parses new pastes from recent-pastes page on pastebin.com and send them to persistent
        """
        try:
            print(f"Requesting page {ARCHIVE_URL}")
            response = requests.get(ARCHIVE_URL)
            assert response.status_code == 200, f"Error: page loading process was failed for url: {ARCHIVE_URL} \n message: {response.reason}"
            source_code = html.fromstring(response.content)
            # href_list contains the href's that link to the specific pastes
            href_list = source_code.xpath(LINK_PATH)
            relevant_href_list = self.get_relevant_links(href_list)
            print("Starts parsing relevant pastes")
            executor = ThreadPoolExecutor(8)
            paste_list = executor.map(self.generate_paste, relevant_href_list)
            executor.shutdown()
            self.controller.insert_paste_list(list(paste_list))
        except:
            print(f"Error: page loading process was failed for url: {ARCHIVE_URL} \n message: {sys.exc_info()}")

    def timer_task(self, loops: int = 3):
        print("Start running")
        for i in range(loops):
            start = datetime.datetime.now()
            print(f"Starts crawl number {i}")
            self.crawl()
            end = datetime.datetime.now()
            duration = (end - start).total_seconds()
            sleep(120 - duration)


def main():
    Crawler().timer_task(loops=5)

if __name__ == '__main__':
    main()
