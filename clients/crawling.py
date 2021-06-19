import threading
from threading import Thread
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

HUMENIZED_DICT = {
    '1 hour ago': 'an hour ago',
    '1 min ago': 'a minute ago',
    '1 sec ago': 'a second ago'
}
LINK_PATH = '//tr/td[1]/a'
DATE_PATH = '//tr/td[2]'
BASIC_URL = 'https://pastebin.com'
ARCHIVE_URL = 'https://pastebin.com/archive'
AUTHOR_PATH = '//div[@class="username"]/a'
CONTENT_PATH = '//textarea'


class Crawler:

    @property
    def controller(self) -> PastesController:
        return PastesController.get_instance()

    @staticmethod
    def extract_date(td: HtmlElement, time: Arrow) -> Arrow:
        """
        :param td: An html element contains a humanized time record
        :param time: An Arrow objects represents crawling time
        :return: An Arrow objects represent humanized time record
        """
        paste_humanized_date = td.text_content()
        if paste_humanized_date in HUMENIZED_DICT.keys():
            paste_humanized_date = HUMENIZED_DICT[paste_humanized_date]
        paste_humanized_date = paste_humanized_date.replace('min', 'minutes')
        paste_date = time.dehumanize(paste_humanized_date)
        return paste_date

    @classmethod
    def generate_paste(cls, element_tuple: tuple[HtmlElement, HtmlElement, Arrow]) -> Paste:
        """
        This method parses one needed paste, generates a matching Paste object
        :param element_tuple: a Tuple consists of:
                              1 - A link html element contains both title and id
                              2 - A td html element contains posting date
                              3 - An Arrow object represent crawling time
        :return: Paste object
        """
        link = element_tuple[0]
        td = element_tuple[1]
        time = element_tuple[2]
        try:
            print("Generating Paste")
            paste_title = link.text_content()
            paste_date = cls.extract_date(td, time)
            href = link.attrib['href']
            paste_url = BASIC_URL + href
            print(f"Loading page: {paste_url}")
            paste_response = requests.get(paste_url)
            assert paste_response.status_code == 200, f"Error: Page loading process was failed for url: {paste_url} \n message: {paste_response.reason}"
            paste_source = html.fromstring(paste_response.content)
            paste_author = paste_source.xpath(AUTHOR_PATH)[0].text_content()
            paste_content = paste_source.xpath(CONTENT_PATH)[0].text_content()
            return Paste(author=paste_author, title=paste_title, content=paste_content, paste_id=href[1:],
                         date=paste_date)
        except:

            print("Error, ", sys.exc_info(),
                  f"\n occurred during generating paste with paste_id: {link.attrib['href'][1:]}")

    def get_relevant_elements(self, link_td_list: Iterator[tuple]) -> List[Tuple]:
        relevant_list = []
        for link, td in link_td_list:
            paste_id = link.attrib['href']
            if self.controller.is_relevant_pate(paste_id):
                relevant_list.append((link, td))
        return relevant_list

    def crawl(self):
        """
        This method parses new pastes from recent-pastes page on pastebin.com and send them to persistent
        """
        try:
            response = requests.get(ARCHIVE_URL)
            assert response.status_code == 200, f"Error: page loading process was failed for url: {ARCHIVE_URL} \n message: {response.reason}"
            now = arrow.utcnow()
            source_code = html.fromstring(response.content)
            # link_list contains the html elements that contain both paste_id and paste_title
            link_list = source_code.xpath(LINK_PATH)
            # td_list contains the html elements that contains paste's date
            td_list = source_code.xpath(DATE_PATH)
            relevant_link_td_list = self.get_relevant_elements(zip(link_list, td_list))
            executor = ThreadPoolExecutor(10)
            value_list = [(link, td, now) for link, td in relevant_link_td_list]
            paste_list = executor.map(self.generate_paste, value_list)
            executor.shutdown()
            self.controller.insert_paste_list(list(paste_list))
        except:
            print(f"Error: page loading process was failed for url: {ARCHIVE_URL} \n message: {sys.exc_info()}")

    def run(self, loops: int = 3):
        for i in range(loops):
            self.crawl()
            sleep(120)
