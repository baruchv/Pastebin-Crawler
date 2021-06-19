from datetime import datetime
from typing import Tuple

import arrow
from arrow import Arrow


class Paste:

    def __init__(self, author: str, title: str, content: str, date: Arrow, paste_id: str) -> None:
        self._author = author
        self._title = title
        self._content = content
        self._date = date.datetime
        self._paste_id = paste_id

    def get_props_tuple(self) -> Tuple:
        tup = (self._author, self._title, self._date, self._content, self._paste_id)
        return tup

    def __str__(self):
        return f"Author: {self._author} \n title: {self._title} \n content {self._content} \n Posing date: {self._date} \n Paste ID: {self._paste_id}"
