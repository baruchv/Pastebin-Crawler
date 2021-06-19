import sys
from typing import List

from src.entities.paste import Paste
from src.repository.pastes_repository import PastesRepository


class PastesController:

    instance = None

    def __init__(self) -> None:
        super().__init__()
        assert PastesController.instance is None, "Error, PasteController is limited to one instance.\n Please use get_instance method"
        self._recent_pastes_ids = None
        self.update_paste_list()

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = PastesController()
        return cls.instance

    @property
    def repository(self) -> PastesRepository:
        return PastesRepository.get_instance()

    def is_relevant_pate(self, paste_id: str) -> bool:
        return paste_id not in self._recent_pastes_ids

    def update_paste_list(self):
        try:
            print("Updating paste_id list in Controller")
            self._recent_pastes_ids = self.repository.get_50_recent_ids()
        except:
            print("Error, ", sys.exc_info(), f"\n occurred during updating paste_id list")

    def insert_paste_list(self, pastes: List[Paste]):
        try:
            print("Sending paste_list to repository")
            values = [paste.get_props_tuple() for paste in pastes]
            self.repository.insert_paste_list(values)
            self.update_paste_list()
        except:
            print("Error, ", sys.exc_info(), f"\n occurred during inserting Paste list \n {pastes}")
