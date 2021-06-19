import os
from typing import List
import pyodbc
from src.entities.paste import Paste

DRIVER = 'Microsoft Access Driver (*.mdb, *.accdb)'
DB_PATH = os.path.join('C:\\Users\\baruc\\Desktop\\Exercise', 'db.accdb')


class PastesRepository:
    instance = None

    def __init__(self) -> None:
        assert PastesRepository.instance is None, "Error: Can't generate more than one instance of PasteRepository"
        PastesRepository.instance = self

    @classmethod
    def get_instance(cls):
        if cls.instance:
            return cls.instance
        return PastesRepository()

    def __get_connection(self):
        con_string = f'DRIVER={DRIVER};DBQ={DB_PATH};'
        print("Setting a DB connection")
        conn = pyodbc.connect(con_string)
        print("Connected To Database")
        return conn

    def insert_paste_list(self, params: List):
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("Inserting pastes")
        cursor.executemany("INSERT into pastes(author, title, date, content, paste_id) values(?,?,?,?,?)", params)
        cursor.close()

    def get_50_recent_ids(self) -> List[str]:
        """
        Since the recent pastes page on websites contains exactly 50 pastes,
        In order to check whether a recieved paste is relevant for persisting, we only need to look at the last 50 in DB.
        :return: A list of the last 50 pastes ids.
        """
        conn = self.__get_connection()
        cursor = conn.cursor()
        print("Querying ids from DB")
        rows = cursor.execute("SELECT TOP 50 paste_id from pastes ORDER BY pastes.id DESC").fetchall()
        id_list = [tup[0] for tup in rows]
        return id_list

    def get_all_pastes(self) -> List[Paste]:
        pass

    def get_paste_by_paste_id(self, paste_io: str) -> Paste:
        pass
