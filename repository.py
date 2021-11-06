from config import Config
from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas
import logging
from enum import Enum

class PageStatus(Enum):
    NOT_ANNOTATED = 'NOT_ANNOTATED'
    SUBMITTED = 'SUBMITTED'
    RETRIEVED = 'RETRIEVED'
    REVIEWED  = 'REVIEWED' 

class DB:
    __instance: MongoClient = None

    @staticmethod
    def get():
        if DB.__instance != None:
            return DB.__instance
        else:
            logging.info(f'Creating a new DB client with URI: {Config.get("MFLIX_DB_URI")} and DB: {Config.get("MFLIX_DB_NAME")}')

            DB.__instance = MongoClient(
                Config.get('MFLIX_DB_URI')
            )[Config.get('MFLIX_DB_NAME')]
            return DB.__instance

def ingest_pdf(row: pandas.Series):
    id = row.id
    fields = row.to_dict()
    fields.pop('id')

    DB.get().pdfs.insert({
        '_id': id,
        **fields
    })

    pages = [{
        '_id': f'{id}-{page_nr}',
        'status': PageStatus.NOT_ANNOTATED
    } for page_nr in range(1, fields['page_count']+1)]

    DB.get().pages.insert_many(pages, {'ordered': False})