from config import Config
from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas
import logging
from enum import Enum
from bson import ObjectId

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
            logging.info(f'Creating a new DB client with URI: {Config.get("mongodb_uri")} and DB: {Config.get("mongodb_db_name")}')

            DB.__instance = MongoClient(
                Config.get('mongodb_uri')
            )[Config.get('mongodb_db_name')]
            return DB.__instance

def ingest_pdf(row: pandas.Series):
    id = row.id
    fields = row.to_dict()
    fields.pop('id')

    logging.debug(f'Ingesting row:\n{str(row)}')

    DB.get().pdfs.insert({
        '_id': id,
        **fields
    })

    pages = [{
        '_id': f'{id}-{page_nr}',
        'status': PageStatus.NOT_ANNOTATED.value
    } for page_nr in range(1, fields['page_count']+1)]

    DB.get().pages.insert_many(pages, ordered=False)