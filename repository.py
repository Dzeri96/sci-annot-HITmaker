from numpy import number
from config import Config
from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas
import logging
from enum import Enum
from bson import ObjectId

from mturk_client import Client

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

def get_random_not_annotated(count: int) -> list[str]:
    result = DB.get().pages\
        .find({'status': PageStatus.NOT_ANNOTATED.value}, {'_id': 1})\
        .limit(count)
    result_list =  [page['_id'] for page in result]

    if(len(result_list) != 0):
        return result_list
    else:
        raise Exception('There are no more unpublished pages!')

def save_hit_type(params: dict):
    if(params['active']):
        # Set all existing HIT types to inactive
        DB.get().hit_types.update_many({}, {'$set': {'active': False}})
    
    DB.get().hit_types.insert_one(params)