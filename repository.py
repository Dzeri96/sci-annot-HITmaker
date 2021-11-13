from numpy import number
from config import Config
from pymongo import MongoClient, DESCENDING, ASCENDING, UpdateOne
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
            logging.debug(f'Creating a new DB client with URI: {Config.get("mongodb_uri")} and DB: {Config.get("mongodb_db_name")}')

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
        .aggregate([
            {
                '$match': {
                    'status': PageStatus.NOT_ANNOTATED.value
                }
            }, {
                '$sample': {
                    'size': count
                }
            }, {
                '$project': {
                    '_id': 1
                }
            }
        ])
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

def get_active_hit_type_or_by_id(id: str=None):
    if (id is not None):
        result = DB.get().hit_types.find({'_id': id})
        logging.debug(f'Returning specific hit type with id {id}')
        return result[0]
    else:
        result = list(DB.get().hit_types.find({'active': True}))
        if(len(result) > 1):
            logging.warning('DB has more than one active HIT type!')
        return result[0]

def update_pages_to_submitted(page_HIT_id_map: dict):
    if(len(page_HIT_id_map) != 0):
        update_operations = [UpdateOne(
            {"_id": page_id},
            {"$set": {'status': PageStatus.SUBMITTED.value}, '$push': {'HIT_ids': HIT_id}}
        ) for page_id, HIT_id in page_HIT_id_map.items()]
        bulk_results = DB.get().pages.bulk_write(update_operations)

        logging.debug(f'update_pages_to_submitted bulk update response: {bulk_results}')