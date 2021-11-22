from config import Config
from pymongo import MongoClient, DESCENDING, ASCENDING, UpdateOne
import pandas
import logging
from page_status import PageStatus

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

    # This is to handle the dumb way pdftoppm names files: 
    # https://gitlab.freedesktop.org/poppler/poppler/-/issues/1172
    nr_digits = len(str(fields['page_count']))
    pages = [{
        '_id': '%s-%0*d' % (id, nr_digits, page_nr),
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
    if page_HIT_id_map:
        update_operations = [UpdateOne(
            {"_id": page_id},
            {  
                "$set": {'status': PageStatus.SUBMITTED.value}, 
                '$push': {
                    'HIT_ids': response['HIT']['HITId'],
                    'published': response['HIT']['CreationTime']
                }
            }
        ) for page_id, response in page_HIT_id_map.items()]
        bulk_results = DB.get().pages.bulk_write(update_operations)

        logging.debug(f'Updated: {bulk_results.modified_count} documents')

def update_pages_from_dict(page_id_ops_dict: dict):
    if page_id_ops_dict:
        update_operations = [UpdateOne(
            {'_id': page_id},
            operations
        ) for page_id, operations in page_id_ops_dict.items()]
        bulk_results = DB.get().pages.bulk_write(update_operations)
        logging.debug(f'Updated: {bulk_results.modified_count} documents')

def get_pages_by_status(status: PageStatus) -> list:
    result = DB.get().pages.find({'status': status.value})
    return list(result)

def get_pages_in_id_list(ids: list[str]) -> list[dict]:
    result = []
    if ids:
        result = DB.get().pages.find({'_id': {'$in': ids}})
    return result

def get_assignment(page_id: str, assignment_id: str):
    result = DB.get().pages.find_one(
        {'_id': page_id, 'assignments.assignment_id': assignment_id},
        {"assignments": {'$elemMatch': {'assignment_id': assignment_id}}}
    )

    if result:
        return result['assignments'][0]
    else:
        raise LookupError(f'Assignment {assignment_id} in page {page_id} not found!')