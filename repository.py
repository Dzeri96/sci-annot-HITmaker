from collections import namedtuple
from typing import Any, NamedTuple
from pymongo.database import Database
from config import Config
from pymongo import MongoClient, DESCENDING, ASCENDING, UpdateOne
import pandas
import logging
from enums.assignment_status import AssignmentStatus
from enums.page_status import PageStatus
from enums.qualification_types import QualificationType
import os
import urllib.request
import numpy as np

QUAL_TYPE_CACHE = {}

class DB:
    __instance: Database

    @staticmethod
    def get():
        if hasattr(DB, '_DB__instance'):
            return DB.__instance
        else:
            logging.debug(f'Creating a new DB client with URI: {Config.get("mongodb_uri")} and DB: {Config.get("mongodb_db_name")}')

            DB.__instance = MongoClient(
                Config.get('mongodb_uri')
            )[Config.get('mongodb_db_name')]
            return DB.__instance

def ingest_pdfs(data: pandas.DataFrame):
    """Ingests a render summary dataframe into the collection pages and pdfs
    
 
    Args:
        data (pandas.DataFrame): Dataframe with the following shape:
            #   Column  Dtype \n
            ---  ------  ----- \n
            0   file    object\n
            1   page_nr int64 \n
            2   width   int64 \n
            3   height  int64 \n
            4   format  object\n
            5   DPI     int64\n

            Where only file and page_nr are required fields.
            Any extra columns are allowed and simply passed to the database.
    """     
    pdf_list = []
    page_list = []

    for index, row in data.iterrows():
        logging.debug(f'Ingesting row: {str(index)}')
        id = row.id
        fields = row.to_dict()
        fields.pop('id')

        pdf_list.append({
            '_id': id,
            **fields
        })
        
        # This is to handle the dumb way pdftoppm names files: 
        # https://gitlab.freedesktop.org/poppler/poppler/-/issues/1172
        nr_digits = len(str(fields['page_count']))
        pages = [{
            '_id': '%s-%0*d' % (id, nr_digits, page_nr),
            'status': PageStatus.NOT_ANNOTATED.value,
            'pdf_id': fields['file']
        } for page_nr in range(1, fields['page_count']+1)]
        page_list.extend(pages)

    logging.debug(f'Inserting {len(pdf_list)} pdfs into the DB...')
    DB.get().pdfs.insert_many(pdf_list, ordered=False)
    logging.debug(f'Inserting {len(page_list)} pages into the DB...')
    DB.get().pages.insert_many(page_list, ordered=False)

def save_hit_type(params: dict):
    if(params['active']):
        # Set all existing HIT types to inactive
        DB.get().hit_types.update_many({}, {'$set': {'active': False}})
    
    DB.get().hit_types.insert_one(params)

def get_active_hit_type_or_by_id(id: str=None)-> dict:
    if (id is not None):
        result = DB.get().hit_types.find({'_id': id, 'environment': Config.get('env_name')})
        logging.debug(f'Returning specific hit type with id {id}')
        return result.next()
    else:
        result = list(DB.get().hit_types.find({'active': True, 'environment': Config.get('env_name')}))
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

        logging.debug(f'Updated: {bulk_results.modified_count} document(s)')

def update_assignment_statuses_from_dict(page_id: str, assignment_id_status_dict: dict[str, AssignmentStatus]):
    if assignment_id_status_dict:
        logging.debug(f'update_assignment_statuses_from_dict called with {assignment_id_status_dict}')
        update_operations = [UpdateOne(
            filter={'assignments': {'$exists': True}, '_id': page_id},
            update={'$set': {'assignments.$[assig].status': status.value, 'assignments.$[assig].reviewed': True}},
            array_filters=[{'assig.assignment_id': {'$eq': assignment_id}}],
            upsert=True
        ) for assignment_id, status in assignment_id_status_dict.items()]
        bulk_results = DB.get().pages.bulk_write(update_operations)
        logging.debug(f'update_assignment_statuses_from_dict updated: {bulk_results.modified_count} document(s)')
        logging.debug(f'update_assignment_statuses_from_dict raw: {bulk_results.bulk_api_result}')
        return bulk_results

def update_pages_from_dict(page_id_ops_dict: dict):
    if page_id_ops_dict:
        update_operations = [UpdateOne(
            {'_id': page_id},
            operations
        ) for page_id, operations in page_id_ops_dict.items()]
        bulk_results = DB.get().pages.bulk_write(update_operations)
        logging.debug(f'update_pages_from_dict updated: {bulk_results.modified_count} document(s)')
        return bulk_results

# TODO: Maybe consolidate with update_pages_from_dict
def update_pages_from_tuples(filter_actions_list: list[tuple]):
    """
        Updates pages by using the first entry in each tuple as a filter, and the second one as the action.
    """
    update_operations = [UpdateOne(
        filters,
        actions
    ) for filters, actions in filter_actions_list]
    if len(update_operations):
        bulk_results = DB.get().pages.bulk_write(update_operations)
        logging.debug(f'Updated: {bulk_results.modified_count} document(s)')
        return bulk_results
    else:
        logging.debug('No update operations provided')
        return None

def get_random_pages_by_status(statuses: list[PageStatus], count: int = None, id_only: bool = False) -> list:
    aggregation_pipeline: list[dict[str, Any]] = [{
        '$match': {
            'status': {'$in': [st.value for st in statuses]}
        }
    }]

    if Config.get('active_page_groups'):
        logging.debug(f'Matching groups {Config.get("active_page_groups")} in get_random_pages_by_status')
        aggregation_pipeline[0]['$match']['group'] = {'$in': Config.get('active_page_groups')}

    if count:
        aggregation_pipeline.append({
            '$sample': {
                'size': count
            }
        })

    if id_only:
        aggregation_pipeline.append({
            '$project': {
                '_id': 1
            }
        })

    result = list(DB.get().pages.aggregate(aggregation_pipeline))

    if(len(result) != 0):
        return result
    else:
        raise LookupError(f'There are no more pages in any of these statuses: {[status.value for status in statuses]}!')

def get_pages_in_id_list(ids: list[str]) -> list[dict]:
    result = []
    if ids:
        result = DB.get().pages.find({'_id': {'$in': ids}})
    return list(result)

def get_page_by_id(id: str) -> dict:
    result = DB.get().pages.find_one({'_id': id})
    logging.debug(f'get_page_by_id result: {result}')

    if result:
        return result
    else:
        raise LookupError(f'Page with id {id} not found!')

def get_assignment(page_id: str, assignment_id: str):
    result = DB.get().pages.find_one(
        {'_id': page_id, 'assignments.assignment_id': assignment_id},
        {"assignments": {'$elemMatch': {'assignment_id': assignment_id}}}
    )

    if result:
        return result['assignments'][0]
    else:
        raise LookupError(f'Assignment {assignment_id} in page {page_id} not found!')

def get_status_counts() -> list[dict[str, int]]:
    pipeline = [
        {
            '$group': {
                '_id': '$status', 
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'status': '$_id', 
                'count': 1
            }
        }, {
            '$sort': {
                'count': 1
            }
        }
    ]
    if Config.get('active_page_groups'):
        logging.debug(f'Matching groups {Config.get("active_page_groups")} in get_status_counts')
        pipeline.insert(0, {
            '$match': {
                'group': {'$in': Config.get('active_page_groups')}
            }
        })

    result = DB.get().pages.aggregate(pipeline)

    return list(result)

# TODO: Maybe add group filter
def get_accepted_assignments(exclude_ids: list[str]):
    """
        Returns objects in the shape of {id_: page id, status: page status, assignment: selected assignment}

        The assignment is selected as follows: If the page is in status VERIFIED, the accepted_assignment_id is used.
        Otherwise, if the page status is REVIEWED, the last assignment from the array is selected.
    """
    pipeline = [
        {
            '$match': {
                '_id': {
                    '$nin': exclude_ids
                }, 
                'status': {
                    '$in': [PageStatus.REVIEWED.value, PageStatus.VERIFIED.value]
                }
            }
        }, {
            '$project': {
                'assignment(s)': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$status', PageStatus.VERIFIED.value
                            ]
                        }, 
                        'then': {
                            '$filter': {
                                'input': '$assignments', 
                                'as': 'assig', 
                                'cond': {
                                    '$eq': [
                                        '$$assig.assignment_id', '$accepted_assignment_id'
                                    ]
                                }
                            }
                        }, 
                        'else': {
                            '$last': '$assignments'
                        }
                    }
                }, 
                'status': 1
            }
        }, {
            '$project': {
                'assignment': {
                    '$cond': {
                        'if': {
                            '$isArray': '$assignment(s)'
                        }, 
                        'then': {
                            '$first': '$assignment(s)'
                        }, 
                        'else': '$assignment(s)'
                    }
                }, 
                'status': 1
            }
        }
    ]
    if Config.get('active_page_groups'):
        logging.debug(f'Matching groups {Config.get("active_page_groups")} in get_accepted_assignments')
        pipeline[0]['$match']['group'] = {'$in': Config.get('active_page_groups')}

    result = DB.get().pages.aggregate(pipeline)
    return result

def save_qual_requirement(keys: dict):
    keys['env'] = Config.get('env_name')
    keys['_id'] = keys['QualificationTypeId']
    DB.get().qual_requirements.insert_one(keys)

def get_qual_type_id(req: QualificationType):
    """
        Returns the qual. req. id which corresponds to the provided name and current env_name,
            or None if it doesn't exist

        The id is cached after the first fetch from the DB.
    """
    if req.value['Name'] in QUAL_TYPE_CACHE.keys():
        return QUAL_TYPE_CACHE[req.value['Name']]
    else:
        search_res = DB.get().qual_requirements.find_one({
            'Name': req.value['Name'],
            'env': Config.get('env_name')
        })
        if search_res:
            QUAL_TYPE_CACHE[req.value['Name']] = search_res['_id']
            return search_res['_id']

def assert_qual_types_exist():
    for qual_type in QualificationType:
        result = get_qual_type_id(qual_type)
        if result is None:
            raise Exception(f'Qualification type {qual_type} is not present in the DB! Please first create it.')

def get_qualification_pages():
    result = DB.get().pages.find(
        {'qualification_page': {'$exists': True, '$eq': True}}
    )
    return list(result)

def get_workers_in_id_list(ids: list[str]):
    result = DB.get().workers.find(
        {'_id': {'$in': ids}}
    )
    return result

def update_workers_from_dict(worker_id_ops_dict: dict):
    if worker_id_ops_dict:
        update_operations = [UpdateOne(
            {'_id': worker_id},
            operations,
            upsert=True
        ) for worker_id, operations in worker_id_ops_dict.items()]
        bulk_results = DB.get().workers.bulk_write(update_operations)
        logging.debug(f'Updated: {bulk_results.modified_count} document(s)')
        return bulk_results

def get_image_as_bytes(page_id) -> bytes:
    """Takes a page id and returns the bytes of the rasterized image.
    If the file is not available locally, it fetches it from image_url_base,
    and saves the response before returning the data.

    Args:
        page_id (_type_): ID of the rasterized page

    Returns:
        bytes: Image as bytes
    """

    img_path = Config.get('image_folder') + page_id + Config.get('image_extension')
    if not os.path.isfile(img_path):
        data: bytes = urllib.request.urlopen(Config.get('image_url_base') + page_id + Config.get('image_extension')).read()
        open(img_path, 'wb').write(data)
        logging.warning(f'Image not found on path {img_path}. Instead, it was downloaded from image_url_base. Consider downloading the rasterized pages locally for better performance.')
    else:
        data = open(img_path, 'rb').read()
    return data

class WorkerPointsBucket(NamedTuple):
    begin: float
    end: float
    count: int

def get_worker_verification_points_distribution(nr_buckets= 10) -> list[WorkerPointsBucket]:
    min_max_res = DB.get().workers.aggregate([
        {
            '$match': {
                '$or': [
                    {
                        'env': None
                    }, {
                        'env': Config.get('env_name')
                    }
                ]
            }
        },
        {
            '$group': {
                'max': {
                    '$max': '$verification_points'
                }, 
                'min': {
                    '$min': '$verification_points'
                }, 
                '_id': None
            }
        }
    ]).next()
    max_points = min_max_res['max']
    min_points = min_max_res['min']
    bucket_boundaries = np.linspace(min_points, max_points+1, nr_buckets).tolist()

    bucket_response =  DB.get().workers.aggregate([
        {
            '$match': {
                '$or': [
                    {
                        'env': None
                    }, {
                        'env': Config.get('env_name')
                    }
                ]
            }
        },
        {
            '$bucket': {
                'groupBy': '$verification_points', 
                'boundaries': bucket_boundaries, 
                'output': {
                    'count': {
                        '$sum': 1
                    }
                }
            }
        }
    ])
    bucket_begin_size_map = {res['_id']:res['count'] for res in bucket_response}
    buckets: list[WorkerPointsBucket] = []
    for i in range(len(bucket_boundaries) - 1):
        begin = bucket_boundaries[i]
        end = bucket_boundaries[i+1]
        count = bucket_begin_size_map[begin] if (begin in bucket_begin_size_map) else 0
        buckets.append(WorkerPointsBucket(begin, end, count))
    return buckets