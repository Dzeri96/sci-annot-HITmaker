import logging
from typing import cast
import coloredlogs
import argparse
import os
import json

from sci_annot_eval.common.bounding_box import RelativeBoundingBox
from config import Config
import pandas as pd
from enums.page_status import PageStatus
from enums.qualification_requirements import QualificationRequirement, init_qual_enum_values
import repository
import mturk_client
from question_form_answers_parser import xml_to_dict, sci_annot_parsers_dict
from sci_annot_eval import evaluation
from sci_annot_eval.parsers import sci_annot_parser
from django.core import management
from django.core.wsgi import get_wsgi_application
from sci_annot_eval.helpers import helpers

answer_parser = sci_annot_parser.SciAnnotParser()

def ingest(path):
    data = pd.read_parquet(path)
    logging.info(f'Ingesting {path} into database...')

    for index, row in data.iterrows():
        repository.ingest_pdf(row)
    
    logging.info(f'Finished ingesting {data.shape[0]} rows')
    
def create_hit_type(active: bool = False):
    params = {
        'title': Config.get('HIT_type_title'),
        'keywords': Config.get('HIT_type_keywords'),
        'description': Config.get('HIT_type_description'),
        'reward': Config.get('HIT_type_reward'),
        'duration_sec': int(Config.get('HIT_type_duration_sec')),
        'auto_approval_delay_sec': int(Config.get('HIT_type_auto_approval_delay_sec'))
    }

    id = mturk_client.create_hit_type(**params)
    params['_id'] = id
    params['active'] = active

    logging.debug(f'Saving HIT type with params: {params}')
    repository.save_hit_type(params)
    logging.info(f'Created HIT type with params: {params}')

def publish_random(count: int, comment: str = None):
    unpublished = repository.get_pages_by_status(PageStatus.NOT_ANNOTATED, count, True)
    publish([page['_id'] for page in unpublished], comment)

def publish(ids: list[str], comment: str = None):
    active_hit_type = repository.get_active_hit_type_or_by_id()
    
    logging.info(f'Active hit type: {active_hit_type}')
    if str(Config.get('accept_prompts')) != 'True':
        price = float(active_hit_type['reward']) * len(ids)
        answer = input(f'This action will cost you {price}$. Are you sure you want to proceed? (N/y)? ')
        if(answer != 'y'):
            print('Cancelling action...')
            return

    page_id_HIT_response_map = {}
    for page in ids:
        img_url = Config.get('image_url_base') + page + Config.get('image_extension')
        try:
            response = mturk_client.create_hit(active_hit_type['_id'], img_url, comment)
            if(response['ResponseMetadata']['HTTPStatusCode'] == 200):
                logging.debug(f'Created hit: {response}')
                page_id_HIT_response_map[page] = response
            else:
                logging.error(f'Could not create HIT. Response was: {response}')
                break
        except Exception as e:
            logging.error(f'Could not create HIT. Exception was: "{e}"')
            break
    
    repository.update_pages_to_submitted(page_id_HIT_response_map)

def fetch_hit_results():
    submitted_pages = repository.get_pages_by_status(PageStatus.SUBMITTED)
    nr_found_pages = len(submitted_pages)
    logging.info(f'Found {nr_found_pages} submitted pages.')

    operation_dict = {}
    for page in submitted_pages:
        latest_hit_id = page['HIT_ids'][-1]
        status = mturk_client.get_HIT_status(latest_hit_id)
        if (status['HIT']['HITStatus'] == 'Reviewable'):
            nr_assignments_available = status['HIT']['NumberOfAssignmentsAvailable']
            status = PageStatus.RETRIEVED.value if nr_assignments_available == 0 else PageStatus.EXPIRED.value

            result_response = mturk_client.get_HIT_results(latest_hit_id)
            nr_results = result_response['NumResults']
            parsed_assignments = []
            if (nr_results > 0):
                for assignment in result_response['Assignments']:
                    parsed_assignments.append({
                        'assignment_id': assignment['AssignmentId'],
                        'worker_id': assignment['WorkerId'],
                        'HIT_id': assignment['HITId'],
                        'auto_approval_time': assignment['AutoApprovalTime'],
                        'submit_time': assignment['SubmitTime'],
                        'answer': xml_to_dict(assignment['Answer'], sci_annot_parsers_dict)
                    })

            operation_dict[page['_id']] = {
                '$set': {
                    'status': status
                }
            }
            if parsed_assignments:
                operation_dict[page['_id']]['$push'] = {
                    'assignments': {'$each': parsed_assignments}
                }
    
    repository.update_pages_from_dict(operation_dict)

def eval_retrieved():
    retrieved = repository.get_pages_by_status(PageStatus.RETRIEVED)
    passed = []
    deferred = []
    rejected = []
    for page in retrieved:
        assignments = page['assignments']
        nr_assignments = len(assignments)
        if (nr_assignments == 0):
            rejected.append(page['_id'])
            logging.warning(f'page {page["_id"]} has no assignments!')
        elif (nr_assignments == 1):
            deferred.append(page['_id'])
        else:
            if (len(assignments) > 2):
                logging.warning(f'page {page["_id"]} has {len(assignments)} assignments! Only the last two will be evaluated')
            answer_1_raw = page['assignments'][-2]['answer']
            answer_2_raw = page['assignments'][-1]['answer']
            img_path = Config.get('image_folder') + page['_id'] + Config.get('image_extension')
            answer_1_parsed = cast(list[RelativeBoundingBox], answer_parser.parse_dict(answer_1_raw, False))
            answer_1_parsed = helpers.make_absolute(helpers.crop_all_to_content(img_path, answer_1_parsed), answer_1_raw['canvasWidth'], answer_1_raw['canvasHeight'])
            answer_2_parsed = cast(list[RelativeBoundingBox], answer_parser.parse_dict(answer_2_raw, False))
            answer_2_parsed = helpers.make_absolute(helpers.crop_all_to_content(img_path, answer_2_parsed), answer_2_raw['canvasWidth'], answer_2_raw['canvasHeight'])
            match = evaluation.check_no_disagreements(answer_1_parsed, answer_2_parsed, 0.95)
            if match:
                logging.debug(f'page {page["_id"]} has matching annotations')
                passed.append(page['_id'])
            else:
                logging.debug(f'page {page["_id"]} doesn\'t have matching annotations')
                deferred.append(page['_id'])

    logging.info(f'Validation results: {len(passed)} - good, {len(deferred)} - deferred, {len(rejected)} - rejected.')

    repository.update_pages_from_dict({id:{'$set': {'status': PageStatus.REVIEWED.value}} for id in passed})
    repository.update_pages_from_dict({id:{'$set': {'status': PageStatus.DEFERRED.value}} for id in deferred})
    repository.update_pages_from_dict({id:{'$set': {'status': PageStatus.REJECTED.value}} for id in rejected})

def start_server():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web.settings')
    application = get_wsgi_application()
    management.call_command('runserver')

def save_answers(output_dir: str):
    existing_ids = [file.split('.')[0] for file in os.listdir(output_dir)]
    accepted_assignments = repository.get_accepted_assignments(existing_ids)
    summary_dict = {}

    nr_files = 0
    for assignment in accepted_assignments:
        file_path = os.path.join(output_dir, assignment['_id']+'.json')
        summary_dict[assignment['_id']] = [
            assignment['status'],
            assignment['assignment']['worker_id']
        ]
        with open(file_path, 'w+') as of:
            json.dump(assignment['assignment']['answer'], of, indent=4)
        nr_files+= 1
    logging.info(f'Saved {nr_files} assignments to disk.')

    export_summary_path = os.path.join(output_dir, 'export_summary.parquet')
    if os.path.isfile(export_summary_path):
        logging.debug('Existing summary found, appending to it...')
        new_summary_df = pd.DataFrame.from_dict(summary_dict, orient='index', columns=['status', 'worker_id'])
        summary_df = pd.read_parquet(export_summary_path)
        summary_df = new_summary_df.combine_first(summary_df)

    else:
        summary_df = pd.DataFrame.from_dict(summary_dict, orient='index', columns=['status', 'worker_id'])
    summary_df.to_parquet(export_summary_path)

def create_qual_requirements():
    created_nr = 0
    for qual in QualificationRequirement:
        logging.debug(f'Creating qual: {qual}')
        if not repository.get_qual_requirement_id(qual):
            keys = mturk_client.create_qual_type(qual)
            repository.save_qual_requirement(keys)
            created_nr += 1
    
    if created_nr:
        logging.info(f'Created {created_nr} qualification requirement(s)')
    else:
        logging.warning(f'All qualification requirements already exist!')

    

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Amazon MTurk HIT client')
    parser.add_argument('--env', '-e', nargs=1, default='.env', help='Environment file', metavar='file')
    parser.add_argument('--create-hit-type', '-t', type=bool, nargs=1, default=False, help='Create new HIT type and optionally set it as the active one', metavar='active')
    parser.add_argument('--ingest', '-i', nargs=1, help='Parquet file with pdf info', metavar='file')
    parser.add_argument('--publish-random', '-p', nargs=1, help='Publish a certain number of HITs from the pool of unpublished pages', metavar='count', type=int)
    parser.add_argument('--publish-specific', '-P', nargs='+', help='Publish a list of pages denoted by space-separated IDs', metavar='IDs', type=str)
    parser.add_argument('--verbose', '-v', help='Enable verbose logging (info, debug)', action='count')
    parser.add_argument('--accept-prompts', '-y', help='Say yes to any prompts (UNSAFE)', action='store_true')
    parser.add_argument('--fetch-results', '-f', help='Fetch results of published HITs', action='store_true')
    parser.add_argument('--evaluate-retrieved', '-E', help='Check inter-annotator agreement of retrieved annotations', action='store_true')
    parser.add_argument('--start-server', '-s', help='Start annotation inspection webserver', action='store_true')
    parser.add_argument('--comment', '-c', help='Pass comment to created HIT that will be saved in the answer', metavar='COMMENT', type=str)
    parser.add_argument('--save-answers', '-S', help='Save answers to individual json files and additionally save a summary.', metavar='OUTPUT_DIR', nargs=1)
    parser.add_argument('--create-qual-reqs', '-q', help='Create qualification requirements', action='store_true')
    

    args = parser.parse_args()
    
    # Initialize env variables in global config
    Config.parse_env_file(args.env[0])

    # Set up logging
    logging_config = {"fmt":'%(asctime)s %(levelname)s: %(message)s', "level": logging.WARNING}
    if(args.verbose == 1):
        logging_config['level'] = logging.INFO
    elif(args.verbose == 2):
        logging_config['level'] = logging.DEBUG
    coloredlogs.install(**logging_config)
    logging.debug('DEBUG LOGGING ENABLED')
    logging.info('INFO LOGGING ENABLED')

    # Init Enums
    init_qual_enum_values()

    # Check for unsafe mode
    if(args.accept_prompts):
        logging.warning('Accepting all prompts! This can cause unwanted money loss!')
        Config.set('accept_prompts', True)

    # Handle arguments
    if(args.ingest):
        ingest(args.ingest[0])
    elif(args.publish_random):
        comment = None
        if(args.comment):
            comment = args.comment
        publish_random(args.publish_random[0], comment)
    elif(args.create_hit_type):
        create_hit_type(args.create_hit_type[0])
    elif(args.fetch_results):
        fetch_hit_results()
    elif(args.evaluate_retrieved):
        eval_retrieved()
    elif(args.publish_specific):
        publish(args.publish_specific)
    elif(args.start_server):
        start_server()
        print()
    elif(args.save_answers):
        save_answers(args.save_answers[0])
    elif(args.create_qual_reqs):
        create_qual_requirements()