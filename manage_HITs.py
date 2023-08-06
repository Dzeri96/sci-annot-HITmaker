from collections import Counter
import logging
from typing import cast, Optional
import coloredlogs
import argparse
import os
import json
from sci_annot_eval.common.bounding_box import AbsoluteBoundingBox
from sci_annot_eval.exporters import sci_annot_exporter
answer_exporter = sci_annot_exporter.SciAnnotExporter()

from config import Config

args = argparse.Namespace()

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Amazon MTurk HIT client')
    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument('--env', '-e', nargs=1, default='.env', help='Environment file (default is .env)', metavar='file')
    parser.add_argument('--verbose', '-v', help='Enable verbose logging (info, debug)', action='count')
    parser.add_argument('--accept-prompts', '-y', help='Say yes to any prompts (UNSAFE)', action='store_true')

    publish_random_parser = subparsers.add_parser(
        'publish-random',
        description='Publish a certain number of HITs from the pool of unpublished pages',
        argument_default=argparse.SUPPRESS
    )
    publish_random_parser.add_argument('count', help='Number of pages', metavar='COUNT', type=int)
    publish_random_parser.add_argument('--comment', '-c', help='Pass comment to created HIT that will be saved in the answer', metavar='COMMENT', type=str, required=False)
    publish_random_parser.add_argument('--minimum-qual-points', '-m', help='The minimum number of qual. points that a turker needs in order to work on these HITs. (default is no requirement)', type=int, default=0)
    publish_random_parser.add_argument('--maximum-qual-points', help='The maximum number of qual. points that a turker needs in order to work on these HITs. (default is no requirement)', type=int, default=0)
    publish_random_parser.add_argument('--require-qualification-done', '-q', help='Indicates that turkers need to have done at least one qualification to work on these HITs.', action='store_true', default=False)

    publish_specific_parser = subparsers.add_parser(
        'publish-specific',
        description='Publish a list of specific pages as HITs',
        argument_default=argparse.SUPPRESS
    )
    publish_specific_parser.add_argument('ids', metavar='IDs', nargs='+', help='Space-separated list of Page IDs to publish')
    publish_specific_parser.add_argument('--comment', '-c', help='Pass comment to created HIT that will be saved in the answer', metavar='COMMENT', type=str, required=False)
    publish_specific_parser.add_argument('--minimum-qual-points', '-m', help='The minimum number of qual. points that a turker needs in order to work on these HITs. (default is 0)', type=int, default=0)
    publish_specific_parser.add_argument('--maximum-qual-points', help='The maximum number of qual. points that a turker needs in order to work on these HITs. (default is no requirement)', type=int, default=0)
    publish_specific_parser.add_argument('--require-qualification-done', '-q', help='Indicates that turkers need to have done at least one qualification to work on these HITs.', action='store_true', default=False)

    mark_for_qualification_parser = subparsers.add_parser(
        'mark-pages-for-qualification',
        description='Mark a list of pages as qualification pages by their IDs',
        argument_default=argparse.SUPPRESS
    )
    mark_for_qualification_parser.add_argument('ids', metavar='IDs', nargs='+', help='Space-separated list of Page IDs to mark as qualification pages')

    publish_qualification_pages_parser = subparsers.add_parser(
        'publish-qualification-pages',
        description='Publish qualification pages to allow turkers to qualify for other tasks',
        argument_default=argparse.SUPPRESS
    )
    publish_qualification_pages_parser.add_argument('--max-assignments', '-a', help='Max. number of turkers that can do one HIT (default is 10)', type=int, default=10)

    create_qual_types_parser = subparsers.add_parser(
        'create-qual-types',
        description='Create qualification types in the current environment'
    )

    start_server_parser = subparsers.add_parser(
        'start-server',
        description='Start annotation inspection webserver'
    )

    fetch_results_parser = subparsers.add_parser(
        'fetch-results',
        description='Fetch results of published HITs'
    )

    eval_retrieved_parser = subparsers.add_parser(
        'eval-retrieved',
        description='Check inter-annotator agreement of retrieved annotations'
    )

    ingest_parser = subparsers.add_parser(
        'ingest',
        description='Ingest information regarding rasterized pages into the database',
        argument_default=argparse.SUPPRESS
    )
    ingest_parser.add_argument('parquet_file', metavar='PATH', help='Parquet file with pdf info')

    create_hit_type_parser = subparsers.add_parser(
        'create-hit-type',
        description='Create new HIT type from data in the .env file for the current environment.'
    )
    create_hit_type_parser.add_argument('--active', '-a', help='Make this HIT type the active one', action='store_true')

    export_answers_parser = subparsers.add_parser(
        'export-answers',
        description='Save answers to individual json files and additionally save a summary.',
        argument_default=argparse.SUPPRESS
    )
    export_answers_parser.add_argument('output_dir', metavar='PATH', help='Output directory')
    export_answers_parser.add_argument('--crop-whitespace', '-c', action='store_true', help='Crop whitespace around bounding boxes')

    compare_assignments_parser = subparsers.add_parser(
        'compare-assignments',
        description='Use the validation package to compare if two assignments match',
        argument_default=argparse.SUPPRESS
    )
    compare_assignments_parser.add_argument('page_id', metavar='PAGE_ID', help='Page ID')
    compare_assignments_parser.add_argument('assignment_1_id', metavar='ASSIG1_ID', help='Assignment 1 ID')
    compare_assignments_parser.add_argument('assignment_2_id', metavar='ASSIG2_ID', help='Assignment 2 ID')

    notify_specific_workers_parser = subparsers.add_parser(
        'notify-specific-workers',
        description='Send an email to a list of worker IDs',
        argument_default=argparse.SUPPRESS
    )
    notify_specific_workers_parser.add_argument('worker_ids', metavar='IDs', nargs='+', help='Space-separated list of worker IDs to notify')
    notify_specific_workers_parser.add_argument('subject', metavar='SUBJECT', help='Subject of the email')
    notify_specific_workers_parser.add_argument('message_text', metavar='TEXT', help='Body of the email')

    notify_workers_in_range_parser = subparsers.add_parser(
        'notify-workers-in-range',
        description='Send an email to workers whose verification points lie in the given range',
        argument_default=argparse.SUPPRESS
    )
    notify_workers_in_range_parser.add_argument('subject', metavar='SUBJECT', help='Subject of the email')
    notify_workers_in_range_parser.add_argument('message_text', metavar='TEXT', help='Body of the email')
    notify_workers_in_range_parser.add_argument('--minimum-qual-points', '-m', help='The minimum number of qual. points that a turker needs in order to work on these HITs.', type=int)
    notify_workers_in_range_parser.add_argument('--maximum-qual-points', help='The maximum number of qual. points that a turker needs in order to work on these HITs.', type=int)

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

    # Check for unsafe mode
    if(args.accept_prompts):
        logging.warning('Accepting all prompts! This can cause unwanted money loss!')
        Config.set('accept_prompts', True)

from sci_annot_eval.common.bounding_box import RelativeBoundingBox
import pandas as pd
from enums.page_status import PageStatus
from enums.qualification_types import QualificationType
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

    repository.ingest_pdfs(data)
    
    logging.info(f'Finished ingesting {data.shape[0]} rows')
    
def create_hit_type(active: bool = False):
    params = {
        'title': Config.get('HIT_type_title'),
        'keywords': Config.get('HIT_type_keywords'),
        'description': Config.get('HIT_type_description'),
        'reward': Config.get('HIT_type_reward'),
        'duration_sec': int(Config.get('HIT_type_duration_sec')),
        'auto_approval_delay_sec': int(Config.get('HIT_type_auto_approval_delay_sec')),
        'environment': Config.get('env_name')
    }

    id = mturk_client.create_hit_type(**params)
    params['_id'] = id
    params['active'] = active

    logging.debug(f'Saving HIT type with params: {params}')
    repository.save_hit_type(params)
    logging.info(f'Created HIT type with params: {params}')

def create_postqual_requirements(
    minimum_qual_points: int= 0,
    did_qual_tasks_required: bool= False,
    maximum_qual_points: int = 0
):
    repository.assert_qual_types_exist()
    qual_requirements = []

    if minimum_qual_points > 0:
        qual_points_id = repository.get_qual_type_id(QualificationType.QUAL_POINTS)
        qual_requirements.append({
            'QualificationTypeId': qual_points_id,
            'Comparator': 'GreaterThanOrEqualTo',
            'RequiredToPreview': True,
            'ActionsGuarded': 'DiscoverPreviewAndAccept',
            'IntegerValues': [
                minimum_qual_points,
            ]
        })
    
    if maximum_qual_points > 0:
        qual_points_id = repository.get_qual_type_id(QualificationType.QUAL_POINTS)
        qual_requirements.append({
            'QualificationTypeId': qual_points_id,
            'Comparator': 'LessThanOrEqualTo',
            'RequiredToPreview': True,
            'ActionsGuarded': 'DiscoverPreviewAndAccept',
            'IntegerValues': [
                maximum_qual_points,
            ]
        })

    if did_qual_tasks_required:
        did_qual_tasks_id = repository.get_qual_type_id(QualificationType.DID_QUAL_TASKS)
        qual_requirements = [{
            'QualificationTypeId': did_qual_tasks_id,
            'Comparator': 'Exists',
            'RequiredToPreview': True,
            'ActionsGuarded': 'DiscoverPreviewAndAccept'
        }]

    return qual_requirements

def publish_random(
    count: int,
    comment: Optional[str] = None,
    minimum_qual_points: int= 0,
    did_qual_tasks_required: bool= False,
    maximum_qual_points: int= 0
):
    unpublished = repository.get_random_pages_by_status([PageStatus.NOT_ANNOTATED], count, True)
    qual_requirements = create_postqual_requirements(minimum_qual_points, did_qual_tasks_required, maximum_qual_points)
    publish([page['_id'] for page in unpublished], comment, qual_requirements= qual_requirements)

def publish(
    ids: list[str],
    comment: Optional[str] = None,
    max_assignments: int = int(Config.get('max_assignments')),
    qual_requirements: list = []
):
    active_hit_type = repository.get_active_hit_type_or_by_id()
    
    logging.info(f'Active hit type: {active_hit_type}')
    if str(Config.get('accept_prompts')) != 'True':
        # Amazon MTurk takes a 20% cut per reward, but $0.01 is the minimum
        price = (float(active_hit_type['reward']) + max(0.01, float(active_hit_type['reward'])*0.2)) * len(ids) * max_assignments
        answer = input(f'This action will cost you {price}$. Are you sure you want to proceed? (N/y)? ')
        if(answer != 'y'):
            print('Cancelling action...')
            return

    page_id_HIT_response_map = {}
    for page in ids:
        img_url = Config.get('image_url_base') + page + Config.get('image_extension')
        try:
            if len(qual_requirements):
                response = mturk_client.create_hit(
                    active_hit_type,
                    img_url,
                    comment,
                    max_assignments,
                    qual_requirements
                )
            else:
                response = mturk_client.create_hit_with_hit_type(
                    active_hit_type['_id'],
                    img_url,
                    comment,
                    max_assignments,
                )
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
    submitted_pages = repository.get_random_pages_by_status([PageStatus.SUBMITTED])
    nr_found_pages = len(submitted_pages)
    logging.info(f'Found {nr_found_pages} submitted pages.')

    operation_dict = {}
    nr_feedback_messages = 0
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
                    parsed_answer = xml_to_dict(assignment['Answer'], sci_annot_parsers_dict)
                    if 'feedback' in parsed_answer:
                        nr_feedback_messages += 1
                    parsed_assignments.append({
                        'assignment_id': assignment['AssignmentId'],
                        'worker_id': assignment['WorkerId'],
                        'HIT_id': assignment['HITId'],
                        'auto_approval_time': assignment['AutoApprovalTime'],
                        'submit_time': assignment['SubmitTime'],
                        'reviewed': False,
                        'environment': Config.get('env_name'),
                        'answer': parsed_answer,
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

    status_counter = Counter(op['$set']['status'] for op in operation_dict.values())
    status_counter[PageStatus.SUBMITTED.value] = nr_found_pages - len(operation_dict)
    logging.info(f'Summary of submitted page statuses: {status_counter}')
    if nr_feedback_messages:
        logging.warning(f'{nr_feedback_messages} feedback message(s) received!')
    repository.update_pages_from_dict(operation_dict)

def crop_compare_answers(answer_1_raw, answer_2_raw, page_id, iou_threshold=0.95):
    img_bytes = repository.get_image_as_bytes(page_id)
    answer_1_parsed = answer_parser.parse_dict_absolute(answer_1_raw)
    # TODO: Clean this up
    answer_1_parsed = helpers.make_relative(helpers.crop_all_to_content(img_bytes, answer_1_parsed), answer_1_raw['canvasWidth'], answer_1_raw['canvasHeight'])
    answer_2_parsed = answer_parser.parse_dict_absolute(answer_2_raw)
    answer_2_parsed = helpers.make_relative(helpers.crop_all_to_content(img_bytes, answer_2_parsed), answer_2_raw['canvasWidth'], answer_2_raw['canvasHeight'])
    return evaluation.check_no_disagreements(answer_1_parsed, answer_2_parsed, iou_threshold)

def compare_assignments(page_id, assignment_1_id, assignment_2_id):
    assignment_1 = repository.get_assignment(page_id, assignment_1_id)
    assignment_2 = repository.get_assignment(page_id, assignment_2_id)
    result = crop_compare_answers(assignment_1['answer'], assignment_2['answer'], page_id, 0.95)

    print('Assignments match') if result else print('Assignments don\'t match')

def eval_retrieved():
    repository.assert_qual_types_exist()
    retrieved = repository.get_random_pages_by_status([PageStatus.RETRIEVED])
    passed = []
    deferred = []
    rejected = []
    worker_id_action_dict = {}
    assignment_action_list = []
    for page in retrieved:
        assignments = page['assignments']
        nr_assignments = len(assignments)
        if (nr_assignments == 0):
            rejected.append(page['_id'])
            logging.warning(f'page {page["_id"]} has no assignments!')
        elif (nr_assignments == 1):
            deferred.append(page['_id'])
        else:
            if 'qualification_page' in page.keys() and page['qualification_page']:
                # This is a qualification page
                ground_truth_raw = page['assignments'][0]['answer']
                for assignment in page['assignments']:
                    if 'reviewed' not in assignment.keys() or not assignment['reviewed']:
                        worker_id = assignment['worker_id']
                        # TODO: Handle ADMIN!
                        if worker_id not in worker_id_action_dict.keys():
                            worker_id_action_dict[worker_id] = {'$set': {'did_qualification_tasks': True}}
                        curr_answer_raw = assignment['answer']
                        match = crop_compare_answers(ground_truth_raw, curr_answer_raw, page['_id'])
                        if match:
                            worker_id_action_dict[worker_id]['$addToSet'] = {
                                'qual_pages_completed': page['_id']
                            }
                        assignment_action_list.append((
                            {'_id': page['_id'], 'assignments.assignment_id': assignment['assignment_id']},
                            {'$set': {'assignments.$.reviewed': True}}
                        ))    
            else:
                # This is a regular page
                if (len(assignments) > 2):
                    logging.warning(f'page {page["_id"]} has {len(assignments)} assignments! Only the last two will be evaluated')
                answer_1_raw = page['assignments'][-2]['answer']
                answer_2_raw = page['assignments'][-1]['answer']
                match = crop_compare_answers(answer_1_raw, answer_2_raw, page['_id'])

                if match:
                    logging.debug(f'page {page["_id"]} has matching annotations')
                    passed.append(page['_id'])
                else:
                    logging.debug(f'page {page["_id"]} doesn\'t have matching annotations')
                    deferred.append(page['_id'])

    logging.info(f'Validation results: {len(passed)} - good, {len(deferred)} - deferred, {len(rejected)} - rejected.')

    # TODO: Consolidate
    repository.update_pages_from_dict({id:{'$set': {'status': PageStatus.REVIEWED.value}} for id in passed})
    repository.update_pages_from_dict({id:{'$set': {'status': PageStatus.DEFERRED.value}} for id in deferred})
    repository.update_pages_from_dict({id:{'$set': {'status': PageStatus.REJECTED.value}} for id in rejected})
    repository.update_pages_from_tuples(assignment_action_list)
    if worker_id_action_dict:
        repository.update_workers_from_dict(worker_id_action_dict)
        updated_workers_curr_state = repository.get_workers_in_id_list(list(worker_id_action_dict.keys()))
        did_qual_tasks_id = repository.get_qual_type_id(QualificationType.DID_QUAL_TASKS)
        qual_points_id = repository.get_qual_type_id(QualificationType.QUAL_POINTS)
        # This is just to satisfy the type system.
        # The assertion that these are not None is done at the beginning of the func.
        if did_qual_tasks_id is not None and qual_points_id is not None:
            for worker in updated_workers_curr_state:
                mturk_client.assign_qualification_to_worker(did_qual_tasks_id, worker['_id'])
                total_qual_points = len(worker['qual_pages_completed']) + worker['verification_points'] if 'verification_points' in worker.keys() else 0
                mturk_client.assign_qualification_to_worker(qual_points_id, worker['_id'], total_qual_points)

def start_server():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web.settings')
    application = get_wsgi_application()
    management.call_command('runserver')

def export_answers(output_dir: str, crop_whitespace: bool):
    logging.debug('Cropping whitespace in output...')
    existing_ids = [file.split('.')[0] for file in os.listdir(output_dir)]
    accepted_page_assignments = repository.get_accepted_assignments(existing_ids)
    summary_dict = {}

    nr_files = 0
    for page_assig in accepted_page_assignments:
        logging.debug(f'Exporting page {page_assig["_id"]}')
        if crop_whitespace:
            assignment = page_assig['assignment']
            orig_answer = assignment['answer']
            orig_bboxes = answer_parser.parse_dict_absolute(orig_answer)
            img_bytes = repository.get_image_as_bytes(page_assig['_id'])
            cropped_bboxes = helpers.crop_all_to_content(img_bytes, orig_bboxes)
            relative_cropped_bboxes = helpers.make_relative(
                cropped_bboxes,
                int(orig_answer['canvasWidth']),
                int(orig_answer['canvasHeight'])
            )
            exported_annots = answer_exporter.export_to_dict(
                relative_cropped_bboxes,
                int(orig_answer['canvasWidth']),
                int(orig_answer['canvasHeight'])
            )
            orig_answer['annotations'] = exported_annots['annotations']
        file_path = os.path.join(output_dir, page_assig['_id']+'.json')
        summary_dict[page_assig['_id']] = [
            page_assig['status'],
            page_assig['assignment']['worker_id']
        ]
        with open(file_path, 'w+') as of:
            json.dump(page_assig['assignment']['answer'], of, indent=4)
        nr_files+= 1
        logging.debug(f'{nr_files}/??: page {page_assig["_id"]} saved')
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

def create_qual_types():
    created_nr = 0
    for qual in QualificationType:
        logging.debug(f'Creating qual: {qual}')
        if not repository.get_qual_type_id(qual):
            keys = mturk_client.create_qual_type(qual)
            repository.save_qual_requirement(keys)
            created_nr += 1
    
    if created_nr:
        logging.info(f'Created {created_nr} qualification requirement(s)')
    else:
        logging.warning(f'All qualification requirements already exist!')

def mark_pages_for_qual(ids: list[str]):
    """
        Marks pages with given IDs as qualification pages by adding a qualification_page=True property to them.
    """
    result = repository.get_pages_in_id_list(ids)
    if len(result) != len(ids):
        logging.fatal(f'Out of {len(ids)} provided IDs, only {len(result)} were found!')
        return
    action_dict = {}
    for page in result:
        if len(page['assignments']) < 1:
            raise Exception(f'Page with id {page["_id"]} has no assignments!')
        else:
            action_dict[page['_id']] = {'$set': {'qualification_page': True}}
    repository.update_pages_from_dict(action_dict)

def pub_qual_pages(max_assignments: int = 10):
    qual_pages = repository.get_qualification_pages()
    id_list = []
    for page in qual_pages:
        # Page has no pending assignments and is ready to be published
        if page['status'] in [PageStatus.NOT_ANNOTATED.value, PageStatus.REVIEWED.value, PageStatus.VERIFIED.value]:
            id_list.append(page['_id'])
        else:
            raise Exception(f'Cannot publish qualification pages because page {page["_id"]} is in the {page["status"]} status')

    did_qual_tasks_id = repository.get_qual_type_id(QualificationType.DID_QUAL_TASKS)
    qual_requirements = [{
        'QualificationTypeId': did_qual_tasks_id,
        'Comparator': 'DoesNotExist',
        'RequiredToPreview': True,
        'ActionsGuarded': 'DiscoverPreviewAndAccept'
    }]

    publish(id_list, max_assignments= max_assignments, qual_requirements= qual_requirements)
    
def notify_workers_in_range(subject: str, message_text: str, **kwargs):
    """_summary_

    Args:
        subject (str): _description_
        text (str): _description_
        minimum_qual_points (int): Minimum qualification points of workers to notify
        maximum_qual_points (int): Maximum qualification points of workers to notify
    """

    workers_in_range = repository.get_workers_in_verification_point_range(
        kwargs.get('minimum_qual_points', None),
        kwargs.get('maximum_qual_points', None)
    )
    
    if len(workers_in_range) > 100:
        # API limit
        logging.error(f'Found {len(workers_in_range)} workers to notify, but the MTurk api only supports up to 100')
        return

    worker_ids = [worker['_id'] for worker in workers_in_range]
    mturk_client.notify_workers(subject, message_text, worker_ids)

if __name__ == '__main__':
    # Handle arguments            
    if args.command == 'mark-pages-for-qualification':
        mark_pages_for_qual(args.ids)
    elif args.command == 'publish-qualification-pages':
        pub_qual_pages(args.max_assignments)
    elif args.command == 'publish-random':
        print(f'Args: {args}')
        comment = None
        if('comment' in args):
            comment = args.comment
        publish_random(
            args.count,
            comment,
            args.minimum_qual_points,
            bool(args.require_qualification_done),
            args.maximum_qual_points
        )
    elif args.command == 'publish-specific':
        comment = None
        if('comment' in args):
            comment = args.comment
        qual_reqs = create_postqual_requirements(
            args.minimum_qual_points,
            args.require_qualification_done,
            args.maximum_qual_points
        )
        publish(args.publish_specific, comment, qual_requirements=qual_reqs)
    elif args.command == 'create-qual-types':
        create_qual_types()
    elif args.command == 'start-server':
        start_server()
    elif args.command == 'fetch-results':
        fetch_hit_results()
    elif args.command == 'eval-retrieved':
        eval_retrieved()
    elif args.command == 'ingest':
        ingest(args.parquet_file)
    elif args.command == 'create-hit-type':
        create_hit_type(args.active)
    elif args.command == 'export-answers':
        export_answers(args.output_dir, args.crop_whitespace)
    elif args.command == 'compare-assignments':
        compare_assignments(args.page_id, args.assignment_1_id, args.assignment_2_id)
    elif args.command == 'notify-specific-workers':
        mturk_client.notify_workers(args.subject, args.message_text, args.worker_ids)
    elif args.command == 'notify-workers-in-range':
        print(args)
        range_args = {}
        if 'minimum_qual_points' in args.__dict__.keys():
            range_args['minimum_qual_points'] = args.minimum_qual_points
        if 'maximum_qual_points' in args.__dict__.keys():
            range_args['maximum_qual_points'] = args.maximum_qual_points
        notify_workers_in_range(args.subject, args.message_text, **range_args)