import logging
import coloredlogs
import argparse
from config import Config
import pandas as pd
from page_status import PageStatus
import repository
import mturk_client
from question_form_answers_parser import xml_to_dict

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

def publish(count: int):
    unpublished = repository.get_random_not_annotated(count)
    active_hit_type = repository.get_active_hit_type_or_by_id()
    
    logging.info(f'Active hit type: {active_hit_type}')
    if str(Config.get('accept_prompts')) != 'True':
        price = float(active_hit_type['reward']) * count
        answer = input(f'This action will cost you {price}$. Are you sure you want to proceed? (N/y)? ')
        if(answer != 'y'):
            print('Cancelling action...')
            return

    page_id_HIT_response_map = {}
    for page in unpublished:
        img_url = Config.get('image_url_base') + page + Config.get('image_extension')
        try:
            response = mturk_client.create_hit(active_hit_type['_id'], img_url)
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
    #mturk_client.fetch_hit_results(['3BVS8WK9Q05JN0TH1A19AAQ6K15IBG'])
    #mturk_client.get_HIT_status('3BVS8WK9Q05JN0TH1A19AAQ6K15IBG')

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
                        'answer': xml_to_dict(assignment['Answer'], ['annotations'])
                    })

            operation_dict[page['_id']] = {
                '$set': {
                    'status': status
                }
            }
            if(len(parsed_assignments) != 0):
                operation_dict[page['_id']]['$push'] = {
                    'assignments': parsed_assignments
                }
    
    repository.update_pages_from_dict(operation_dict)
            



if __name__ == '__main__':
    parser = argparse.ArgumentParser('Amazon MTurk HIT client')
    parser.add_argument('--env', '-e', nargs=1, default='.env', help='Environment file', metavar='file')
    parser.add_argument('--create-hit-type', '-t', type=bool, nargs=1, default=False, help='Create new HIT type and optionally set it as the active one', metavar='active')
    parser.add_argument('--ingest', '-i', nargs=1, help='Parquet file with pdf info', metavar='file')
    parser.add_argument('--publish-random', '-p', nargs=1, help='Publish a certain number of HITs from the pool of unpublished pages', metavar='count', type=int)
    parser.add_argument('--verbose', '-v', help='Enable verbose logging (info, debug)', action='count')
    parser.add_argument('--accept-prompts', '-y', help='Say yes to any prompts (UNSAFE)', action='store_true')
    parser.add_argument('--fetch-results', '-f', help='Fetch results of published HITs', action='store_true')


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

    # Handle arguments
    if(args.ingest):
        ingest(args.ingest[0])
    elif(args.publish_random):
        publish(args.publish_random[0])
    elif(args.create_hit_type):
        create_hit_type(args.create_hit_type[0])
    elif(args.fetch_results):
        fetch_hit_results()

