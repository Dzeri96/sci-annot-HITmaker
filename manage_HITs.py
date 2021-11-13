import logging
import coloredlogs
import argparse
from config import Config
import pandas as pd
import repository
import mturk_client

def ingest(path):
    data = pd.read_parquet(path)
    logging.info(f'Ingesting {path} into database...')

    for index, row in data.iterrows():
        repository.ingest_pdf(row)
    
    logging.info(f'Finished ingesting {data.shape[0]} rows')

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

    page_HIT_id_map = {}
    for page in unpublished:
        img_url = Config.get('image_url_base') + page
        try:
            response = mturk_client.create_hit(active_hit_type['_id'], img_url)
            if(response['ResponseMetadata']['HTTPStatusCode'] == 200):
                logging.debug(f'Created hit: {response}')
                page_HIT_id_map[page] = response['HIT']['HITId']
            else:
                logging.error(f'Could not create HIT. Response was: {response}')
                break
        except Exception as e:
            logging.error(f'Could not create HIT. Exception was: "{e}"')
            break
    
    repository.update_pages_to_submitted(page_HIT_id_map)
    
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Amazon MTurk HIT client')
    parser.add_argument('--env', '-e', nargs=1, default='.env', help='Environment file', metavar='file')
    parser.add_argument('--create-hit-type', '-t', type=bool, nargs=1, default=False, help='Create new HIT type and optionally set it as the active one', metavar='active')
    parser.add_argument('--ingest', '-i', nargs=1, help='Parquet file with pdf info', metavar='file')
    parser.add_argument('--publish-random', '-p', nargs=1, help='Publish a certain number of HITs from the pool of unpublished pages', metavar='count', type=int)
    parser.add_argument('--verbose', '-v', help='Enable verbose logging (info, debug)', action='count')
    parser.add_argument('--accept-prompts', '-y', help='Say yes to any prompts (UNSAFE)', action='store_true')

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
    
