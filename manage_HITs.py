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
    print(f'unpub: {unpublished}')
    
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


    # Handle arguments
    if(args.ingest):
        ingest(args.ingest[0])
    elif(args.publish_random):
        publish(args.publish_random[0])
    elif(args.create_hit_type):
        create_hit_type(args.create_hit_type[0])
    
