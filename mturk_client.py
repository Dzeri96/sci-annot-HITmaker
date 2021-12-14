import boto3
from config import Config
import logging
import json
from urllib import parse

from enums.qualification_requirements import QualificationRequirement

class Client:
    __instance = None

    @staticmethod
    def get():
        logging.debug(f'Creating a new BOTO3 MTurk client')

        if Client.__instance != None:
            return Client.__instance
        else:
            Client.__instance = boto3.client(
                'mturk',
                endpoint_url=Config.get('endpoint_url'),
                region_name=Config.get('region_name'),
                aws_access_key_id=Config.get('aws_access_key_id'),
                aws_secret_access_key=Config.get('aws_secret_access_key'),
            )
            return Client.__instance

def create_hit_type (
    title: str,
    keywords: str,
    description: str,
    reward: str,
    duration_sec: int,
    auto_approval_delay_sec: int
) -> str:

    response = Client.get().create_hit_type(
        AutoApprovalDelayInSeconds=auto_approval_delay_sec,
        AssignmentDurationInSeconds=duration_sec,
        Reward=reward,
        Title=title,
        Keywords=keywords,
        Description=description
    )

    logging.debug(f'mturk create_hit_type response: {response}')

    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return response['HITTypeId']
    else:
        raise Exception('Could not create HIT type: ' + str(response))

def create_hit(type_id: str, image_url: str, comment: str = None):
    external_url = Config.get('external_url')
    fullUrl = f'{external_url}?image={image_url}'
    if comment:
        fullUrl += f'&amp;comment={parse.quote_plus(comment)}'
    question_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <ExternalQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd">
        <ExternalURL>{fullUrl}</ExternalURL>
        <FrameHeight>0</FrameHeight>
    </ExternalQuestion>
    '''

    response = Client.get().create_hit_with_hit_type(
        HITTypeId=type_id,
        MaxAssignments=int(Config.get('max_assignments')),
        LifetimeInSeconds=int(Config.get('lifetime_sec')),
        Question=question_xml
    )

    return response

def get_HIT_status(id: str):
    response = Client.get().get_hit(
        HITId=id
    )
    logging.debug(f'HIT status: {response}')
    return response

def get_HIT_results(hit_id: str)-> dict:
    response = Client.get().list_assignments_for_hit(HITId=hit_id)
    return response

def list_hits():
    response = Client.get().list_hits()
    logging.debug(f'List hits response: {json.dumps(response)}')

    return response

def create_qual_type(qual: QualificationRequirement) -> dict:
    """
        Creates a new qualification type and returns the contents of the QualificationType response key.
    """
    response = Client.get().create_qualification_type(**qual.value)
    logging.debug(f'Created Qualification Type with name {qual.value["Name"]}')
    return response['QualificationType']