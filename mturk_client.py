import boto3
from config import Config
import logging
import json

class Client:
    __instance: boto3.Session.client = None

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

def create_hit(type_id: str, image_url: str):
    external_url = Config.get('external_url')
    question_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <ExternalQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd">
        <ExternalURL>{external_url}?image={image_url}</ExternalURL>
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
#print(create_hit_type(client))
#print(create_hit(client, '3K17V65Z3L957DTKD6L8VG0D6AHM89'))
#print(get_hit(client, '308KJXFUJRG4D440P80HT689DSETAB'))
#client.delete_hit(HITId='33TGB4G0LPRG9HCT0ISVDEJ7EDMTXE')
#print(client.list_assignments_for_hit(HITId='308KJXFUJRG4D440P80HT689DSETAB')) 