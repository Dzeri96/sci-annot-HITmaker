import boto3
from config import Config
import logging
import json
from urllib import parse
from botocore.config import Config as BotoConfig
from typing import Optional

from enums.qualification_types import QualificationType

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
                config=BotoConfig(
                    retries = {
                        'max_attempts': 15,
                        'mode': 'adaptive'
                    }
                )
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

def create_hit(
    hit_type: dict,
    image_url: str,
    comment: Optional[str] = None,
    max_assignments: int = int(Config.get('max_assignments')),
    qualification_requirements: list = []
):
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

    arguments = {
        'Title': hit_type['title'],
        'Keywords': hit_type['keywords'],
        'Description': hit_type['description'],
        'Reward': hit_type['reward'],
        'AssignmentDurationInSeconds': hit_type['duration_sec'],
        'AutoApprovalDelayInSeconds': hit_type['auto_approval_delay_sec'],
        'MaxAssignments': max_assignments,
        'LifetimeInSeconds': int(Config.get('lifetime_sec')),
        'Question': question_xml
    }

    if qualification_requirements:
        arguments['QualificationRequirements'] = qualification_requirements

    response = Client.get().create_hit(
        **arguments
    )

    return response

def create_hit_with_hit_type(
    type_id: str,
    image_url: str,
    comment: Optional[str] = None,
    max_assignments: int = int(Config.get('max_assignments')),
):
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

    arguments = {
        'HITTypeId': type_id,
        'MaxAssignments': max_assignments,
        'LifetimeInSeconds': int(Config.get('lifetime_sec')),
        'Question': question_xml
    }

    response = Client.get().create_hit_with_hit_type(
        **arguments
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

def create_qual_type(qual: QualificationType) -> dict:
    """
        Creates a new qualification type and returns the contents of the QualificationType response key.
    """
    response = Client.get().create_qualification_type(**qual.value)
    logging.debug(f'Created Qualification Type with name {qual.value["Name"]}')
    return response['QualificationType']

def assign_qualification_to_worker(
    qual_id: str,
    worker_id: str,
    integer_value: Optional[int] = None,
    send_notification: bool = False
):
    args = {
        'QualificationTypeId': qual_id,
        'WorkerId':worker_id,
        'SendNotification': send_notification
    }

    if integer_value is not None:
        args['IntegerValue'] = integer_value

    logging.debug(f'Calliing associate_qualification with following args: {args}')    
    response = Client.get().associate_qualification_with_worker(**args)
    return response

def approve_assignment(
    assignment_id: str,
    requester_feedback: str= Config.get('approve_assignment_feedback'),
):
    logging.debug(f'Approving assignment {assignment_id} with feedback: "{requester_feedback}"')
    Client.get().approve_assignment(
        AssignmentId=assignment_id,
        RequesterFeedback=requester_feedback,
        OverrideRejection=True
    )

def reject_assignment(
    assignment_id: str,
    requester_feedback: str= Config.get('reject_assignment_feedback'),
):
    logging.debug(f'Rejecting assignment {assignment_id} with feedback: "{requester_feedback}"')
    Client.get().reject_assignment(
        AssignmentId=assignment_id,
        RequesterFeedback=requester_feedback
    )

def notify_workers(
    subject: str,
    message_text: str,
    worker_ids: list[str]
):
    if(len(worker_ids)) and subject and message_text:
        logging.info(f'Notifying {len(worker_ids)} worker(s) with subject "{subject}"')
        response = Client.get().notify_workers(
            Subject=subject,
            MessageText=message_text,
            WorkerIds=worker_ids
        )
        logging.debug(f'notify_workers response: {response}')