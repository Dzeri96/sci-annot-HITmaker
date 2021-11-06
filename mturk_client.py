import boto3
from config import Config
import logging

class Client:
    __instance: boto3.Session.client = None

    @staticmethod
    def get():
        logging.info(f'Creating a new BOTO3 MTurk client')

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

def create_hit_type():

    response = Client.get().create_hit_type(
        AutoApprovalDelayInSeconds=216000,
        AssignmentDurationInSeconds=300,
        Reward='0.02',
        Title='Draw bounding boxes around elements in scientific publications',
        Keywords='image,bounding,box,figures,tables,captions,science,publications',
        Description='In this task you are asked to find any figures or tables on a page and draw a bounding box around them (if there are any). Aditionally, you should draw bounding boxes around captions that explain those figures or tables.'
    )

    return response

def create_hit(type: str):
    question_xml = '''<?xml version="1.0" encoding="UTF-8"?>
    <ExternalQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd">
        <ExternalURL>https://tunnel.dzeri.me</ExternalURL>
        <FrameHeight>0</FrameHeight>
    </ExternalQuestion>
    '''

    response = Client.get().create_hit_with_hit_type(
        HITTypeId=type,
        MaxAssignments=1,
        LifetimeInSeconds=600,
        Question=question_xml
    )

    return response

def get_hit(id: str):
    response = Client.get().get_hit(
        HITId=id
    )

    return response

#print(create_hit_type(client))
#print(create_hit(client, '3K17V65Z3L957DTKD6L8VG0D6AHM89'))
#print(get_hit(client, '308KJXFUJRG4D440P80HT689DSETAB'))
#client.delete_hit(HITId='33TGB4G0LPRG9HCT0ISVDEJ7EDMTXE')
#print(client.list_assignments_for_hit(HITId='308KJXFUJRG4D440P80HT689DSETAB')) 