from enum import Enum
import logging
from typing import Any
from config import Config

# I know this is an abomination, but I have no more time left
# The different placeholders are there so that python doesn't merge the keys
class QualificationRequirement(Enum):
    DID_QUAL_TASKS = {'_placeholder': '0'}
    QUAL_POINTS = {'_placeholder': '1'}

def init_qual_enum_values():
    """
        Needs to be called before using the QualificationRequirement Enum to initialize it with values from Config
    """
    logging.debug('Intializing QualificationRequirement values...')

    QualificationRequirement.DID_QUAL_TASKS.value.update({
        'Name': Config.get('qualification_did_qual_tasks_name'),
        'Description': Config.get('qualification_did_qual_tasks_description'),
        'QualificationTypeStatus': 'Active',
        'AutoGranted': False
    })
    QualificationRequirement.DID_QUAL_TASKS.value.pop('_placeholder')

    QualificationRequirement.QUAL_POINTS.value.update({
        'Name': Config.get('qualification_qual_points_name'),
        'Description': Config.get('qualification_qual_points_description'),
        'QualificationTypeStatus': 'Active',
        'AutoGranted': False
    })
    QualificationRequirement.QUAL_POINTS.value.pop('_placeholder')