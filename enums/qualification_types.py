from enum import Enum
import logging
from typing import Any
from config import Config

# I know this is an abomination, but I have no more time left
# The different placeholders are there so that python doesn't merge the keys
class QualificationType(Enum):
    DID_QUAL_TASKS = {
        'Name': Config.get('qualification_did_qual_tasks_name'),
        'Description': Config.get('qualification_did_qual_tasks_description'),
        'QualificationTypeStatus': 'Active',
        'AutoGranted': False
    }
    QUAL_POINTS = {
        'Name': Config.get('qualification_qual_points_name'),
        'Description': Config.get('qualification_qual_points_description'),
        'QualificationTypeStatus': 'Active',
        'AutoGranted': False
    }