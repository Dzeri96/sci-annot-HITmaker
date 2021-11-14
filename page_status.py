from enum import Enum

class PageStatus(Enum):
    NOT_ANNOTATED = 'NOT_ANNOTATED'
    SUBMITTED = 'SUBMITTED'
    EXPIRED  ='EXPIRED'
    RETRIEVED = 'RETRIEVED'
    REVIEWED  = 'REVIEWED' 