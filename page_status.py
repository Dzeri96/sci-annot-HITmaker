from enum import Enum

class PageStatus(Enum):
    NOT_ANNOTATED = 'NOT_ANNOTATED'
    SUBMITTED = 'SUBMITTED'
    EXPIRED  ='EXPIRED'
    RETRIEVED = 'RETRIEVED'
    # Passed review
    REVIEWED  = 'REVIEWED'
    # Deferred for manual review
    DEFERRED = 'DEFERRED'
    REJECTED = 'REJECTED'