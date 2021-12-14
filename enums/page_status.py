from enum import Enum

class PageStatus(Enum):
    NOT_ANNOTATED = 'NOT_ANNOTATED'
    SUBMITTED = 'SUBMITTED'
    EXPIRED  ='EXPIRED'
    RETRIEVED = 'RETRIEVED'
    # Passed automatic review
    REVIEWED  = 'REVIEWED'
    # Passed manual review
    VERIFIED = 'VERIFIED'
    # Deferred for manual review
    DEFERRED = 'DEFERRED'
    REJECTED = 'REJECTED'