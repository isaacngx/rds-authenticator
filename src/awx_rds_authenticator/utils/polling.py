"""Generic polling utilities for async operations."""
import logging
import time
from enum import Enum

LOG = logging.getLogger(__name__)


class OperationType(Enum):
    """Types of operations that can be polled."""
    CREATE = "creation"
    DELETE = "deletion"


def poll_assignment_status(
    sso_client,
    instance_arn: str,
    account_assignments: list[dict],
    operation_type: OperationType,
) -> tuple[str, list[dict]]:
    time.sleep(2)

    describe_fn = {
        OperationType.CREATE: sso_client.describe_account_assignment_creation_status,
        OperationType.DELETE: sso_client.describe_account_assignment_deletion_status,
    }[operation_type]
    
    config = {
        OperationType.CREATE: {
            "status_key": "AccountAssignmentCreationStatus",
            "request_param": "AccountAssignmentCreationRequestId",
        },
        OperationType.DELETE: {
            "status_key": "AccountAssignmentDeletionStatus",
            "request_param": "AccountAssignmentDeletionRequestId",
        },
    }[operation_type]
    
    overall_status = "SUCCEEDED"
    current_assignments = []
    
    for assignment in account_assignments:
        current_assignment = describe_fn(
            InstanceArn=instance_arn,
            **{config["request_param"]: assignment["RequestId"]},
        )[config["status_key"]]

        current_assignments.append(current_assignment)

    for current_assignment in current_assignments:
        
        if current_assignment["Status"] == "FAILED":
            LOG.error(f"Account assignment {operation_type.value} failed for RequestId: {current_assignment['RequestId']}")
            overall_status = "FAILED"
            break
        
        if current_assignment["Status"] != "SUCCEEDED":
            overall_status = "IN_PROGRESS"
            break

    return overall_status, current_assignments
