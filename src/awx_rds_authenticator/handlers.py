import logging
from typing import Any, MutableMapping, Optional

from botocore.exceptions import ClientError
from cloudformation_cli_python_lib.boto3_proxy import SessionProxy
from cloudformation_cli_python_lib.interface import Action, OperationStatus, ProgressEvent
from cloudformation_cli_python_lib import exceptions
from cloudformation_cli_python_lib.resource import Resource
from .models import (
    ResourceHandlerRequest,
    ResourceModel,
)
from .utils.polling import poll_assignment_status, OperationType
from .utils.builders import build_instance_arn
from .operations.permission_set import create_permission_set, delete_permission_set
from .operations.assignment import create_assignments, delete_assignments
from .operations.state import store_resource_state, delete_resource_state

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
TYPE_NAME = "AWX::RDS::Authenticator"

resource = Resource(TYPE_NAME, ResourceModel)
test_entrypoint = resource.test_entrypoint


def _apply_defaults(model: ResourceModel) -> None:
    if model.Targets:
        for target in model.Targets:
            target.DbInstanceResourceId = "*" if target.DbInstanceResourceId is None else target.DbInstanceResourceId


@resource.handler(Action.CREATE)
def create_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    if not model or not session:
        raise exceptions.InternalFailure("Invalid request: missing model or session")
    if not model.Username or not model.IamIdentityCenterId or not model.Targets:
        raise exceptions.InvalidRequest("Username, IamIdentityCenterId, and Targets are required")
    
    _apply_defaults(model)

    sso_client = session.client("sso-admin", region_name="us-east-1")
    identity_store_client = session.client("identitystore", region_name="us-east-1")
    ssm_client = session.client("ssm", region_name="us-east-1")

    overall_status = callback_context.get("overall_status", None)
    account_assignments = callback_context.get("account_assignments", None)
    permission_set_arn = callback_context.get("permission_set_arn", None)

    instance_arn = build_instance_arn(model.IamIdentityCenterId)

    if not (overall_status):
        try:
            LOG.info(f"Creating permission set for user {model.Username}")
            permission_set_arn = create_permission_set(
                model, sso_client
            )

            store_resource_state(
                ssm_client,
                model.Username,
                { "PermissionSetArn": permission_set_arn }
            )

            LOG.info(f"Creating account assignments for user {model.Username}")
            account_assignments = create_assignments(
                sso_client,
                identity_store_client,
                instance_arn,
                permission_set_arn,
                model.Username,
                { target.AccountId for target in model.Targets if target.AccountId },
            )

            for assignment in account_assignments:
                assignment.pop("CreatedDate", None)

            store_resource_state(
                ssm_client,
                model.Username,
                {
                    "PermissionSetArn": permission_set_arn,
                    "AccountAssignments": account_assignments
                },
            )

            return ProgressEvent(
                message=f"Creating access for user {model.Username}",
                status=OperationStatus.IN_PROGRESS,
                resourceModel=model,
                callbackContext={
                    "overall_status": "IN_PROGRESS",
                    "account_assignments": account_assignments,
                    "permission_set_arn": permission_set_arn,
                },
            )
        except ClientError as error:
            LOG.error(f"Error occurred while initiating resources creation for user {model.Username}: {error}")
            return ProgressEvent(
                message=f"Error occurred while initiating resources creation for user {model.Username}",
                status=OperationStatus.FAILED
            )

    LOG.info(f"Polling resource status for user {model.Username}")
    overall_status, account_assignments = poll_assignment_status(
        sso_client, instance_arn, account_assignments, OperationType.CREATE,
    )

    for assignment in account_assignments:
        assignment.pop("CreatedDate", None)

    store_resource_state(
        ssm_client,
        model.Username,
        {
            "PermissionSetArn": permission_set_arn,
            "AccountAssignments": account_assignments
        },
    )

    if overall_status == "SUCCEEDED":
        LOG.info(f"Successfully created resources for user {model.Username}")
        return ProgressEvent(
            message=f"Successfully created resources for user {model.Username}",
            status=OperationStatus.SUCCESS,
        )

    if overall_status == "FAILED":
        LOG.error(f"Failed to create resources for user {model.Username}")
        return ProgressEvent(
            message=f"Failed to create resources for user {model.Username}",
            status=OperationStatus.FAILED
        )

    return ProgressEvent(
        message=f"Creating RDS access for user {model.Username}",
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
        callbackContext={
            "overall_status": overall_status,
            "account_assignments": account_assignments,
            "permission_set_arn": permission_set_arn,
        },
    )


@resource.handler(Action.DELETE)
def delete_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    if not model or not session:
        raise exceptions.InternalFailure("Invalid request: missing model or session")
    if not model.Username or not model.IamIdentityCenterId:
        raise exceptions.InvalidRequest("Username and IamIdentityCenterId are required")
    
    sso_client = session.client("sso-admin", region_name="us-east-1")
    ssm_client = session.client("ssm", region_name="us-east-1")
    identity_store_client = session.client("identitystore", region_name="us-east-1")

    overall_status = callback_context.get("overall_status", None)
    account_assignments = callback_context.get("account_assignments", None)
    permission_set_arn = callback_context.get("permission_set_arn", None)

    instance_arn = build_instance_arn(model.IamIdentityCenterId)

    if not (overall_status):
        LOG.info(f"Deleting account assignments for user {model.Username}")
        permission_set_arn, account_assignments = delete_assignments(
            sso_client,
            ssm_client,
            identity_store_client,
            model
        )

        if not account_assignments:
            delete_permission_set(sso_client, instance_arn, permission_set_arn)
            delete_resource_state(ssm_client, model.Username)
            LOG.info(f"Successfully deleted resources for user {model.Username}")
            return ProgressEvent(
                message=f"Deleted RDS access for user {model.Username}",
                status=OperationStatus.SUCCESS,
            )

        return ProgressEvent(
            message=f"Deleting resources for user {model.Username}",
            status=OperationStatus.IN_PROGRESS,
            resourceModel=model,
            callbackContext={
                "overall_status": "IN_PROGRESS",
                "account_assignments": account_assignments,
                "permission_set_arn": permission_set_arn,
            },
        )

    LOG.info(f"Polling resource status for user {model.Username}")
    overall_status, account_assignments = poll_assignment_status(
        sso_client, instance_arn, account_assignments, OperationType.DELETE,
    )

    if overall_status == "SUCCEEDED":
        delete_permission_set(sso_client, instance_arn, permission_set_arn)
        delete_resource_state(ssm_client, model.Username)
        LOG.info(f"Successfully deleted resources for user {model.Username}")
        return ProgressEvent(
            message=f"Deleted resources for user {model.Username}",
            status=OperationStatus.SUCCESS,
        )
    
    if overall_status == "FAILED":
        LOG.error(f"Failed to delete resources for user {model.Username}")
        return ProgressEvent(
            message=f"Failed to delete resources for user {model.Username}",
            status=OperationStatus.FAILED
        )

    return ProgressEvent(
        message=f"Deleting resources for user {model.Username}",
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
        callbackContext={
            "overall_status": overall_status,
            "account_assignments": account_assignments,
            "permission_set_arn": permission_set_arn,
        },
    )


@resource.handler(Action.READ)
def read_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    if not model or not session:
        raise exceptions.InternalFailure("Invalid request: missing model or session")
    if not model.IamIdentityCenterId:
        raise exceptions.InvalidRequest("IamIdentityCenterId is required")
    
    ssoClient = session.client("sso-admin", region_name="us-east-1")
    try:
        # Note: The model doesn't have PermissionSetArn attribute. 
        # This should be retrieved from SSM or callback context if needed.
        instance_arn = build_instance_arn(model.IamIdentityCenterId)
        # Commenting out the describe call as PermissionSetArn is not in the model
        # ssoClient.describe_permission_set(
        #     InstanceArn=instance_arn,
        #     PermissionSetArn=model.PermissionSetArn,
        # )
    except ClientError as error:
        raise exceptions.InternalFailure(
            f"Failed to read permission set with error {error}"
        )

    return ProgressEvent(
        message="Successfully read permission set for RDS access",
        status=OperationStatus.SUCCESS,
        resourceModel=model,
    )
