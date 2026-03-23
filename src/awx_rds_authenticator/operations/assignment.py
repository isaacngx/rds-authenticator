import logging

from awx_rds_authenticator.operations.state import load_resource_state
from awx_rds_authenticator.utils.builders import build_instance_arn

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def create_assignments(
    sso_client,
    identity_store_client,
    instance_arn: str,
    permission_set_arn: str,
    username: str,
    target_accounts: set[str],
) -> list[dict]:
    sso_instance = sso_client.describe_instance(InstanceArn=instance_arn)
    user_id = identity_store_client.get_user_id(
        IdentityStoreId=sso_instance["IdentityStoreId"],
        AlternateIdentifier={
            "UniqueAttribute": {
                "AttributePath": "Username",
                "AttributeValue": username,
            }
        },
    )["UserId"]
    
    account_assignments = [
        sso_client.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set_arn,
            PrincipalType="USER",
            PrincipalId=user_id,
            TargetType="AWS_ACCOUNT",
            TargetId=target_account,
        )["AccountAssignmentCreationStatus"]
        for target_account in target_accounts
    ]

    return account_assignments


def delete_assignments(
    sso_client,
    ssm_client,
    identity_store_client,
    model,
) -> tuple[str, list[dict]]:
    resource_state = load_resource_state(ssm_client, model.Username)
    instance_arn = build_instance_arn(model.IamIdentityCenterId)
    permission_set_arn = resource_state["PermissionSetArn"]
    account_assignments = (
        resource_state["AccountAssignments"] if "AccountAssignments" in resource_state
        else []
    )

    if not account_assignments:
        LOG.info(f"No account assignments found in resource state for user {model.Username}")
        return permission_set_arn, []

    account_ids = {
        account_assignment["TargetId"] for account_assignment in account_assignments 
        if account_assignment["Status"] == "SUCCEEDED"
    }

    sso_instance = sso_client.describe_instance(InstanceArn=instance_arn)
    user_id = identity_store_client.get_user_id(
        IdentityStoreId=sso_instance["IdentityStoreId"],
        AlternateIdentifier={
            "UniqueAttribute": {
                "AttributePath": "Username",
                "AttributeValue": model.Username,
            }
        },
    )["UserId"]

    return (
        permission_set_arn,
        [
            sso_client.delete_account_assignment(
                InstanceArn=instance_arn,
                PermissionSetArn=permission_set_arn,
                PrincipalType="USER",
                PrincipalId=user_id,
                TargetType="AWS_ACCOUNT",
                TargetId=account_id,
            )["AccountAssignmentDeletionStatus"]
            for account_id in account_ids
        ]
    )
