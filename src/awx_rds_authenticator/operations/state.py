import json


def store_resource_state(
    ssm_client,
    username: str,
    state: dict,
) -> None:
    ssm_client.put_parameter(
        Name=f"/awx/rds/authenticator/{username}",
        Value=json.dumps(state),
        Type="String",
        Overwrite=True,
    )


def load_resource_state(ssm_client, username: str) -> dict:
    response = ssm_client.get_parameter(
        Name=f"/awx/rds/authenticator/{username}",
    )
    return json.loads(response["Parameter"]["Value"])


def delete_resource_state(ssm_client, username: str) -> None:
    ssm_client.delete_parameter(Name=f"/awx/rds/authenticator/{username}")
