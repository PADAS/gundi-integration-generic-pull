import datetime
import httpx
import json
import logging
import stamina
import pyjq
from app.actions.configurations import AuthenticateConfig, GenericPullConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import find_config_for_action

logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


async def get_auth_token(integration, auth_config):
    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.post(
            auth_config.auth_url,
            json={
                "username": auth_config.username,
                "password": auth_config.password
            }
        )
        response.raise_for_status()
        json_response = response.json()
        token_field = auth_config.token_field
        return json_response[token_field]


@stamina.retry(on=httpx.HTTPError, wait_initial=1.0, wait_jitter=5.0, wait_max=32.0)
async def pull_data(integration, action_config: GenericPullConfig):
    url = action_config.url
    auth_config = find_config_for_action(integration.configurations, "auth")
    if auth_config.auth_type == "username_password":
        token = await get_auth_token(integration, integration.auth_config)
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={
                    "Authorization": f"Bearer {token}"
                }
            )
            response.raise_for_status()
            return response.json()
    else:
        raise ValueError(f"Invalid auth type: {auth_config.auth_type}. Please review the configuration.")


async def filter_and_transform(data: list, jq_filter: str, output_type: str):
    filter_expression = jq_filter.replace("\n", "").replace(" ", "")
    transformed_data = pyjq.all(filter_expression, data)
    return transformed_data


#@activity_logger()
async def action_pull_data(integration, action_config: GenericPullConfig):
    logger.info(f"Executing action_generic_pull action for integration {integration} with action_config {action_config}...")

    data_points = await pull_data(integration, action_config)
    input_data = [json.loads(i.json()) for i in data_points]
    transformed_data = await filter_and_transform(
        data=input_data,
        jq_filter=action_config.jq_filter,
        output_type=action_config.output_type
    )
    logger.info(f"Transformed Data: {transformed_data}")
    if action_config.output_type == "obv":  # ToDo: Use an enum?
        response = await send_observations_to_gundi(
            observations=transformed_data,
            integration_id=integration.id
        )
    elif action_config.output_type == "ev":
        response = await send_events_to_gundi(
            events=transformed_data,
            integration_id=integration.id
        )
    else:
        raise ValueError(f"Invalid output type: {action_config.output_type}. Please review the configuration.")
    data_points_qty = len(response)
    logger.info(f"'{data_points_qty}' data point(s) sent to Gundi.")
    return {"data_points_qty": data_points_qty}

