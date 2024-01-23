import asyncio
import json
import logging

import aiohttp
import stamina
from functools import wraps
from gcloud.aio import pubsub
from gundi_core.events import (
    SystemEventBaseModel,
    IntegrationActionCustomLog,
    CustomActivityLog,
    IntegrationActionStarted,
    ActionExecutionStarted,
    IntegrationActionFailed,
    ActionExecutionFailed,
    IntegrationActionComplete,
    ActionExecutionComplete,
)
from app import settings


logger = logging.getLogger(__name__)


# Publish events for other services or system components
@stamina.retry(
    on=(aiohttp.ClientError, asyncio.TimeoutError), attempts=5
)
async def publish_event(event: SystemEventBaseModel, topic_name: str):
    timeout_settings = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(
        raise_for_status=True, timeout=timeout_settings
    ) as session:
        client = pubsub.PublisherClient(session=session)
        # Get the topic
        topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
        # Prepare the payload
        binary_payload = json.dumps(event.dict(), default=str).encode("utf-8")
        messages = [pubsub.PubsubMessage(binary_payload)]
        logger.debug(f"Sending event {event} to PubSub topic {topic_name}..")
        try:  # Send to pubsub
            response = await client.publish(topic, messages)
        except Exception as e:
            logger.exception(
                f"Error publishing system event topic {topic_name}: {e}. This will be retried."
            )
            raise e
        else:
            logger.debug(f"System event {event} published successfully.")
            logger.debug(f"GCP PubSub response: {response}")


async def log_activity(integration_id: str, action_id: str, title: str, level="INFO", config_data: dict = None, data: dict = None):
    """
    This is a helper method to send custom activity logs to the portal.
    :param integration_id:
    :param action_id:
    :param title: A human-readable string that will appear in the activity log
    :param level: The level of the log, e.g. DEBUG, INFO, WARNING, ERROR
    :param data: Any extra data to be logged as a dict
    :return: None
    """
    print(f"{level}: {title} - Data: {data}")
    await publish_event(
        event=IntegrationActionCustomLog(
            payload=CustomActivityLog(
                integration_id=integration_id,
                action_id=action_id,
                config_data=config_data,
                title=title,
                level=level,
                data=data
            )
        ),
        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
    )


def activity_logger(on_start=True, on_completion=True, on_error=True):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            integration = kwargs.get("integration")
            integration_id = str(integration.id) if integration else None
            action_id = func.__name__.replace("action_", "")
            action_config = kwargs.get("action_config")
            config_data = action_config.data if action_config else {}
            if on_start:
                await publish_event(
                    event=IntegrationActionStarted(
                        payload=ActionExecutionStarted(
                            integration_id=integration_id,
                            action_id=action_id,
                            config_data=config_data,
                        )
                    ),
                    topic_name=settings.INTEGRATION_EVENTS_TOPIC,
                )
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                if on_error:
                    await publish_event(
                        event=IntegrationActionFailed(
                            payload=ActionExecutionFailed(
                                integration_id=integration_id,
                                action_id=action_id,
                                config_data=config_data,
                                error=str(e)
                            )
                        ),
                        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
                    )
                raise e
            else:
                if on_completion:
                    await publish_event(
                        event=IntegrationActionComplete(
                            payload=ActionExecutionComplete(
                                integration_id=integration_id,
                                action_id=action_id,
                                config_data=config_data,
                                result=result
                            )
                        ),
                        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
                    )
                return result
        return wrapper
    return decorator


