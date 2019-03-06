import logging
import os

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.aiohttp import AioHttpIntegration


logging.basicConfig(level=logging.DEBUG)


sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    environment=os.environ['SENTRY_ENVIRONMENT'],
    integrations=[
        LoggingIntegration(
            level=logging.DEBUG,
            event_level=logging.WARNING
        ),
        CeleryIntegration(),
        AioHttpIntegration(),
    ],
)
