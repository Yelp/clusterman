import os

import pyramid_swagger
import pyramid_uwsgi_metrics
import uwsgi_metrics
import yelp_pyramid
from pyramid.config import Configurator
from yelp_servlib import config_util
from yelp_servlib import logging_util

import clusterman.config

SERVICE_CONFIG_PATH = os.environ.get('SERVICE_CONFIG_PATH')
SERVICE_ENV_CONFIG_PATH = os.environ.get('SERVICE_ENV_CONFIG_PATH')

uwsgi_metrics.initialize()


def _create_application(service_config_path, service_env_config_path):
    """Create the WSGI application, post-fork."""

    # Create a basic pyramid Configurator.
    config = Configurator(settings={
        'service_name': 'clusterman',
        'pyramid_swagger.skip_validation': [
            r'^/static/?',
            r'^/api-docs/?',
            r'^/swagger.json',
            r'^/status$',
            r'^/status/metrics$',
        ],
    })

    config_util.load_default_config(
        service_config_path,
        service_env_config_path,
    )

    # Add the service's custom configuration, routes, etc.
    config.include(clusterman.config.routes)

    config.include('pyramid_distributed_context')

    # Include the yelp_pyramid library default configuration after our
    # configuration so that the yelp_pyramid configuration can base decisions
    # on the service's configuration.
    config.include(yelp_pyramid)

    config.include(pyramid_swagger)

    # Display metrics on the '/status/metrics' endpoint
    config.include(pyramid_uwsgi_metrics)

    # Scan the service package to attach any decorated views.
    config.scan('clusterman')

    return config.make_wsgi_app()


def create_application(
    service_config_path=SERVICE_CONFIG_PATH,
    service_env_config_path=SERVICE_ENV_CONFIG_PATH,
):
    with logging_util.log_create_application('clusterman_uwsgi'):
        return _create_application(
            service_config_path,
            service_env_config_path,
        )
