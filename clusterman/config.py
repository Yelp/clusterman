import logging


log = logging.getLogger('clusterman.config')


def routes(config):
    """Add routes to the configuration."""
    config.add_route('api.hello', '/hello/{name}')
