import logging
import socket

import clog.handlers
import staticconf


namespace = 'clog'
clog_namespace = staticconf.NamespaceGetters(namespace)  # type: ignore
DETAILED_FORMAT = '\t'.join(
    [
        '%(asctime)s',
        socket.gethostname(),
        '%(process)s',
        '%(name)s',
        '%(levelname)s',
        '%(message)s'
    ]
)


log_stream_name = clog_namespace.get_string('log_stream_name')
log_stream_format = clog_namespace.get_string(
    'log_stream_format', default=DETAILED_FORMAT
)
log_stream_level = clog_namespace.get_string(
    'log_stream_level', default='INFO'
)
enable_uwsgi_mule_offload = clog_namespace.get_bool(
    'enable_uwsgi_mule_offload', default=False
)


def initialize():
    """Initialize clog from staticconf config."""
    if enable_uwsgi_mule_offload and clog.uwsgi_plugin_enabled:
        clog.uwsgi_patch_global_state()

    add_clog_handler(
        name=log_stream_name.value,
        level=getattr(logging, log_stream_level.value),
        log_format=log_stream_format.value)


def add_clog_handler(name, level=logging.INFO, log_format=DETAILED_FORMAT):
    """Add a CLog logging handler for the stream 'name'.

    :param name: the name of the log
    :type name: string
    :param level: the logging level of the handler
    :type level: int
    """
    clog_handler = clog.handlers.CLogHandler(name)
    clog_handler.setLevel(level)
    formatter = logging.Formatter(log_format)
    clog_handler.setFormatter(formatter)
    logging.root.addHandler(clog_handler)
