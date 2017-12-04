import logging

from clusterman.args import parse_args
from clusterman.config import setup_config


def setup_logging():
    EVENT_LOG_LEVEL = 25
    logging.addLevelName(EVENT_LOG_LEVEL, 'EVENT')

    def event(self, message, *args, **kwargs):
        if self.isEnabledFor(EVENT_LOG_LEVEL):
            self._log(EVENT_LOG_LEVEL, message, args, **kwargs)
    logging.Logger.event = event


def main():
    args = parse_args('Mesos cluster scaling and management')
    setup_logging()
    setup_config(args)
    args.entrypoint(args)


if __name__ == '__main__':
    main()
