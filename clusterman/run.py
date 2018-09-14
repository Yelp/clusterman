from clusterman.args import parse_args
from clusterman.config import setup_config
from clusterman.util import setup_logging


def main():
    args = parse_args('Mesos cluster scaling and management')
    setup_logging()
    setup_config(args)
    args.entrypoint(args)


if __name__ == '__main__':
    main()
