from clusterman.args import parse_args


def main():
    args = parse_args('Mesos cluster scaling and management')
    args.entrypoint(args)


if __name__ == '__main__':
    main()
