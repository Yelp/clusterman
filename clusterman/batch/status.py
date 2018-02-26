import sys


def check_status(batch_name):
    # status written by BatchRunningSentinelMixin
    status_file = f'/tmp/{batch_name}.running'
    # pid written by yelp_batch
    pid_file = f'/nail/run/{batch_name}.pid'

    try:
        with open(status_file) as f:
            status_pid = f.read()
        with open(pid_file) as f:
            batch_pid = f.read()
    except FileNotFoundError:
        print(f'{batch_name} has not finished initialization')
        sys.exit(1)

    assert status_pid == batch_pid
    print(f'{batch_name} completed initialization and is running at PID {status_pid}')


if __name__ == '__main__':
    check_status(sys.argv[1])
