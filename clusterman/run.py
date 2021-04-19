# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys

from clusterman.args import parse_args
from clusterman.config import setup_config
from clusterman.util import setup_logging


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv, "Cluster scaling and management for Mesos and Kubernetes")

    setup_logging(args.log_level)
    setup_config(args)

    try:
        args.entrypoint(args)
    except Exception as e:
        print(f"Exception of type {e.__class__.__name__} occured")

        if e.args:
            for arg in e.args:
                print(arg)

        exit(1)


if __name__ == "__main__":
    main()
