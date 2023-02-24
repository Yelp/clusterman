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
import argparse
import socket
from functools import partial

import colorlog

from clusterman.util import limit_function_runtime


logger = colorlog.getLogger(__name__)
TIMEOUT_TIME_SECONDS = 15


def timeout_wrapper(main):
    def wrapper(args: argparse.Namespace):
        # After 10s, prints a warning message if the command is running from the wrong place
        def timeout_handler():
            warning_string = "This command is taking a long time to run; are you running from the right account?"
            if "yelpcorp" in socket.getfqdn():
                warning_string += "\nHINT: try ssh'ing to adhoc-prod or another box in the right account."
            logger.warning(warning_string)

        limit_function_runtime(partial(main, args), TIMEOUT_TIME_SECONDS, timeout_handler)

    return wrapper
