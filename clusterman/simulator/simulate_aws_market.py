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
from typing import List
from typing import Optional

from clusterman.aws.markets import _InstanceMarket
from clusterman.aws.markets import InstanceResources

EC2_AZS: List[Optional[str]] = [
    None,
    "us-east-1a",
    "us-east-1b",
    "us-east-1c",
    "us-west-1a",
    "us-west-1b",
    "us-west-1c",
    "us-west-2a",
    "us-west-2b",
    "us-west-2c",
]


class simulate_InstanceMarket(_InstanceMarket):
    __slots__ = ()

    def __new__(cls, instance: str, az: Optional[str]):
        if mock_get_instance_type(instance) is not None and az in EC2_AZS:
            return super().__new__(cls, instance, az)
        else:
            raise ValueError(f"Invalid AWS market specified: <{instance}, {az}> (choices from {EC2_AZS})")


def mock_get_instance_type(instance_type):
    instance_types = {
        "t2.2xlarge": InstanceResources(8.0, 32.0, None, 0),
        "m6a.4xlarge": InstanceResources(16.0, 64.0, None, 0),
        "m5.large": InstanceResources(2.0, 8.0, None, 0),
        "m5.4xlarge": InstanceResources(16.0, 64.0, None, 0),
        "m4.4xlarge": InstanceResources(16.0, 64.0, None, 0),
        "m3.xlarge": InstanceResources(4.0, 15.0, 80.0, 0),
        "c3.xlarge": InstanceResources(4.0, 7.5, 80.0, 0),
        "c3.4xlarge": InstanceResources(16.0, 30.0, 320.0, 0),
        "c3.8xlarge": InstanceResources(32.0, 60.0, 640.0, 0),
        "i2.4xlarge": InstanceResources(16.0, 122.0, 3200.0, 0),
        "i2.8xlarge": InstanceResources(32.0, 244.0, 6400.0, 0),
    }
    return instance_types[instance_type]


def mock_get_market_resources(simulate_InstanceMarket):
    return mock_get_instance_type(simulate_InstanceMarket.instance)
