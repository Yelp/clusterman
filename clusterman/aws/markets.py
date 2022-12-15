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
from functools import lru_cache
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
import colorlog

from mypy_extensions import TypedDict

from clusterman.aws.client import ec2


logger = colorlog.getLogger(__name__)

class InstanceResources(NamedTuple):
    cpus: float
    mem: float
    disk: Optional[float]
    gpus: int


class _InstanceMarket(NamedTuple):
    instance: str
    az: Optional[str]


class MarketDict(TypedDict):
    InstanceType: str
    SubnetId: str
    Placement: Mapping


class InstanceMarket(_InstanceMarket):
    __slots__ = ()

    def __new__(cls, instance: str, az: Optional[str]):
        if get_instance_type(instance) is not None and az in EC2_AZS:
            return super().__new__(cls, instance, az)
        else:
            raise ValueError(f"Invalid AWS market specified: <{instance}, {az}> (choices from {EC2_AZS})")

    def __repr__(self) -> str:
        return f"<{self.instance}, {self.az}>"

    @classmethod
    def parse(cls, string: str):
        sans_brackets = string[1:-1]
        return cls(*sans_brackets.split(", "))


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

@lru_cache(maxsize=64)
def get_instance_type(instance_type: str) -> Optional[InstanceResources]:
    res = {}
    try:
        res = ec2.describe_instance_types(InstanceTypes=[instance_type]).get('InstanceTypes')[0]
    except Exception as e:
        logger.warning(f"Error occoured while describing instance type {instance_type} : {e}")
        return None

    vcpu_count = res.get('VCpuInfo',{}).get('DefaultVCpus',0.0) +0.0
    mem_size = res.get('MemoryInfo',{}).get('SizeInMiB',0.0)/1024.0
    disk_size = res.get('InstanceStorageInfo',{}).get('TotalSizeInGB',None)
    gpu_size = res.get('GpuInfo',{}).get('Gpus',[{}])[0].get('Count',0)

    return InstanceResources(vcpu_count,mem_size,
                              disk_size,gpu_size)


def get_market_resources(market: InstanceMarket) -> Optional[InstanceResources]:
    return get_instance_type(market.instance)


def get_market(instance_type: str, subnet_id: Optional[str]) -> InstanceMarket:
    az: Optional[str]
    if subnet_id is not None:
        az = subnet_to_az(subnet_id)
    else:
        az = None
    return InstanceMarket(instance_type, az)


def get_instance_market(aws_instance_object: MarketDict) -> InstanceMarket:
    instance_type = aws_instance_object["InstanceType"]
    subnet_id = aws_instance_object.get("SubnetId")
    if subnet_id:
        return get_market(instance_type, subnet_id)
    else:
        az = aws_instance_object.get("Placement", {}).get("AvailabilityZone")
        return InstanceMarket(instance_type, az)


@lru_cache(maxsize=32)
def subnet_to_az(subnet_id: str) -> str:
    return ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]["AvailabilityZone"]