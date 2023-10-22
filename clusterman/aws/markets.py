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


EC2_INSTANCE_TYPES: Mapping[str, InstanceResources] = {
    "t2.nano": InstanceResources(cpus=1.0, mem=512.0, disk=None, gpus=0),
    "t2.micro": InstanceResources(cpus=1.0, mem=1024.0, disk=None, gpus=0),
    "t2.small": InstanceResources(cpus=1.0, mem=2048.0, disk=None, gpus=0),
    "t2.medium": InstanceResources(cpus=2.0, mem=4096.0, disk=None, gpus=0),
    "t2.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "t2.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "t2.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m6a.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m6a.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m6a.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m6a.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=None, gpus=0),
    "m6a.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=None, gpus=0),
    "m6a.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=None, gpus=0),
    "m6a.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=None, gpus=0),
    "m6a.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=None, gpus=0),
    "m6a.32xlarge": InstanceResources(cpus=128.0, mem=524288.0, disk=None, gpus=0),
    "m6a.48xlarge": InstanceResources(cpus=192.0, mem=786432.0, disk=None, gpus=0),
    "m6i.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m6i.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m6i.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m6i.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=None, gpus=0),
    "m6i.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=None, gpus=0),
    "m6i.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=None, gpus=0),
    "m6i.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=None, gpus=0),
    "m6i.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=None, gpus=0),
    "m6i.32xlarge": InstanceResources(cpus=128.0, mem=524288.0, disk=None, gpus=0),
    "m6id.large": InstanceResources(cpus=2.0, mem=8192.0, disk=118.0, gpus=0),
    "m6id.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=237.0, gpus=0),
    "m6id.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=474.0, gpus=0),
    "m6id.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=950.0, gpus=0),
    "m6id.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=1900.0, gpus=0),
    "m6id.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=2850.0, gpus=0),
    "m6id.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=3800.0, gpus=0),
    "m6id.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=5700.0, gpus=0),
    "m6id.32xlarge": InstanceResources(cpus=128.0, mem=524288.0, disk=7600.0, gpus=0),
    "m5.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m5.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m5.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m5.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=None, gpus=0),
    "m5.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=None, gpus=0),
    "m5.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=None, gpus=0),
    "m5.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=None, gpus=0),
    "m5.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=None, gpus=0),
    "m5a.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m5a.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m5a.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m5a.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=None, gpus=0),
    "m5a.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=None, gpus=0),
    "m5a.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=None, gpus=0),
    "m5a.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=None, gpus=0),
    "m5a.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=None, gpus=0),
    "m5ad.large": InstanceResources(cpus=2.0, mem=8192.0, disk=75.0, gpus=0),
    "m5ad.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=150.0, gpus=0),
    "m5ad.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=300.0, gpus=0),
    "m5ad.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=600.0, gpus=0),
    "m5ad.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=1200.0, gpus=0),
    "m5ad.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=1800.0, gpus=0),
    "m5ad.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=2400.0, gpus=0),
    "m5ad.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=3600.0, gpus=0),
    "m5d.large": InstanceResources(cpus=2.0, mem=8192.0, disk=75.0, gpus=0),
    "m5d.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=150.0, gpus=0),
    "m5d.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=300.0, gpus=0),
    "m5d.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=600.0, gpus=0),
    "m5d.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=1200.0, gpus=0),
    "m5d.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=1800.0, gpus=0),
    "m5d.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=2400.0, gpus=0),
    "m5d.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=3600.0, gpus=0),
    "m5dn.large": InstanceResources(cpus=2.0, mem=8192.0, disk=75.0, gpus=0),
    "m5dn.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=150.0, gpus=0),
    "m5dn.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=300.0, gpus=0),
    "m5dn.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=600.0, gpus=0),
    "m5dn.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=1200.0, gpus=0),
    "m5dn.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=1800.0, gpus=0),
    "m5dn.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=2400.0, gpus=0),
    "m5dn.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=3600.0, gpus=0),
    "m5n.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m5n.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m5n.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m5n.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=None, gpus=0),
    "m5n.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=None, gpus=0),
    "m5n.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=None, gpus=0),
    "m5n.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=None, gpus=0),
    "m5n.24xlarge": InstanceResources(cpus=96.0, mem=393216.0, disk=None, gpus=0),
    "m5zn.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m5zn.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m5zn.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m5zn.3xlarge": InstanceResources(cpus=12.0, mem=49152.0, disk=None, gpus=0),
    "m5zn.6xlarge": InstanceResources(cpus=24.0, mem=98304.0, disk=None, gpus=0),
    "m5zn.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=None, gpus=0),
    "m4.large": InstanceResources(cpus=2.0, mem=8192.0, disk=None, gpus=0),
    "m4.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=None, gpus=0),
    "m4.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=None, gpus=0),
    "m4.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=None, gpus=0),
    "m4.10xlarge": InstanceResources(cpus=40.0, mem=163840.0, disk=None, gpus=0),
    "m4.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=None, gpus=0),
    "m3.medium": InstanceResources(cpus=1.0, mem=3840.0, disk=4.0, gpus=0),
    "m3.large": InstanceResources(cpus=2.0, mem=7680.0, disk=32.0, gpus=0),
    "m3.xlarge": InstanceResources(cpus=4.0, mem=15360.0, disk=80.0, gpus=0),
    "m3.2xlarge": InstanceResources(cpus=8.0, mem=30720.0, disk=160.0, gpus=0),
    "c6i.large": InstanceResources(cpus=2.0, mem=4096.0, disk=None, gpus=0),
    "c6i.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=None, gpus=0),
    "c6i.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=None, gpus=0),
    "c6i.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=None, gpus=0),
    "c6i.8xlarge": InstanceResources(cpus=32.0, mem=65536.0, disk=None, gpus=0),
    "c6i.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=None, gpus=0),
    "c6i.16xlarge": InstanceResources(cpus=64.0, mem=131072.0, disk=None, gpus=0),
    "c6i.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=None, gpus=0),
    "c6i.32xlarge": InstanceResources(cpus=128.0, mem=262144.0, disk=None, gpus=0),
    "c6id.large": InstanceResources(cpus=2.0, mem=4096.0, disk=118.0, gpus=0),
    "c6id.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=237.0, gpus=0),
    "c6id.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=474.0, gpus=0),
    "c6id.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=950.0, gpus=0),
    "c6id.8xlarge": InstanceResources(cpus=32.0, mem=65536.0, disk=1900.0, gpus=0),
    "c6id.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=2850.0, gpus=0),
    "c6id.16xlarge": InstanceResources(cpus=64.0, mem=131072.0, disk=3800.0, gpus=0),
    "c6id.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=5700.0, gpus=0),
    "c6id.32xlarge": InstanceResources(cpus=128.0, mem=262144.0, disk=7600.0, gpus=0),
    "c6a.large": InstanceResources(cpus=2.0, mem=4096.0, disk=None, gpus=0),
    "c6a.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=None, gpus=0),
    "c6a.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=None, gpus=0),
    "c6a.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=None, gpus=0),
    "c6a.8xlarge": InstanceResources(cpus=32.0, mem=65536.0, disk=None, gpus=0),
    "c6a.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=None, gpus=0),
    "c6a.16xlarge": InstanceResources(cpus=64.0, mem=131072.0, disk=None, gpus=0),
    "c6a.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=None, gpus=0),
    "c6a.32xlarge": InstanceResources(cpus=128.0, mem=262144.0, disk=None, gpus=0),
    "c6a.48xlarge": InstanceResources(cpus=192.0, mem=393216.0, disk=None, gpus=0),
    "c6a.metal": InstanceResources(cpus=192.0, mem=393216.0, disk=None, gpus=0),
    "c5.large": InstanceResources(cpus=2.0, mem=4096.0, disk=None, gpus=0),
    "c5.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=None, gpus=0),
    "c5.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=None, gpus=0),
    "c5.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=None, gpus=0),
    "c5.9xlarge": InstanceResources(cpus=36.0, mem=73728.0, disk=None, gpus=0),
    "c5.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=None, gpus=0),
    "c5.18xlarge": InstanceResources(cpus=72.0, mem=147456.0, disk=None, gpus=0),
    "c5.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=None, gpus=0),
    "c5a.large": InstanceResources(cpus=2.0, mem=4096.0, disk=None, gpus=0),
    "c5a.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=None, gpus=0),
    "c5a.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=None, gpus=0),
    "c5a.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=None, gpus=0),
    "c5a.8xlarge": InstanceResources(cpus=32.0, mem=65536.0, disk=None, gpus=0),
    "c5a.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=None, gpus=0),
    "c5a.16xlarge": InstanceResources(cpus=64.0, mem=131072.0, disk=None, gpus=0),
    "c5a.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=None, gpus=0),
    "c5ad.large": InstanceResources(cpus=2.0, mem=4096.0, disk=75.0, gpus=0),
    "c5ad.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=150.0, gpus=0),
    "c5ad.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=300.0, gpus=0),
    "c5ad.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=600.0, gpus=0),
    "c5ad.8xlarge": InstanceResources(cpus=32.0, mem=65536.0, disk=1200.0, gpus=0),
    "c5ad.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=1800.0, gpus=0),
    "c5ad.16xlarge": InstanceResources(cpus=64.0, mem=131072.0, disk=2400.0, gpus=0),
    "c5ad.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=3800.0, gpus=0),
    "c5d.large": InstanceResources(cpus=2.0, mem=4096.0, disk=50.0, gpus=0),
    "c5d.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=100.0, gpus=0),
    "c5d.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=200.0, gpus=0),
    "c5d.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=400.0, gpus=0),
    "c5d.9xlarge": InstanceResources(cpus=36.0, mem=73728.0, disk=900.0, gpus=0),
    "c5d.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=1800.0, gpus=0),
    "c5d.18xlarge": InstanceResources(cpus=72.0, mem=147456.0, disk=1800.0, gpus=0),
    "c5d.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=3600.0, gpus=0),
    "c5n.large": InstanceResources(cpus=2.0, mem=4096.0, disk=None, gpus=0),
    "c5n.xlarge": InstanceResources(cpus=4.0, mem=8192.0, disk=None, gpus=0),
    "c5n.2xlarge": InstanceResources(cpus=8.0, mem=16384.0, disk=None, gpus=0),
    "c5n.4xlarge": InstanceResources(cpus=16.0, mem=32768.0, disk=None, gpus=0),
    "c5n.9xlarge": InstanceResources(cpus=36.0, mem=73728.0, disk=None, gpus=0),
    "c5n.12xlarge": InstanceResources(cpus=48.0, mem=98304.0, disk=None, gpus=0),
    "c5n.18xlarge": InstanceResources(cpus=72.0, mem=147456.0, disk=None, gpus=0),
    "c5n.24xlarge": InstanceResources(cpus=96.0, mem=196608.0, disk=None, gpus=0),
    "c4.large": InstanceResources(cpus=2.0, mem=3840.0, disk=None, gpus=0),
    "c4.xlarge": InstanceResources(cpus=4.0, mem=7680.0, disk=None, gpus=0),
    "c4.2xlarge": InstanceResources(cpus=8.0, mem=15360.0, disk=None, gpus=0),
    "c4.4xlarge": InstanceResources(cpus=16.0, mem=30720.0, disk=None, gpus=0),
    "c4.8xlarge": InstanceResources(cpus=36.0, mem=61440.0, disk=None, gpus=0),
    "c3.large": InstanceResources(cpus=2.0, mem=3840.0, disk=32.0, gpus=0),
    "c3.xlarge": InstanceResources(cpus=4.0, mem=7680.0, disk=80.0, gpus=0),
    "c3.2xlarge": InstanceResources(cpus=8.0, mem=15360.0, disk=160.0, gpus=0),
    "c3.4xlarge": InstanceResources(cpus=16.0, mem=30720.0, disk=320.0, gpus=0),
    "c3.8xlarge": InstanceResources(cpus=32.0, mem=61440.0, disk=640.0, gpus=0),
    "x1.32xlarge": InstanceResources(cpus=128.0, mem=1998848.0, disk=3840.0, gpus=0),
    "x1.16xlarge": InstanceResources(cpus=64.0, mem=999424.0, disk=1920.0, gpus=0),
    "r6i.large": InstanceResources(cpus=2.0, mem=16384.0, disk=None, gpus=0),
    "r6i.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=None, gpus=0),
    "r6i.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=None, gpus=0),
    "r6i.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=None, gpus=0),
    "r6i.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=None, gpus=0),
    "r6i.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=None, gpus=0),
    "r6i.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=None, gpus=0),
    "r6i.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=None, gpus=0),
    "r6i.32xlarge": InstanceResources(cpus=128.0, mem=1048576.0, disk=None, gpus=0),
    "r6id.large": InstanceResources(cpus=2.0, mem=16384.0, disk=118.0, gpus=0),
    "r6id.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=237.0, gpus=0),
    "r6id.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=474.0, gpus=0),
    "r6id.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=950.0, gpus=0),
    "r6id.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=1900.0, gpus=0),
    "r6id.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=2850.0, gpus=0),
    "r6id.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=3800.0, gpus=0),
    "r6id.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=5700.0, gpus=0),
    "r6id.32xlarge": InstanceResources(cpus=128.0, mem=1048576.0, disk=7600.0, gpus=0),
    "r6idn.large": InstanceResources(cpus=2.0, mem=16384.0, disk=118.0, gpus=0),
    "r6idn.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=237.0, gpus=0),
    "r6idn.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=474.0, gpus=0),
    "r6idn.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=950.0, gpus=0),
    "r6idn.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=1900.0, gpus=0),
    "r6idn.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=2850.0, gpus=0),
    "r6idn.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=3800.0, gpus=0),
    "r6idn.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=5700.0, gpus=0),
    "r6idn.32xlarge": InstanceResources(cpus=128.0, mem=1048576.0, disk=7600.0, gpus=0),
    "r5.large": InstanceResources(cpus=2.0, mem=16384.0, disk=None, gpus=0),
    "r5.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=None, gpus=0),
    "r5.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=None, gpus=0),
    "r5.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=None, gpus=0),
    "r5.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=None, gpus=0),
    "r5.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=None, gpus=0),
    "r5.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=None, gpus=0),
    "r5.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=None, gpus=0),
    "r5a.large": InstanceResources(cpus=2.0, mem=16384.0, disk=None, gpus=0),
    "r5a.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=None, gpus=0),
    "r5a.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=None, gpus=0),
    "r5a.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=None, gpus=0),
    "r5a.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=None, gpus=0),
    "r5a.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=None, gpus=0),
    "r5a.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=None, gpus=0),
    "r5a.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=None, gpus=0),
    "r5b.large": InstanceResources(cpus=2.0, mem=16384.0, disk=None, gpus=0),
    "r5b.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=None, gpus=0),
    "r5b.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=None, gpus=0),
    "r5b.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=None, gpus=0),
    "r5b.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=None, gpus=0),
    "r5b.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=None, gpus=0),
    "r5b.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=None, gpus=0),
    "r5b.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=None, gpus=0),
    "r5ad.large": InstanceResources(cpus=2.0, mem=16384.0, disk=75.0, gpus=0),
    "r5ad.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=150.0, gpus=0),
    "r5ad.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=300.0, gpus=0),
    "r5ad.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=600.0, gpus=0),
    "r5ad.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=1200.0, gpus=0),
    "r5ad.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=1800.0, gpus=0),
    "r5ad.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=2400.0, gpus=0),
    "r5ad.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=3600.0, gpus=0),
    "r5d.large": InstanceResources(cpus=2.0, mem=16384.0, disk=75.0, gpus=0),
    "r5d.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=150.0, gpus=0),
    "r5d.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=300.0, gpus=0),
    "r5d.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=600.0, gpus=0),
    "r5d.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=1200.0, gpus=0),
    "r5d.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=1800.0, gpus=0),
    "r5d.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=2400.0, gpus=0),
    "r5d.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=3600.0, gpus=0),
    "r5dn.large": InstanceResources(cpus=2.0, mem=16384.0, disk=75.0, gpus=0),
    "r5dn.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=150.0, gpus=0),
    "r5dn.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=300.0, gpus=0),
    "r5dn.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=600.0, gpus=0),
    "r5dn.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=1200.0, gpus=0),
    "r5dn.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=1800.0, gpus=0),
    "r5dn.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=2400.0, gpus=0),
    "r5dn.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=3600.0, gpus=0),
    "r5n.large": InstanceResources(cpus=2.0, mem=16384.0, disk=None, gpus=0),
    "r5n.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=None, gpus=0),
    "r5n.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=None, gpus=0),
    "r5n.4xlarge": InstanceResources(cpus=16.0, mem=131072.0, disk=None, gpus=0),
    "r5n.8xlarge": InstanceResources(cpus=32.0, mem=262144.0, disk=None, gpus=0),
    "r5n.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=None, gpus=0),
    "r5n.16xlarge": InstanceResources(cpus=64.0, mem=524288.0, disk=None, gpus=0),
    "r5n.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=None, gpus=0),
    "r4.large": InstanceResources(cpus=2.0, mem=15616.0, disk=None, gpus=0),
    "r4.xlarge": InstanceResources(cpus=4.0, mem=31232.0, disk=None, gpus=0),
    "r4.2xlarge": InstanceResources(cpus=8.0, mem=62464.0, disk=None, gpus=0),
    "r4.4xlarge": InstanceResources(cpus=16.0, mem=124928.0, disk=None, gpus=0),
    "r4.8xlarge": InstanceResources(cpus=32.0, mem=249856.0, disk=None, gpus=0),
    "r4.16xlarge": InstanceResources(cpus=64.0, mem=499712.0, disk=None, gpus=0),
    "r3.large": InstanceResources(cpus=2.0, mem=15616.0, disk=32.0, gpus=0),
    "r3.xlarge": InstanceResources(cpus=4.0, mem=31232.0, disk=80.0, gpus=0),
    "r3.2xlarge": InstanceResources(cpus=8.0, mem=62464.0, disk=160.0, gpus=0),
    "r3.4xlarge": InstanceResources(cpus=16.0, mem=124928.0, disk=320.0, gpus=0),
    "r3.8xlarge": InstanceResources(cpus=32.0, mem=249856.0, disk=320.0, gpus=0),
    "i2.xlarge": InstanceResources(cpus=4.0, mem=31232.0, disk=800.0, gpus=0),
    "i2.2xlarge": InstanceResources(cpus=8.0, mem=62464.0, disk=1600.0, gpus=0),
    "i2.4xlarge": InstanceResources(cpus=16.0, mem=124928.0, disk=3200.0, gpus=0),
    "i2.8xlarge": InstanceResources(cpus=32.0, mem=249856.0, disk=6400.0, gpus=0),
    "i3.large": InstanceResources(cpus=2.0, mem=15616.0, disk=0.475, gpus=0),
    "i3.xlarge": InstanceResources(cpus=4.0, mem=31232.0, disk=0.95, gpus=0),
    "i3.2xlarge": InstanceResources(cpus=8.0, mem=62464.0, disk=1.9, gpus=0),
    "i3.4xlarge": InstanceResources(cpus=16.0, mem=124928.0, disk=3.8, gpus=0),
    "i3.8xlarge": InstanceResources(cpus=32.0, mem=249856.0, disk=7.6, gpus=0),
    "i3.16xlarge": InstanceResources(cpus=64.0, mem=499712.0, disk=15.2, gpus=0),
    "i3en.large": InstanceResources(cpus=2.0, mem=16384.0, disk=1250.0, gpus=0),
    "i3en.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=2500.0, gpus=0),
    "i3en.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=5000.0, gpus=0),
    "i3en.3xlarge": InstanceResources(cpus=12.0, mem=98304.0, disk=7500.0, gpus=0),
    "i3en.6xlarge": InstanceResources(cpus=24.0, mem=196608.0, disk=15000.0, gpus=0),
    "i3en.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=60000.0, gpus=0),
    "d2.xlarge": InstanceResources(cpus=4.0, mem=31232.0, disk=6000.0, gpus=0),
    "d2.2xlarge": InstanceResources(cpus=8.0, mem=62464.0, disk=12000.0, gpus=0),
    "d2.4xlarge": InstanceResources(cpus=16.0, mem=124928.0, disk=24000.0, gpus=0),
    "d2.8xlarge": InstanceResources(cpus=36.0, mem=249856.0, disk=48000.0, gpus=0),
    "z1d.large": InstanceResources(cpus=2.0, mem=16384.0, disk=75.0, gpus=0),
    "z1d.xlarge": InstanceResources(cpus=4.0, mem=32768.0, disk=150.0, gpus=0),
    "z1d.2xlarge": InstanceResources(cpus=8.0, mem=65536.0, disk=300.0, gpus=0),
    "z1d.3xlarge": InstanceResources(cpus=12.0, mem=98304.0, disk=450.0, gpus=0),
    "z1d.6xlarge": InstanceResources(cpus=24.0, mem=196608.0, disk=900.0, gpus=0),
    "z1d.12xlarge": InstanceResources(cpus=48.0, mem=393216.0, disk=1800.0, gpus=0),
    "g2.2xlarge": InstanceResources(cpus=8.0, mem=15360.0, disk=60.0, gpus=1),
    "g2.8xlarge": InstanceResources(cpus=32.0, mem=61440.0, disk=240.0, gpus=4),
    "g3.4xlarge": InstanceResources(cpus=16.0, mem=124928.0, disk=None, gpus=1),
    "g3.8xlarge": InstanceResources(cpus=32.0, mem=249856.0, disk=None, gpus=2),
    "g3.16xlarge": InstanceResources(cpus=64.0, mem=499712.0, disk=None, gpus=4),
    "g3s.xlarge": InstanceResources(cpus=4.0, mem=31232.0, disk=None, gpus=1),
    "g4dn.xlarge": InstanceResources(cpus=4.0, mem=16384.0, disk=125.0, gpus=1),
    "g4dn.2xlarge": InstanceResources(cpus=8.0, mem=32768.0, disk=225.0, gpus=1),
    "g4dn.4xlarge": InstanceResources(cpus=16.0, mem=65536.0, disk=225.0, gpus=1),
    "g4dn.8xlarge": InstanceResources(cpus=32.0, mem=131072.0, disk=900.0, gpus=1),
    "g4dn.16xlarge": InstanceResources(cpus=64.0, mem=262144.0, disk=900.0, gpus=1),
    "g4dn.12xlarge": InstanceResources(cpus=48.0, mem=196608.0, disk=900.0, gpus=4),
    "p2.xlarge": InstanceResources(cpus=4.0, mem=62464.0, disk=None, gpus=1),
    "p2.8xlarge": InstanceResources(cpus=32.0, mem=499712.0, disk=None, gpus=8),
    "p2.16xlarge": InstanceResources(cpus=64.0, mem=786432.0, disk=None, gpus=16),
    "p3.2xlarge": InstanceResources(cpus=8.0, mem=62464.0, disk=None, gpus=1),
    "p3.8xlarge": InstanceResources(cpus=32.0, mem=249856.0, disk=None, gpus=4),
    "p3.16xlarge": InstanceResources(cpus=64.0, mem=499712.0, disk=None, gpus=8),
    "p3dn.24xlarge": InstanceResources(cpus=96.0, mem=786432.0, disk=1800.0, gpus=8),
}

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


@lru_cache(maxsize=128)
def fetch_instance_type_from_aws(instance_type: str) -> InstanceResources:
    res = {}
    try:
        logger.info(f"fetching instance-type {instance_type} details from AWS.")
        res = ec2.describe_instance_types(InstanceTypes=[instance_type]).get("InstanceTypes")[0]
    except Exception as e:
        raise ValueError(f"Error occoured while describing instance type {instance_type} : {e}")

    vcpu_count = res.get("VCpuInfo", {}).get("DefaultVCpus", 0.0) + 0.0
    mem_size = res.get("MemoryInfo", {}).get("SizeInMiB", 0.0) + 0.0
    disk_size = res.get("InstanceStorageInfo", {}).get("TotalSizeInGB", None)
    gpu_size = res.get("GpuInfo", {}).get("Gpus", [{}])[0].get("Count", 0)

    return InstanceResources(vcpu_count, mem_size, disk_size, gpu_size)


def get_instance_type(instance_type: str) -> InstanceResources:
    if instance_type in EC2_INSTANCE_TYPES:
        return EC2_INSTANCE_TYPES[instance_type]
    else:
        return fetch_instance_type_from_aws(instance_type)


def get_market_resources(market: InstanceMarket) -> InstanceResources:
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
