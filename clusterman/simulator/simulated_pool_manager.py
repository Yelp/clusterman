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
from typing import cast
from typing import Collection
from typing import Optional
from typing import Sequence

import staticconf

from clusterman.autoscaler.pool_manager import MAX_MIN_NODE_SCALEIN_UPTIME_SECONDS
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.config import POOL_NAMESPACE
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.interfaces.types import InstanceMetadata
from clusterman.simulator import simulator
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster
from clusterman.simulator.simulated_cluster_connector import SimulatedClusterConnector
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup
from clusterman.util import read_int_or_inf


class SimulatedPoolManager(PoolManager):
    def __init__(
        self,
        cluster: str,
        pool: str,
        configs: Sequence,
        simulator: "simulator.Simulator",
    ) -> None:
        self.draining_enabled = False
        self.cluster = cluster
        self.pool = pool
        self.simulator = simulator
        groups = [SimulatedSpotFleetResourceGroup(config, self.simulator) for config in configs]
        self.resource_groups = {group.id: group for group in groups}
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool, scheduler="mesos"))
        self.min_capacity = self.pool_config.read_int("scaling_limits.min_capacity")
        self.max_capacity = self.pool_config.read_int("scaling_limits.max_capacity")
        self.max_tasks_to_kill = read_int_or_inf(self.pool_config, "scaling_limits.max_tasks_to_kill")
        self.cluster_connector = SimulatedClusterConnector(self.cluster, self.pool, self.simulator)
        self.max_weight_to_add = self.pool_config.read_int("scaling_limits.max_weight_to_add")
        self.max_weight_to_remove = self.pool_config.read_int("scaling_limits.max_weight_to_remove")
        self.min_node_scalein_uptime = min(
            self.pool_config.read_int("scaling_limits.min_node_scalein_uptime_seconds", default=-1),
            MAX_MIN_NODE_SCALEIN_UPTIME_SECONDS,
        )

    def reload_state(self) -> None:
        pass

    def get_node_metadatas(
        self,
        aws_state_filter: Optional[Collection[str]] = None,
    ) -> Sequence[ClusterNodeMetadata]:
        agent_metadatas = []
        for group in self.resource_groups.values():
            for instance in cast(SimulatedAWSCluster, group).instances.values():
                if aws_state_filter and "running" not in aws_state_filter:
                    continue

                metadata = ClusterNodeMetadata(
                    self.cluster_connector.get_agent_metadata(instance.ip_address),
                    InstanceMetadata(
                        group_id=group.id,
                        hostname=f"{instance.id}.com",
                        instance_id=instance.id,
                        ip_address=instance.ip_address,
                        is_stale=group.is_stale,
                        market=instance.market,
                        state="running",
                        uptime=(self.simulator.current_time - instance.start_time),
                        weight=group.market_weight(instance.market),
                    ),
                )
                agent_metadatas.append(metadata)

        return agent_metadatas

    @property
    def non_orphan_fulfilled_capacity(self):
        return self._calculate_non_orphan_fulfilled_capacity()
