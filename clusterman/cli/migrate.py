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
import time

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import subparser
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.migration.event import ConditionOperator
from clusterman.migration.event import ConditionTrait
from clusterman.migration.event import MigrationCondition
from clusterman.migration.event import MigrationEvent
from clusterman.migration.event_enums import MigrationStatus


def _get_cluster_connector(args: argparse.Namespace) -> KubernetesClusterConnector:
    connector = KubernetesClusterConnector(args.cluster, args.pool, init_crd=True)
    connector.reload_client()
    return connector


def main_start(args: argparse.Namespace) -> None:
    condition = MigrationCondition.from_dict(
        {name.split("_", 1)[1]: val for name, val in vars(args).items() if name.startswith("condition_")},
    )
    event = MigrationEvent(
        resource_name="-".join((args.cluster, args.pool, str(int(time.time())))),
        cluster=args.cluster,
        pool=args.pool,
        label_selectors=args.label_selector,
        condition=condition,
    )
    connector = _get_cluster_connector(args)
    connector.create_node_migration_resource(event, MigrationStatus.PENDING)


def main_stop(args: argparse.Namespace) -> None:
    connector = _get_cluster_connector(args)
    events = connector.list_node_migration_resources(statuses=[MigrationStatus.PENDING, MigrationStatus.INPROGRESS])
    for event in events:
        print(f"Marking event {event} to be stopped")
        connector.mark_node_migration_resource(event.resource_name, MigrationStatus.STOP)


def _setup_common_args(
    required_named_args: argparse._ArgumentGroup,
    optional_named_args: argparse._ArgumentGroup,
):
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
    add_cluster_config_directory_arg(optional_named_args)
    # For CLI compatibility reasons, as the only supported scheduler is k8s
    add_scheduler_arg(optional_named_args)


@subparser("migrate", "trigger node migration for a pool", main_start)
def add_migration_parser(
    subparser: argparse.ArgumentParser,
    required_named_args: argparse._ArgumentGroup,
    optional_named_args: argparse._ArgumentGroup,
):  # pragma: no cover
    _setup_common_args(required_named_args, optional_named_args)
    condition_group = subparser.add_argument_group(
        title="migration condition", description="Defines the desired final state for the migration"
    )
    condition_group.add_argument(
        "condition_trait",
        type=str,
        help="Metadata based on which nodes to migrate are selected",
        choices=[entry.value for entry in ConditionTrait],
    )
    condition_group.add_argument(
        "condition_operator",
        type=str,
        help="Metadata comparison operator",
        choices=[entry.value for entry in ConditionOperator],
    )
    condition_group.add_argument(
        "condition_target",
        type=str,
        help="Target value for node selection condition",
    )
    optional_named_args.add_argument(
        "--label-selector",
        type=str,
        action="append",
        help="Further filter node selection with label selector expression",
        default=[],
    )


@subparser("migrate-stop", "stop node migration for a pool", main_stop)
def add_migration_stop_parser(
    subparser: argparse.ArgumentParser,
    required_named_args: argparse._ArgumentGroup,
    optional_named_args: argparse._ArgumentGroup,
):  # pragma: no cover
    _setup_common_args(required_named_args, optional_named_args)
