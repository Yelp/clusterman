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
# K8s CRD
MIGRATION_CRD_GROUP = "clusterman.yelp.com"
MIGRATION_CRD_VERSION = "v1"
MIGRATION_CRD_PLURAL = "nodemigrations"
MIGRATION_CRD_KIND = "NodeMigration"
MIGRATION_CRD_STATUS_LABEL = "clusterman.yelp.com/migration_status"
MIGRATION_CRD_ATTEMPTS_LABEL = "clusterman.yelp.com/attempts"

# Default settings
DEFAULT_POOL_PRESCALING = 0
DEFAULT_NODE_BOOT_WAIT = "3m"
DEFAULT_NODE_BOOT_TIMEOUT = "10m"
DEFAULT_WORKER_TIMEOUT = "2h"
DEFAULT_HEALTH_CHECK_INTERVAL = "2m"
DEFAULT_ALLOWED_FAILED_DRAINS = 3
DEFAULT_ORPHAN_CAPACITY_TOLLERANCE = 0
DEFAULT_MAX_UPTIME_WORKER_SKIPS = 6
MAX_ORPHAN_CAPACITY_TOLLERANCE = 0.2

# Worker parameters
UPTIME_CHECK_INTERVAL_SECONDS = 60 * 60  # 1 hour
INITIAL_POOL_HEALTH_TIMEOUT_SECONDS = 15 * 60
SUPPORTED_POOL_SCHEDULER = "kubernetes"

# SFX metrics keys
SFX_NODE_DRAIN_COUNT = "clusterman.node_migration.drain_count"
SFX_MIGRATION_JOB_DURATION = "clusterman.node_migration.duration"
SFX_DRAINED_NODE_UPTIME = "clusterman.node_migration.drained_node_uptime"
