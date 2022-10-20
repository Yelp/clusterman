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
from unittest import mock

import pytest
import staticconf.testing

from clusterman.batch.drainer import NodeDrainerBatch


class LoopBreak(Exception):
    pass


def test_drainer_batch_process_queues():
    batch = NodeDrainerBatch()
    batch.run_interval = 5
    batch.logger = mock.MagicMock()
    batch.options = mock.MagicMock(cluster="westeros-prod", autorestart_interval_minutes=0)
    with mock.patch(
        "clusterman.batch.drainer.DrainingClient",
        autospec=True,
    ) as mock_draining_client, staticconf.testing.PatchConfiguration(
        {
            "clusters": {
                "westeros-prod": {
                    "mesos_master_fqdn": "westeros-prod",
                    "cluster_manager": "mesos",
                }
            }
        },
    ), mock.patch(
        "clusterman.batch.drainer.time.sleep", autospec=True, side_effect=LoopBreak
    ), mock.patch(
        "clusterman.batch.drainer.KubernetesClusterConnector",
        autospec=True,
    ):

        mock_draining_client.return_value.process_termination_queue.return_value = False
        mock_draining_client.return_value.process_drain_queue.return_value = False
        mock_draining_client.return_value.process_warning_queue.return_value = False
        with pytest.raises(LoopBreak):
            batch.run()
        assert mock_draining_client.return_value.process_termination_queue.called
        assert mock_draining_client.return_value.process_drain_queue.called
        assert mock_draining_client.return_value.clean_processing_hosts_cache.called
        assert mock_draining_client.return_value.process_warning_queue.called
