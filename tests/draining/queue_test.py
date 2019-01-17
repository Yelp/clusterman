import arrow
import mock
import pytest

from clusterman.draining.queue import DrainingClient
from clusterman.draining.queue import Host
from clusterman.draining.queue import main
from clusterman.draining.queue import process_queues
from clusterman.draining.queue import setup_config
from clusterman.draining.queue import terminate_host
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup


@pytest.fixture
def mock_draining_client():
    with mock.patch(
        'clusterman.draining.queue.sqs', autospec=True
    ) as mock_sqs, mock.patch(
        'clusterman.draining.queue.staticconf', autospec=True
    ):
        mock_sqs.send_message = mock.Mock()
        mock_sqs.receive_message = mock.Mock()
        mock_sqs.delete_message = mock.Mock()
        return DrainingClient('mycluster')


def test_submit_host_for_draining(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_instance = mock.Mock(
            instance_id='i123',
            instance_ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        assert mock_draining_client.submit_host_for_draining(
            mock_instance,
            sender=SpotFleetResourceGroup,
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'sfr',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_submit_host_for_termination(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json, mock.patch(
        'clusterman.draining.queue.staticconf', autospec=True,
    ) as mock_staticconf:
        mock_host = mock.Mock(
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
            sender='clusterman',
        )
        assert mock_draining_client.submit_host_for_termination(
            mock_host,
            delay=0,
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.termination_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'clusterman',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )

        assert mock_draining_client.submit_host_for_termination(
            mock_host,
        ) == mock_draining_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
            }
        )
        mock_draining_client.client.send_message.assert_called_with(
            QueueUrl=mock_draining_client.termination_queue_url,
            DelaySeconds=mock_staticconf.read_int.return_value,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'clusterman',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_get_host_to_drain(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_draining_client.client.receive_message.return_value = {'Messages': []}
        assert mock_draining_client.get_host_to_terminate() is None
        mock_draining_client.client.receive_message.return_value = {'Messages': [{
            'MessageAttributes': {'Sender': {'StringValue': 'clusterman'}},
            'ReceiptHandle': 'receipt_id',
            'Body': 'Helloworld',
        }]}
        mock_json.loads.return_value = {
            'instance_id': 'i123',
            'ip': '10.1.1.1',
            'hostname': 'host123',
            'group_id': 'sfr123',
        }

        assert mock_draining_client.get_host_to_terminate() == Host(
            sender='clusterman',
            receipt_handle='receipt_id',
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        mock_json.loads.assert_called_with('Helloworld')
        mock_draining_client.client.receive_message.assert_called_with(
            QueueUrl=mock_draining_client.drain_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        )


def test_get_host_to_terminate(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_draining_client.client.receive_message.return_value = {'Messages': []}
        assert mock_draining_client.get_host_to_terminate() is None
        mock_draining_client.client.receive_message.return_value = {'Messages': [{
            'MessageAttributes': {'Sender': {'StringValue': 'clusterman'}},
            'ReceiptHandle': 'receipt_id',
            'Body': 'Helloworld',
        }]}
        mock_json.loads.return_value = {
            'instance_id': 'i123',
            'ip': '10.1.1.1',
            'hostname': 'host123',
            'group_id': 'sfr123',
        }

        assert mock_draining_client.get_host_to_terminate() == Host(
            sender='clusterman',
            receipt_handle='receipt_id',
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        mock_json.loads.assert_called_with('Helloworld')
        mock_draining_client.client.receive_message.assert_called_with(
            QueueUrl=mock_draining_client.termination_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        )


def test_delete_drain_message(mock_draining_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_draining_client.delete_drain_messages(mock_hosts)
    mock_draining_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_draining_client.drain_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_draining_client.drain_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_delete_terminate_message(mock_draining_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_draining_client.delete_terminate_messages(mock_hosts)
    mock_draining_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_draining_client.termination_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_draining_client.termination_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_process_termination_queue(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.terminate_host', autospec=True,
    ) as mock_terminate, mock.patch(
        'clusterman.draining.queue.down', autospec=True,
    ) as mock_down, mock.patch(
        'clusterman.draining.queue.up', autospec=True,
    ) as mock_up, mock.patch(
        'clusterman.draining.queue.DrainingClient.get_host_to_terminate', autospec=True,
    ) as mock_get_host_to_terminate, mock.patch(
        'clusterman.draining.queue.DrainingClient.delete_terminate_messages', autospec=True,
    ) as mock_delete_terminate_messages:
        mock_mesos_client = mock.Mock()
        mock_get_host_to_terminate.return_value = None
        mock_draining_client.process_termination_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_terminate.called
        assert not mock_terminate.called
        assert not mock_delete_terminate_messages.called

        mock_host = mock.Mock(hostname='', instance_id='i123')
        mock_draining_client.draining_host_ttl_cache[mock_host.instance_id] = arrow.now()
        mock_get_host_to_terminate.return_value = mock_host
        mock_draining_client.process_termination_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_terminate.called
        mock_terminate.assert_called_with(mock_host)
        assert not mock_down.called
        assert not mock_up.called
        mock_delete_terminate_messages.assert_called_with(mock_draining_client, [mock_host])

        mock_host = mock.Mock(hostname='host1', ip='10.1.1.1', instance_id='i123')
        mock_draining_client.draining_host_ttl_cache[mock_host.instance_id] = arrow.now()
        mock_get_host_to_terminate.return_value = mock_host
        mock_draining_client.process_termination_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_terminate.called
        mock_terminate.assert_called_with(mock_host)
        mock_down.assert_called_with(mock_mesos_client, ['host1|10.1.1.1'])
        mock_up.assert_called_with(mock_mesos_client, ['host1|10.1.1.1'])
        mock_delete_terminate_messages.assert_called_with(mock_draining_client, [mock_host])


def test_process_drain_queue(mock_draining_client):
    with mock.patch(
        'clusterman.draining.queue.drain', autospec=True,
    ) as mock_drain, mock.patch(
        'clusterman.draining.queue.DrainingClient.get_host_to_drain', autospec=True,
    ) as mock_get_host_to_drain, mock.patch(
        'clusterman.draining.queue.DrainingClient.delete_drain_messages', autospec=True,
    ) as mock_delete_drain_messages, mock.patch(
        'clusterman.draining.queue.DrainingClient.submit_host_for_termination', autospec=True,
    ) as mock_submit_host_for_termination, mock.patch(
        'clusterman.draining.queue.arrow', autospec=False,
    ) as mock_arrow, mock.patch(
        'clusterman.draining.queue.staticconf.read_int', autospec=True, return_value=1,
    ):
        mock_arrow.now = mock.Mock(return_value=mock.Mock(timestamp=1))
        mock_mesos_client = mock.Mock()
        mock_get_host_to_drain.return_value = None
        mock_draining_client.process_drain_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_drain.called
        assert not mock_drain.called
        assert not mock_submit_host_for_termination.called

        mock_host = mock.Mock(hostname='')
        mock_get_host_to_drain.return_value = mock_host
        mock_draining_client.process_drain_queue(mock_mesos_client)
        mock_submit_host_for_termination.assert_called_with(mock_draining_client, mock_host, delay=0)
        mock_delete_drain_messages.assert_called_with(mock_draining_client, [mock_host])
        assert not mock_drain.called

        mock_host = Host(
            hostname='host1',
            ip='10.1.1.1',
            group_id='sfr1',
            instance_id='i123',
            sender='mmb',
            receipt_handle='aaaaa',
        )
        mock_get_host_to_drain.return_value = mock_host
        mock_draining_client.process_drain_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_drain.called
        mock_drain.assert_called_with(
            mock_mesos_client,
            ['host1|10.1.1.1'],
            1000000000,
            1000000000,
        )
        mock_submit_host_for_termination.assert_called_with(mock_draining_client, mock_host)
        mock_delete_drain_messages.assert_called_with(mock_draining_client, [mock_host])

        # test we can't submit same host twice
        mock_host = Host(
            hostname='host1',
            ip='10.1.1.1',
            group_id='sfr1',
            instance_id='i123',
            sender='mmb',
            receipt_handle='bbb',
        )
        mock_drain.reset_mock()
        mock_submit_host_for_termination.reset_mock()
        mock_get_host_to_drain.return_value = mock_host
        mock_draining_client.process_drain_queue(mock_mesos_client)
        assert mock_draining_client.get_host_to_drain.called
        assert not mock_drain.called
        assert not mock_submit_host_for_termination.called
        mock_delete_drain_messages.assert_called_with(mock_draining_client, [mock_host])


def test_clean_processing_hosts_cache(mock_draining_client):
    mock_draining_client.draining_host_ttl_cache['i123'] = arrow.get('2018-12-17T16:01:59')
    mock_draining_client.draining_host_ttl_cache['i456'] = arrow.get('2018-12-17T16:02:00')
    with mock.patch(
        'clusterman.draining.queue.arrow', autospec=False
    ) as mock_arrow, mock.patch(
        'clusterman.draining.queue.DRAIN_CACHE_SECONDS', 60
    ):
        mock_arrow.now = mock.Mock(return_value=arrow.get('2018-12-17T16:02:00'))
        mock_draining_client.clean_processing_hosts_cache()
        assert 'i123' not in mock_draining_client.draining_host_ttl_cache
        assert 'i456' in mock_draining_client.draining_host_ttl_cache


def test_process_queues():
    with mock.patch(
        'clusterman.draining.queue.DrainingClient', autospec=True,
    ) as mock_draining_client, mock.patch(
        'clusterman.draining.queue.staticconf.read_string', return_value='westeros-prod', autospec=True
    ), mock.patch(
        'clusterman.draining.queue.time.sleep', autospec=True, side_effect=LoopBreak
    ):
        with pytest.raises(LoopBreak):
            process_queues('westeros-prod')
        assert mock_draining_client.return_value.process_termination_queue.called
        assert mock_draining_client.return_value.process_drain_queue.called
        assert mock_draining_client.return_value.clean_processing_hosts_cache.called


def test_terminate_host():
    mock_host = mock.Mock(instance_id='i123', sender='sfr', group_id='sfr123')
    mock_sfr = mock.Mock()
    with mock.patch.dict(
        'clusterman.draining.queue.RESOURCE_GROUPS', {'sfr': mock_sfr}, clear=True
    ):
        terminate_host(mock_host)
        mock_sfr.assert_called_with('sfr123')
        mock_sfr.return_value.terminate_instances_by_id.assert_called_with(['i123'])


def test_main():
    with mock.patch(
        'clusterman.draining.queue.setup_config', autospec=True,
    ), mock.patch(
        'clusterman.draining.queue.process_queues', autospec=True,
    ) as mock_process_queues:
        main(mock.Mock())
        assert mock_process_queues.called


def test_setup_config():
    with mock.patch(
        'clusterman.draining.queue.load_default_config', autospec=True,
    ), mock.patch(
        'clusterman.draining.queue.staticconf', autospec=True,
    ):
        setup_config('clustername', '/nail/blah', 'debug')


class LoopBreak(Exception):
    pass
