import mock
import pytest

from clusterman.draining.queue import Host
from clusterman.draining.queue import main
from clusterman.draining.queue import process_drain_queue
from clusterman.draining.queue import process_queues
from clusterman.draining.queue import process_termination_queue
from clusterman.draining.queue import setup_config
from clusterman.draining.queue import SqsClient
from clusterman.draining.queue import terminate_host
from clusterman.mesos.spot_fleet_resource_group import SpotFleetResourceGroup


@pytest.fixture
def mock_sqs_client():
    with mock.patch(
        'clusterman.draining.queue.sqs', autospec=True
    ) as mock_sqs, mock.patch(
        'clusterman.draining.queue.staticconf', autospec=True
    ):
        mock_sqs.send_message = mock.Mock()
        mock_sqs.receive_message = mock.Mock()
        mock_sqs.delete_message = mock.Mock()
        return SqsClient('mycluster')


def test_submit_host_for_draining(mock_sqs_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_instance = mock.Mock(
            instance_id='i123',
            instance_ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        assert mock_sqs_client.submit_host_for_draining(
            mock_instance,
            sender=SpotFleetResourceGroup,
        ) == mock_sqs_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
            }
        )
        mock_sqs_client.client.send_message.assert_called_with(
            QueueUrl=mock_sqs_client.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'sfr',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_submit_host_for_termination(mock_sqs_client):
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
        assert mock_sqs_client.submit_host_for_termination(
            mock_host,
            delay=0,
        ) == mock_sqs_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
            }
        )
        mock_sqs_client.client.send_message.assert_called_with(
            QueueUrl=mock_sqs_client.termination_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'clusterman',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )

        assert mock_sqs_client.submit_host_for_termination(
            mock_host,
        ) == mock_sqs_client.client.send_message.return_value
        mock_json.dumps.assert_called_with(
            {
                'instance_id': 'i123',
                'ip': '10.1.1.1',
                'hostname': 'host123',
                'group_id': 'sfr123',
            }
        )
        mock_sqs_client.client.send_message.assert_called_with(
            QueueUrl=mock_sqs_client.termination_queue_url,
            DelaySeconds=mock_staticconf.read_int.return_value,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': 'clusterman',
                },
            },
            MessageBody=mock_json.dumps.return_value,
        )


def test_get_host_to_drain(mock_sqs_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_sqs_client.client.receive_message.return_value = {'Messages': []}
        assert mock_sqs_client.get_host_to_terminate() is None
        mock_sqs_client.client.receive_message.return_value = {'Messages': [{
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

        assert mock_sqs_client.get_host_to_terminate() == Host(
            sender='clusterman',
            receipt_handle='receipt_id',
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        mock_json.loads.assert_called_with('Helloworld')
        mock_sqs_client.client.receive_message.assert_called_with(
            QueueUrl=mock_sqs_client.drain_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        )


def test_get_host_to_terminate(mock_sqs_client):
    with mock.patch(
        'clusterman.draining.queue.json', autospec=True,
    ) as mock_json:
        mock_sqs_client.client.receive_message.return_value = {'Messages': []}
        assert mock_sqs_client.get_host_to_terminate() is None
        mock_sqs_client.client.receive_message.return_value = {'Messages': [{
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

        assert mock_sqs_client.get_host_to_terminate() == Host(
            sender='clusterman',
            receipt_handle='receipt_id',
            instance_id='i123',
            ip='10.1.1.1',
            hostname='host123',
            group_id='sfr123',
        )
        mock_json.loads.assert_called_with('Helloworld')
        mock_sqs_client.client.receive_message.assert_called_with(
            QueueUrl=mock_sqs_client.termination_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        )


def test_delete_drain_message(mock_sqs_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_sqs_client.delete_drain_messages(mock_hosts)
    mock_sqs_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_sqs_client.drain_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_sqs_client.drain_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_delete_terminate_message(mock_sqs_client):
    mock_hosts = [
        mock.Mock(receipt_handle=1),
        mock.Mock(receipt_handle=2),
    ]

    mock_sqs_client.delete_terminate_messages(mock_hosts)
    mock_sqs_client.client.delete_message.assert_has_calls([
        mock.call(
            QueueUrl=mock_sqs_client.termination_queue_url,
            ReceiptHandle=1,
        ),
        mock.call(
            QueueUrl=mock_sqs_client.termination_queue_url,
            ReceiptHandle=2,
        ),
    ])


def test_process_termination_queue(mock_sqs_client):
    with mock.patch(
        'clusterman.draining.queue.terminate_host', autospec=True,
    ) as mock_terminate, mock.patch(
        'clusterman.draining.queue.down', autospec=True,
    ) as mock_down, mock.patch(
        'clusterman.draining.queue.up', autospec=True,
    ) as mock_up, mock.patch(
        'clusterman.draining.queue.SqsClient.get_host_to_terminate', autospec=True,
    ) as mock_get_host_to_terminate, mock.patch(
        'clusterman.draining.queue.SqsClient.delete_terminate_messages', autospec=True,
    ) as mock_delete_terminate_messages:
        mock_mesos_client = mock.Mock()
        mock_get_host_to_terminate.return_value = None
        process_termination_queue(mock_sqs_client, mock_mesos_client, 'mesos.yelp.com')
        assert mock_sqs_client.get_host_to_terminate.called
        assert not mock_terminate.called
        assert not mock_delete_terminate_messages.called

        mock_host = mock.Mock(hostname='')
        mock_get_host_to_terminate.return_value = mock_host
        process_termination_queue(mock_sqs_client, mock_mesos_client, 'mesos.yelp.com')
        assert mock_sqs_client.get_host_to_terminate.called
        mock_terminate.assert_called_with(mock_host)
        assert not mock_down.called
        assert not mock_up.called
        mock_delete_terminate_messages.assert_called_with(mock_sqs_client, [mock_host])

        mock_host = mock.Mock(hostname='host1', ip='10.1.1.1')
        mock_get_host_to_terminate.return_value = mock_host
        process_termination_queue(mock_sqs_client, mock_mesos_client, 'mesos.yelp.com')
        assert mock_sqs_client.get_host_to_terminate.called
        mock_terminate.assert_called_with(mock_host)
        mock_down.assert_called_with(mock_mesos_client, ['host1|10.1.1.1'])
        mock_up.assert_called_with(mock_mesos_client, ['host1|10.1.1.1'])
        mock_delete_terminate_messages.assert_called_with(mock_sqs_client, [mock_host])


def test_process_drain_queue(mock_sqs_client):
    with mock.patch(
        'clusterman.draining.queue.drain', autospec=True,
    ) as mock_drain, mock.patch(
        'clusterman.draining.queue.SqsClient.get_host_to_drain', autospec=True,
    ) as mock_get_host_to_drain, mock.patch(
        'clusterman.draining.queue.SqsClient.delete_drain_messages', autospec=True,
    ) as mock_delete_drain_messages, mock.patch(
        'clusterman.draining.queue.SqsClient.submit_host_for_termination', autospec=True,
    ) as mock_submit_host_for_termination, mock.patch(
        'clusterman.draining.queue.datetime.datetime', autospec=True,
    ) as mock_date, mock.patch(
        'clusterman.draining.queue.staticconf.read_int', autospec=True, return_value=1,
    ):
        mock_now = mock.Mock(return_value=mock.Mock(strftime=mock.Mock(return_value=1)))
        mock_date.now = mock_now
        mock_mesos_client = mock.Mock()
        mock_get_host_to_drain.return_value = None
        process_drain_queue(mock_sqs_client, mock_mesos_client, 'mesos.yelp.com')
        assert mock_sqs_client.get_host_to_drain.called
        assert not mock_drain.called
        assert not mock_submit_host_for_termination.called

        mock_host = mock.Mock(hostname='')
        mock_get_host_to_drain.return_value = mock_host
        process_drain_queue(mock_sqs_client, mock_mesos_client, 'mesos.yelp.com')
        mock_submit_host_for_termination.assert_called_with(mock_sqs_client, mock_host, delay=0)
        mock_delete_drain_messages.assert_called_with(mock_sqs_client, [mock_host])
        assert not mock_drain.called

        mock_host = mock.Mock(hostname='host1', ip='10.1.1.1')
        mock_get_host_to_drain.return_value = mock_host
        process_drain_queue(mock_sqs_client, mock_mesos_client, 'mesos.yelp.com')
        assert mock_sqs_client.get_host_to_drain.called
        mock_drain.assert_called_with(
            mock_mesos_client,
            ['host1|10.1.1.1'],
            1000000000,
            1000000000,
        )
        mock_submit_host_for_termination.assert_called_with(mock_sqs_client, mock_host)
        mock_delete_drain_messages.assert_called_with(mock_sqs_client, [mock_host])


def test_process_queues():
    with mock.patch(
        'clusterman.draining.queue.SqsClient', autospec=True,
    ), mock.patch(
        'clusterman.draining.queue.staticconf.read_string', return_value='westeros-prod', autospec=True
    ), mock.patch(
        'clusterman.draining.queue.process_drain_queue', autospec=True,
    ) as mock_process_drain_queue, mock.patch(
        'clusterman.draining.queue.process_termination_queue', autospec=True,
    ) as mock_process_termination_queue, mock.patch(
        'clusterman.draining.queue.time.sleep', autospec=True, side_effect=LoopBreak
    ):
        with pytest.raises(LoopBreak):
            process_queues('westeros-prod')
        assert mock_process_termination_queue.called
        assert mock_process_drain_queue.called


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
