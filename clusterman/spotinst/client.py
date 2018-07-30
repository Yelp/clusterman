import json

from spotinst_sdk import SpotinstClient

from clusterman.config import get_spotinst_config_path


_CLIENT = None


def get_spotinst_client():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = _create_spotinst_client()

    return _CLIENT


def _create_spotinst_client():
    with open(get_spotinst_config_path()) as fd:
        data = json.load(fd)

    return SpotinstClient(
        auth_token=data['api_token'],
        account_id=data['account_id']
    )
