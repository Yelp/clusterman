import json

from spotinst_sdk import SpotinstClient


SPOTINST_CONFIG = '/nail/etc/spotinst.json'
_CLIENT = None


def get_spotinst_client():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = _create_spotinst_client()

    return _CLIENT


def _create_spotinst_client():
    with open(SPOTINST_CONFIG) as fd:
        data = json.load(fd)

    return SpotinstClient(
        auth_token=data['api_token'],
        account_id=data['account_id']
    )
