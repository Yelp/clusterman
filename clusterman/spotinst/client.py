import json

from spotinst_sdk import SpotinstClient


SPOTINST_CONFIG = '/nail/etc/spotinst.json'
_CLIENT = None


def get_spotinst_client():
    global _CLIENT
    return _CLIENT


class Client(object):
    def __init__(self):
        self.client = None
        self._account_id = None
        self._api_token = None
        self._load_config()
        self._initialize_client()

    def _load_config(self):
        with open(SPOTINST_CONFIG) as fd:
            data = json.load(fd)

        self._account_id = data['account_id']
        self._api_token = data['api_token']

    def _initialize_client(self):
        self.client = SpotinstClient(
            auth_token=self._api_token,
            account_id=self._account_id
        )


if _CLIENT is None:
    _CLIENT = Client()
