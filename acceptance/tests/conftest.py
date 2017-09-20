import pytest
from webtest import TestApp
from yelp_compose.testing import sandbox


@pytest.fixture(scope="session")
def testapp():
    with sandbox.running_sandbox():
        yield TestApp('http://' + sandbox.get_service_uri('clusterman'))
