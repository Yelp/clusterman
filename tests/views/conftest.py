import pytest
from pyramid.testing import DummyRequest
from webtest import TestApp

from clusterman.webapp import create_application


@pytest.fixture
def dummy_request():
    return DummyRequest()


@pytest.fixture
def test_app():
    """Creates a test app for use in integration tests."""
    return TestApp(create_application('config.yaml', 'config-env-dev.yaml'))
