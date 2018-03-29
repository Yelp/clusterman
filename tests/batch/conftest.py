import pytest
import staticconf.testing


@pytest.fixture(autouse=True)
def mock_setup_config_directory():
    with staticconf.testing.PatchConfiguration(
        {'cluster_config_directory': '/a/fake/directory/'}
    ):
        yield
