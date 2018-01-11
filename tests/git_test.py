import mock
import pytest

from clusterman.exceptions import GitError
from clusterman.git import _add_clusterman_signals_to_path
from clusterman.git import _sha_from_branch_or_tag


@pytest.fixture
def mock_sha():
    with mock.patch('clusterman.git._sha_from_branch_or_tag') as m:
        m.return_value = 'abcdefabcdefabcdefabcdefabcdefabcdefabcd'
        yield


@pytest.fixture
def mock_cache():
    with mock.patch('clusterman.git._get_cache_location') as m:
        m.return_value = '/foo'
        yield


@mock.patch('clusterman.git.subprocess.run')
def test_sha_from_branch_or_tag(mock_run):
    mock_run.return_value.returncode = 0
    mock_run.return_value.stdout = 'abcdefabcdefabcdefabcdefabcdefabcdefabcd\trefs/heads/a_branch'.encode()
    assert _sha_from_branch_or_tag('a_branch') == 'abcdefabcdefabcdefabcdefabcdefabcdefabcd'


@mock.patch('clusterman.git.subprocess.run')
def test_sha_from_branch_or_tag_failed(mock_run):
    mock_run.return_value.returncode = 2
    with pytest.raises(GitError):
        _sha_from_branch_or_tag('a_branch')


@mock.patch('clusterman.git.sys')
@mock.patch('clusterman.git.logger')
@mock.patch('clusterman.git.subprocess.run')
class TestAddClustermanSignalsToPath:
    def test_already_added(self, mock_run, mock_logger, mock_sys, mock_sha, mock_cache):
        mock_sys.path = ['/foo/clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd']
        _add_clusterman_signals_to_path('a_branch')
        assert mock_run.call_count == 0
        assert mock_logger.debug.call_count == 0
        assert mock_sys.path == ['/foo/clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd']

    def test_not_present(self, mock_run, mock_logger, mock_sys, mock_sha, mock_cache):
        mock_sys.path = []
        _add_clusterman_signals_to_path('a_branch')
        assert mock_run.call_count == 1
        assert mock_logger.debug.call_count == 0
        assert mock_sys.path == ['/foo/clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd']

    def test_present(self, mock_run, mock_logger, mock_sys, mock_sha, mock_cache):
        mock_sys.path = []
        with mock.patch('clusterman.git.os.path.exists') as mock_exists:
            mock_exists.return_value = True
            _add_clusterman_signals_to_path('a_branch')

        assert mock_run.call_count == 0
        assert mock_logger.debug.call_count == 1
        assert mock_sys.path == ['/foo/clusterman_signals_abcdefabcdefabcdefabcdefabcdefabcdefabcd']
