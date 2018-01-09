
class ClustermanException(Exception):
    pass


class GitError(ClustermanException):
    pass


class MetricsError(ClustermanException):
    pass


class MesosRoleManagerError(ClustermanException):
    pass


class SignalConfigurationError(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass
