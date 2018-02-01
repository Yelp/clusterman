
class ClustermanException(Exception):
    pass


class MetricsError(ClustermanException):
    pass


class MesosRoleManagerError(ClustermanException):
    pass


class NoSignalConfiguredException(ClustermanException):
    pass


class SignalValidationError(ClustermanException):
    pass


class SignalConnectionError(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass
