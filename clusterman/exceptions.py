
class ClustermanException(Exception):
    pass


class MetricsError(ClustermanException):
    pass


class MesosRoleManagerError(ClustermanException):
    pass


class NoSignalConfiguredException(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass


class ClustermanSignalError(ClustermanException):
    pass


class SignalValidationError(ClustermanSignalError):
    pass


class SignalConnectionError(ClustermanSignalError):
    pass


class SimulationError(ClustermanException):
    pass
