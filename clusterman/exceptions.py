
class ClustermanException(Exception):
    pass


class AutoscalerError(ClustermanException):
    pass


class ClustermanSignalError(ClustermanException):
    pass


class MetricsError(ClustermanException):
    pass


class MesosPoolManagerError(ClustermanException):
    pass


class NoSignalConfiguredException(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass


class SignalValidationError(ClustermanSignalError):
    pass


class SignalConnectionError(ClustermanSignalError):
    pass


class SimulationError(ClustermanException):
    pass
