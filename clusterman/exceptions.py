
class ClustermanException(Exception):
    pass


class MarketProtectedException(ClustermanException):
    pass


class MetricsError(ClustermanException):
    pass


class MesosRoleManagerError(ClustermanException):
    pass


class SignalConfigurationError(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass


class ResourceGroupProtectedException(ClustermanException):
    pass
