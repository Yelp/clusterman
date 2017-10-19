
class ClustermanException(Exception):
    pass


class MesosRoleManagerError(ClustermanException):
    pass


class ResourceGroupProtectedException(ClustermanException):
    pass


class MarketProtectedException(ClustermanException):
    pass


class ResourceGroupError(ClustermanException):
    pass
