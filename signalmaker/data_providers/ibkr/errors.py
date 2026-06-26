class IBKRError(Exception):
    pass


class IBKRDisabledError(IBKRError):
    pass


class IBKRMissingTokenError(IBKRError):
    pass


class IBKRRequestError(IBKRError):
    pass


class IBKRNoDataError(IBKRError):
    pass


class IBKRAuthConfigurationError(IBKRError):
    pass
