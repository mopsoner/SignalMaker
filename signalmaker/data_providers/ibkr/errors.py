class IBKRError(Exception):
    pass


class IBKRDisabledError(IBKRError):
    pass


class IBKRGatewayConnectionError(IBKRError):
    pass


class IBKRContractNotFoundError(IBKRError):
    pass


class IBKRContractAmbiguousError(IBKRError):
    pass


class IBKRHistoricalDataError(IBKRError):
    pass
