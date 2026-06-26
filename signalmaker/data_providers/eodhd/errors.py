class EODHDError(Exception):
    pass

class EODHDDisabledError(EODHDError):
    pass

class EODHDMissingApiKeyError(EODHDError):
    pass

class EODHDRequestError(EODHDError):
    pass

class EODHDNoDataError(EODHDError):
    pass
