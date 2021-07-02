"""ERRORS"""


class Error(Exception):

    def __init__(self, message, status=500):
        self.message = message
        self.status = status

    @property
    def serialize(self):
        return {
            'status': self.status,
            'message': self.message
        }

    def __str__(self):
        return self.message


class CartoError(Error):
    pass


class GeostoreNotFound(Error):
    pass


class DBError(Error):
    pass


class CacheError(Error):
    pass
