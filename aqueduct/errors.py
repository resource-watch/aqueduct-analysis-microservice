"""ERRORS"""


class Error(Exception):

    def __init__(self, message):
        self.message = message

    @property
    def serialize(self):
        return {
            'message': self.message
        }

class CartoError(Error):
    pass

class GeostoreNotFound(Error):
    pass

class DBError(Error):
    pass
