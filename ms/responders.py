from hyp.marshmallow import Responder
from ms.schemas import ErrorSchema


class ErrorResponder(Responder):
    TYPE = 'errors'
    SERIALIZER = ErrorSchema
