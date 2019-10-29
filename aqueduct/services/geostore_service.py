"""Geostore SERVICE"""
from CTRegisterMicroserviceFlask import request_to_microservice
from CTRegisterMicroserviceFlask.errors import NotFound

from aqueduct.errors import GeostoreNotFound


class GeostoreService(object):
    """."""

    @staticmethod
    def execute(config):
        try:
            response = request_to_microservice(config)
            if not response or response.get('errors'):
                raise GeostoreNotFound
            geostore = response.get('data', None).get('attributes', None)
            geojson = geostore.get('geojson', None).get('features', None)[0]

        except NotFound as e:
            exception_message = str(e) if str(e) else 'Could not reach geostore service'
            raise GeostoreNotFound(message=exception_message)
        except Exception as e:
            raise GeostoreNotFound(message=str(e))
        return geojson

    @staticmethod
    def get(geostore):
        config = {
            'uri': '/geostore/' + geostore,
            'method': 'GET'
        }
        return GeostoreService.execute(config)
