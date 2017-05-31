"""Geostore SERVICE"""

from aqueduct.errors import GeostoreNotFound
from CTRegisterMicroserviceFlask import request_to_microservice


class GeostoreService(object):
    """."""

    @staticmethod
    def execute(config):
        try:
            response = request_to_microservice(config)
            if not response or response.get('errors'):
                raise GeostoreNotFound
            geostore = response.get('data', None).get('attributes', None)
            geojson = geostore.get('geojson', None)
        except Exception as e:
            raise GeostoreNotFound(message=str(e))
        return geojson

    @staticmethod
    def get(geostore):
        config = {
            'uri': '/v1/geostore/'+ geostore,
            'method': 'GET'
        }
        return GeostoreService.execute(config)