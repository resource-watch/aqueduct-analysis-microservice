"""Geostore SERVICE"""

from RWAPIMicroservicePython import request_to_microservice
from RWAPIMicroservicePython.errors import NotFound

from aqueduct.errors import GeostoreNotFound


class GeostoreService(object):
    """."""

    @staticmethod
    def execute(uri, api_key):
        try:
            response = request_to_microservice(uri=uri, api_key=api_key, method="GET")
            if not response or response.get("errors"):
                raise GeostoreNotFound
            geostore = response.get("data", None).get("attributes", None)
            geojson = geostore.get("geojson", None).get("features", None)[0]

        except NotFound as e:
            exception_message = str(e) if str(e) else "Could not reach geostore service"
            raise GeostoreNotFound(message=exception_message)
        except Exception as e:
            raise GeostoreNotFound(message=str(e))
        return geojson

    @staticmethod
    def get(geostore, api_key):
        uri = f"/v1/geostore/{geostore}"
        return GeostoreService.execute(uri, api_key)
