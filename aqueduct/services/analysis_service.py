"""Aqueduct point analysis SERVICE (WRAPPER)"""
import logging

import geojson as geoj
import json
from aqueduct.services.carto_service import CartoService
from aqueduct.errors import CartoError



class AnalysisService(object):
    """."""

    @staticmethod
    def analyze(wscheme, geojson):
        """Query GEE using supplied args with threshold and polygon."""

        try:
        	geometry = geoj.loads(geoj.dumps(geojson))

        	points = ['\'\'Point({0} {1})\'\''.format(point[0], point[1]) if geometry["geometry"]["type"] =='MultiPoint' else None for point in geometry["geometry"]["coordinates"]]
        	data = CartoService.get_table(wscheme, points)
        except CartoError as e:
            raise e
        except Exception as e:
            raise e

        return data.get('rows')
