"""Aqueduct point analysis SERVICE (WRAPPER)"""
import logging

import geojson as geoj
import json
from aqueduct.services.carto_service import CartoService
from aqueduct.errors import CartoError



class AnalysisService(object):
    """."""

    @staticmethod
    def analyze(geojson, wscheme):
        """Query GEE using supplied args with threshold and polygon."""
        
        try:
        	t = geoj.loads(geoj.dumps(geojson))
        	points = ['\'\'Point({0} {1})\'\''.format(geometry["geometry"]["coordinates"][0], geometry["geometry"]["coordinates"][1]) if geometry["geometry"]["type"] =='Point' else None for geometry in t.__geo_interface__['features']]
        	data = CartoService.get_table(wscheme,points)
        except CartoError as e:
            raise e
        except Exception as e:
            raise e

        return data.get('rows')
