"""MIDDLEWARE"""
import logging
from functools import wraps
from flask import request

from aqueduct.routes.api import error
from aqueduct.services.geostore_service import GeostoreService
from aqueduct.errors import GeostoreNotFound


def get_geo_by_hash(func):
    """Get geodata"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if request.method == 'GET':
            geostore = request.args.get('geostore')
            logging.info('[middleware]: ' + geostore)
            if not geostore:
                return error(status=400, detail='Geostore is required')
            try:
                geojson = GeostoreService.get(geostore)
            except GeostoreNotFound:
                return error(status=404, detail='Geostore not found')
            kwargs["geojson"] = geojson
        return func(*args, **kwargs)
    return wrapper