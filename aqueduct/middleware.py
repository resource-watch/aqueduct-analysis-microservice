"""MIDDLEWARE"""
import logging
from functools import wraps

from flask import request

from aqueduct.errors import GeostoreNotFound
from aqueduct.routes.api import error
from aqueduct.services.geostore_service import GeostoreService


def remove_keys(keys, dictionary):
    """Get geodata"""
    for key in keys:
        try:
            del dictionary[key]
        except KeyError:
            pass
    return dictionary

def is_microservice_or_admin(func):
    """Check if auth is admin"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.debug("[MIDDLEWARE ]: Checking microservice user")
        logged_user = request.json.get("loggedUser", None)
        if (logged_user.get("id") == "microservice") or (logged_user.get("role") == "ADMIN"):
            logging.debug("is microservice or admin")
            return func(*args, **kwargs)
        else:
            return error(status=401, detail="Unauthorized")

    return wrapper

def sanitize_parameters(func):
    """Sets any queryparams in the kwargs"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logging.info(f'[middleware] [sanitizer] args: {args}')
            myargs = dict(request.args)
            # Exclude params like loggedUser here
            sanitized_args = remove_keys(['loggedUser'], myargs)
            kwargs['params'] = sanitized_args
        except GeostoreNotFound:
            return error(status=404, detail='body params not found')

        return func(*args, **kwargs)

    return wrapper


def get_geo_by_hash(func):
    """Get geodata"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        geostore = kwargs["sanitized_params"]["geostore"]
        logging.info(f'[middleware]: {geostore}')
        try:
            geojson = GeostoreService.get(geostore, request.headers.get("x-api-key"))
            kwargs["sanitized_params"]["geojson"] = geojson
        except GeostoreNotFound as e:
            return error(status=404, detail='Geostore not found: {}'.format(e.message))
        
        return func(*args, **kwargs)

    return wrapper

