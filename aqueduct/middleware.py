"""MIDDLEWARE"""
import logging
from functools import wraps
from flask import request, jsonify, json

from aqueduct.routes.api import error
from aqueduct.services.geostore_service import GeostoreService
from aqueduct.errors import GeostoreNotFound


def remove_keys(keys, dictionary):
    for key in keys:
        try:
            del dictionary[key]
        except KeyError:
            pass
    return dictionary

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


def get_wscheme(func):
    """Get weight schema (wscheme) where applicable or return None"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if request.method == 'GET':
            wscheme = request.args.get('wscheme', None)
            kwargs["wscheme"] = wscheme
        return func(*args, **kwargs)
    return wrapper    
