"""VALIDATORS"""
from ast import literal_eval
from functools import wraps
from flask import request

from aqueduct.routes.api import error

def validate_geostore(func):
    """World Validation"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if request.method == 'GET':
            geostore = request.args.get('geostore')
            if not geostore:
                return error(status=400, detail='Geostore is required')
        return func(*args, **kwargs)
    return wrapper

def validate_weights(func):
    """World Validation"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if request.method == 'GET':
            wscheme = literal_eval(request.args.get('wscheme'))
            if not wscheme:
                return error(status=400, detail='wscheme is required')
            elif len(wscheme) != 12:
            	return error(status=400, detail='please a valid weight scheme array is needed: [1,1,1,1,1,1,1,1,1,1,1,1]')
            elif type(wscheme) != list:
                return error(status=400, detail='this is not a valid weight scheme array, required something like: [1,1,1,1,1,1,1,1,1,1,1,1]')
        return func(*args, **kwargs)
    return wrapper

def validate_params_cba(func):
    """World Validation"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if request.method == 'GET':
            userSelections = request.args
            if not userSelections:
                return error(status=400, detail='User Selections are required')
            elif len(request.args) != 12:
                return error(status=400, detail='please a valid wscheme array is needed: [1,1,1,1,1,1,1,1,1,1,1,1]')
        return func(*args, **kwargs)
    return wrapper

def validate_params_risk(func):
    """World Validation"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if request.method == 'GET':
            wscheme = literal_eval(request.args.get('wscheme'))
            if not wscheme:
                return error(status=400, detail='wscheme is required')
            elif len(wscheme) != 12:
                return error(status=400, detail='please a valid wscheme array is needed: [1,1,1,1,1,1,1,1,1,1,1,1]')
        return func(*args, **kwargs)
    return wrapper