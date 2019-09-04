"""VALIDATORS"""
import json
import logging
from functools import wraps

from cerberus import Validator
from flask import request

from aqueduct.routes.api import error


def myCoerc(n):
    try:
        return lambda v: None if v in ('null') else n(v)
    except Exception:
        return None


null2int = myCoerc(int)
null2float = myCoerc(float)

to_bool = lambda v: v.lower() in ('true', '1')
to_lower = lambda v: v.lower()
# to_list = lambda v: json.loads(v.lower())
to_list = lambda v: json.loads(v)


def validate_wra_params(func):
    """Water Risk atlas parameters validation"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        validation_schema = {
            'wscheme': {
                'type': 'string',
                'maxlength': 13,
                'schema': {'type': 'integer',
                           'nullable': True,
                           'coerce': null2int,
                           'anyof': [{'min': 0, 'max': 4}]},
                'required': True},
            'geostore': {
                'type': 'string',
                'required': True
            }
        }
        if request.method == 'GET':
            logging.info(f"[VALIDATOR - wra_weights]: {kwargs}")
            validator = Validator(validation_schema, allow_unknown=True)

            if not validator.validate(kwargs):
                return error(status=400, detail=validator.errors)
            kwargs['sanitized_params'] = validator.normalized(kwargs)
        return func(*args, **kwargs)

    return wrapper


def validate_params_cba(func):
    """World Validation"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        validation_schema = {
            'geogunit_unique_name': {'type': 'string', 'required': True},
            'existing_prot': {
                'type': 'integer',
                'required': False,
                'coerce': null2int,
                'default': None,
                'nullable': True,
                'min': 0,
                'max': 1000
            },
            'scenario': {
                'type': 'string',
                'required': True,
                'allowed': ["business as usual", "pessimistic", "optimistic"],
                'coerce': to_lower
            },
            'prot_fut': {
                'type': 'integer',
                'required': False,
                'coerce': null2int,
                'default': None,
                'nullable': True,
                'min': 0,
                'max': 1000
            },
            'implementation_start': {
                'type': 'integer',
                'required': True,
                'coerce': int,
                'min': 2020,
                'max': 2079
            },
            'implementation_end': {
                'type': 'integer',
                'required': True,
                'coerce': int,
                'min': 2021,
                'max': 2080
            },
            'infrastructure_life': {
                'type': 'integer',
                'required': True,
                'coerce': int,
                'min': 1,
                'max': 100
            },
            'benefits_start': {
                'type': 'integer',
                'required': True,
                'coerce': int,
                'min': 2020,
                'max': 2080
            },
            'ref_year': {
                'type': 'integer',
                'required': True,
                'coerce': int,
                'allowed': [2030, 2050, 2080]
            },
            'estimated_costs': {
                'type': 'float',
                'required': False,
                'coerce': null2float,
                'default': None,
                'nullable': True,
                'min': 0,
                'max': 2
            },
            'discount_rate': {
                'type': 'float',
                'required': True,
                'coerce': float,
                'min': 0,
                'max': 1
            },
            'om_costs': {
                'type': 'float',
                'required': True,
                'coerce': float,
                'min': 0,
                'max': 1
            },
            'user_urb_cost': {
                'type': 'float',
                'required': False,
                'coerce': null2float,
                'default': None,
                'nullable': True,
                'min': 0,
                'max': 1000
            },
            'user_rur_cost': {
                'type': 'float',
                'required': False,
                'coerce': null2float,
                'default': None,
                'nullable': True,
                'min': 0,
                'max': 1000
            }
        }
        logging.info(f"[VALIDATOR - cba_params]: {kwargs}")
        validator = Validator(validation_schema, allow_unknown=True)
        if not validator.validate(kwargs['params']):
            return error(status=400, detail=validator.errors)

        kwargs['sanitized_params'] = validator.normalized(kwargs['params'])
        return func(*args, **kwargs)

    return wrapper


def validate_params_cba_def(func):
    """World Validation"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        validation_schema = {
            'geogunit_unique_name': {'type': 'string', 'required': True},
            'scenario': {
                'type': 'string',
                'required': True,
                'allowed': ["business as usual", "pessimistic", "optimistic"],
                'coerce': to_lower
            },
            'flood': {
                'type': 'string',
                'required': False,
                'coerce': to_lower,
                'default': 'riverine',
                'allowed': ["riverine", "coastal"],
            },
            'sub_scenario': {
                'type': 'boolean',
                'required': False,
                'default': False,
                'coerce': (str, to_bool)
            }
        }
        logging.debug(f"[VALIDATOR - cba_def_params]: {kwargs}")
        validator = Validator(validation_schema, allow_unknown=True)
        if not validator.validate(kwargs['params']):
            return error(status=400, detail=validator.errors)

        kwargs['sanitized_params'] = validator.normalized(kwargs['params'])
        logging.debug(f"[VALIDATOR - cba_def_params]: {kwargs['sanitized_params']}")
        return func(*args, **kwargs)

    return wrapper


def validate_params_risk(func):
    """World Validation"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        validation_schema = {
            'geogunit_unique_name': {'type': 'string', 'required': True},
            'existing_prot': {
                'type': 'integer',
                'required': False,
                'coerce': null2int,
                'default': None,
                'nullable': True,
                'min': 0,
                'max': 1000
            },
            'scenario': {
                'type': 'string',
                'required': True,
                'allowed': ["business as usual", "pessimistic", "optimistic"],
                'coerce': to_lower

            },
            'sub_scenario': {
                'type': 'boolean',
                'required': True,
                'coerce': (str, to_bool)
            },
            'exposure': {
                'type': 'string',
                'required': True,
                'coerce': to_lower
            },
            'flood': {
                'type': 'string',
                'required': True,
                'coerce': to_lower
            }

        }

        validator = Validator(validation_schema, allow_unknown=True)
        if not validator.validate(kwargs['params']):
            logging.debug(f"[VALIDATOR - risk_params]: {kwargs}")
            return error(status=400, detail=validator.errors)

        kwargs['sanitized_params'] = validator.normalized(kwargs['params'])

        return func(*args, **kwargs)

    return wrapper
