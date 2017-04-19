"""API ROUTER"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from flask import jsonify
from ps.routes.api.v1 import endpoints, error
from ps.validators import validate_greeting
from ps.serializers import serialize_greeting


@endpoints.route('/hello', strict_slashes=False, methods=['GET'])
@validate_greeting
def say_hello():
    """World Endpoint"""
    logging.info('[ROUTER]: Getting world')
    data = {
        'word': 'hello',
        'propertyTwo': 'random',
        'propertyThree': 'value'
    }
    if False:
        return error(status=400, detail='Not valid')
    return jsonify(data=[serialize_greeting(data)]), 200
