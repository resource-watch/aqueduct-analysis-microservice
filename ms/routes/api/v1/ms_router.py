import os
import json
import csv
import StringIO
import logging

from flask import jsonify, request, Response, stream_with_context
import requests

from . import endpoints
from ms.responders import ErrorResponder
from ms.utils.http import request_to_microservice

@endpoints.route('/hello', methods=['GET'])
def say_hello():
    """Query GEE Dataset Endpoint"""
    logging.info('Doing GEE Query')
    return jsonify({'data': 'hello'}), 200
