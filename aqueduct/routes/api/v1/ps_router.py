"""API ROUTER"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from ast import literal_eval

from flask import jsonify, request, Blueprint
from aqueduct.routes.api import error
from aqueduct.services.analysis_service import AnalysisService
from aqueduct.validators import validate_geostore, validate_weights
from aqueduct.serializers import serialize_response
from aqueduct.middleware import get_geo_by_hash
from aqueduct.errors import CartoError

aqueduct_analysis_endpoints_v1 = Blueprint('aqueduct_analysis_endpoints_v1', __name__)

def analyze(geojson):
    """Analyze water risk"""
    geojson = geojson or None
    wscheme = literal_eval(request.args.get('wscheme')) or [1] * 12

    if not geojson:
        return error(status=400, detail='Geojson is required')

    try:
        data = {
        'rows': AnalysisService.analyze(
            geojson=geojson,
            wscheme=wscheme
            )}
        logging.info('[ROUTER]: Carto query load', str(data['rows']))
    except CartoError as e:
        logging.error('[ROUTER]: '+e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail='Generic Error')

    data['wscheme'] = wscheme
    return jsonify(serialize_response(data)), 200


@aqueduct_analysis_endpoints_v1.route('/', strict_slashes=False, methods=['GET'])
@validate_geostore
@validate_weights
@get_geo_by_hash
def get_by_geostore(geojson):
    """By Geostore Endpoint"""
    logging.info('[ROUTER]: Getting water risk analysis by geostore')
    return analyze(geojson)
