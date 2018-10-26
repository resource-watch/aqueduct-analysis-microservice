"""API ROUTER"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from ast import literal_eval
import pandas as pd
from flask import jsonify, request, Blueprint, json
from aqueduct.routes.api import error
from aqueduct.services.analysis_service import AnalysisService
from aqueduct.services.cba_service import CBAEndService, CBAICache
from aqueduct.services.cba_defaults_service import CBADefaultService
from aqueduct.services.risk_service import RiskService
from aqueduct.validators import validate_geostore, validate_weights, validate_params_cba, validate_params_cba_def, validate_params_risk
from aqueduct.serializers import serialize_response, serialize_response_cba ,serialize_response_default, serialize_response_risk
from aqueduct.middleware import get_geo_by_hash
from aqueduct.errors import CartoError, DBError

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

@aqueduct_analysis_endpoints_v1.route('/cba', strict_slashes=False, methods=['GET'])
@validate_params_cba
def precalc_cba():
    logging.info('[ROUTER]: Getting cba default')

    try:
        USER_INPUTS = {
    "geogunit_unique_name" : request.args.get("geogunit_unique_name"),
    "existing_prot" : None if request.args.get("existing_prot") == 'null' else round(float(request.args.get("existing_prot"))) ,
    "scenario" : request.args.get("scenario"),
    "prot_fut" : int(request.args.get("prot_fut")),
    "implementation_start" : int(request.args.get("implementation_start")),
    "implementation_end" : int(request.args.get("implementation_end")),
    "infrastructure_life" : int(request.args.get("infrastructure_life")),
    "benefits_start" :int(request.args.get("benefits_start")),
    "ref_year" : int(request.args.get("ref_year")),
    "estimated_costs" : None if request.args.get("estimated_costs") == 'null' else float(request.args.get("estimated_costs")) ,
    "discount_rate" : float(request.args.get("discount_rate")),
    "om_costs" : float(request.args.get("om_costs")),
    "user_urb_cost" :  None if request.args.get("user_urb_cost") == 'null' else float(request.args.get("user_urb_cost")) ,
    "user_rur_cost" : None
    }
        output = CBAICache(USER_INPUTS).execute()
    except DBError as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)

    return jsonify({'status':'saved'}), 200

@aqueduct_analysis_endpoints_v1.route('/cba/widget/<widget_id>', strict_slashes=False, methods=['GET'])
@validate_params_cba
def get_cba_widget(widget_id):
    """By Geostore Endpoint"""
    logging.info('[ROUTER]: Getting cba widget ' + widget_id)
    try:
        USER_INPUTS = {
    "geogunit_unique_name" : request.args.get("geogunit_unique_name"),
    "existing_prot" : None if request.args.get("existing_prot") == 'null' else round(float(request.args.get("existing_prot"))),
    "scenario" : request.args.get("scenario"),
    "prot_fut" : int(request.args.get("prot_fut")),
    "implementation_start" : int(request.args.get("implementation_start")),
    "implementation_end" : int(request.args.get("implementation_end")),
    "infrastructure_life" : int(request.args.get("infrastructure_life")),
    "benefits_start" :int(request.args.get("benefits_start")),
    "ref_year" : int(request.args.get("ref_year")),
    "estimated_costs" : None if request.args.get("estimated_costs") == 'null' else float(request.args.get("estimated_costs")) ,
    "discount_rate" : float(request.args.get("discount_rate")),
    "om_costs" : float(request.args.get("om_costs")),
    "user_urb_cost" :  None if request.args.get("user_urb_cost") == 'null' else float(request.args.get("user_urb_cost")) ,
    "user_rur_cost" : None
    }
        output = CBAEndService(USER_INPUTS)

    except DBError as e:
        logging.error('[ROUTER]: '+e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)
    
    if 'format' in request.args and request.args.get("format")=='json':
        return jsonify(serialize_response_cba(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200, {'Content-Disposition': 'attachment', 'filename': '{0}.json'.format(widget_id)}
    elif 'format' in request.args and request.args.get("format")=='csv':
        return pd.DataFrame(output.get_widget(widget_id)['data']).to_csv() , 200, {'Content-Type':'text/csv', 'Content-Disposition': 'attachment', 'filename': '{0}.csv'.format(widget_id)}
    else:
        return jsonify(serialize_response_cba(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200

@aqueduct_analysis_endpoints_v1.route('/cba/default', strict_slashes=False, methods=['GET'])
@validate_params_cba_def
def get_cba_default():
    logging.info('[ROUTER]: Getting cba default')
    try:
        USER_INPUTS = request.args
        output = CBADefaultService(USER_INPUTS)
    except DBError as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)

    return jsonify(serialize_response_default(output.default())), 200

@aqueduct_analysis_endpoints_v1.route('/risk/widget/<widget_id>', strict_slashes=False, methods=['GET'])
@validate_params_risk
def get_risk_widget(widget_id):
    logging.info('[ROUTER]: Getting risk widget ' + widget_id)
    try:
        USER_INPUTS = {
                "geogunit_unique_name" : request.args.get("geogunit_unique_name"),
                "existing_prot" : None if request.args.get("existing_prot") == 'null' else int(request.args.get("existing_prot")),
                "scenario" : request.args.get("scenario").lower(),
                "sub_scenario" : True if request.args.get("sub_scenario") == 'true' else False,
                "exposure" : request.args.get("exposure").lower(),
                "flood" : request.args.get("flood").lower()
            }

        output = RiskService(USER_INPUTS)

    except DBError as e:
        logging.error('[ROUTER]: '+e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)
    if 'format' in request.args and request.args.get("format")=='json':
        return jsonify(serialize_response_risk(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200, {'Content-Disposition': 'attachment', 'filename': '{0}.json'.format(widget_id)}
    elif 'format' in request.args and request.args.get("format")=='csv':
        return pd.DataFrame(output.get_widget(widget_id)['data']).to_csv() , 200, {'Content-Type':'text/csv', 'Content-Disposition': 'attachment', 'filename': '{0}.csv'.format(widget_id)}
    else:
        return jsonify(serialize_response_risk(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200




