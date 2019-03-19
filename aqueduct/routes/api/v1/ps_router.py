"""API ROUTER"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from ast import literal_eval
import pandas as pd
import geojson as geoj
from flask import jsonify, request, Blueprint, json
from aqueduct.routes.api import error
from aqueduct.services.analysis_service import AnalysisService
from aqueduct.services.carto_service import CartoService
from aqueduct.services.cba_service import CBAEndService, CBAICache
from aqueduct.services.cba_defaults_service import CBADefaultService
from aqueduct.services.risk_service import RiskService
from aqueduct.validators import validate_wra_params, validate_params_cba, validate_params_cba_def, validate_params_risk
from aqueduct.serializers import serialize_response, serialize_response_cba ,serialize_response_default, serialize_response_risk
from aqueduct.middleware import get_geo_by_hash, sanitize_parameters, get_wra_params
from aqueduct.errors import CartoError, DBError

aqueduct_analysis_endpoints_v1 = Blueprint('aqueduct_analysis_endpoints_v1', __name__)

"""
WATER RISK ATLAS ENDPOINTS
"""
def analyze(geojson, analysis_type, wscheme, month, year, change_type, indicator, scenario):
    """Analyze water risk"""
    try:
        geometry = geoj.loads(geoj.dumps(geojson))
        if geometry["geometry"]["type"] != 'MultiPoint':
            return error(status=500, detail=f'Error: geostore must be of multipoint type, not {geometry["geometry"]["type"]}.')
        point_list = [f"\'\'Point({point[0]} {point[1]})\'\'" for point in geometry["geometry"]["coordinates"]]
        tmp = ", ".join(point_list)
        points = f"[{tmp}]"
        logging.info(f'[ROUTER] [ps_router.analyze]: points {points}')
        data = CartoService.get_table(points, analysis_type, wscheme, month, year, change_type, indicator, scenario)
    except CartoError as e:
        logging.error('[ROUTER]: '+e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail='Generic Error')
    data['analysis_type'] = analysis_type
    data['wscheme'] = wscheme
    data['month'] = month
    data['year'] = year
    data['change_type'] = change_type
    data['indicator'] = indicator
    data['scenario'] = scenario
    return jsonify(serialize_response(data)), 200


@aqueduct_analysis_endpoints_v1.route('/', strict_slashes=False, methods=['GET'])
@get_wra_params
@get_geo_by_hash
def get_by_geostore(geojson, analysis_type, wscheme, month, year, change_type, indicator, scenario):
    """By Geostore Endpoint"""
    logging.info(f'[ROUTER] [get_by_geostore]: Getting water risk analysis by geostore {wscheme} \n {geojson} \n {analysis_type}')
    return analyze(geojson, analysis_type, wscheme, month, year, change_type, indicator, scenario)

"""
FLOOD ENDPOINTS
"""
@aqueduct_analysis_endpoints_v1.route('/cba', strict_slashes=False, methods=['GET'])
@sanitize_parameters
@validate_params_cba
def precalc_cba(**kwargs):
    """precache cba middle table if needed endpoint"""
    logging.info('[ROUTER]: Getting cba default')

    try:
        output = CBAICache(kwargs['sanitized_params']).execute()
    except DBError as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)

    return jsonify({'status':'saved'}), 200

@aqueduct_analysis_endpoints_v1.route('/cba/widget/<widget_id>', strict_slashes=False, methods=['GET'])
@sanitize_parameters
@validate_params_cba
def get_cba_widget(widget_id, **kwargs):
    """Gets a CBA widget
    widget_id: [export, flood_prot, mainteinance, impl_cost, net_benefits, annual_costs, table]
    """
    logging.info(f'[ROUTER]: Getting cba widget: {widget_id}')
    try:
        output = CBAEndService(kwargs['sanitized_params'])

    except DBError as e:
        logging.error('[ROUTER]: '+e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)
    
    ## shity code; to redo one day
    if 'format' in request.args and request.args.get("format")=='json':
        return jsonify(serialize_response_cba(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200, {'Content-Disposition': 'attachment', 'filename': '{0}.json'.format(widget_id)}
    elif 'format' in request.args and request.args.get("format")=='csv':
        return pd.DataFrame(output.get_widget(widget_id)['data']).to_csv() , 200, {'Content-Type':'text/csv', 'Content-Disposition': 'attachment', 'filename': '{0}.csv'.format(widget_id)}
    else:
        return jsonify(serialize_response_cba(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200

@aqueduct_analysis_endpoints_v1.route('/cba/default', strict_slashes=False, methods=['GET'])
@sanitize_parameters
@validate_params_cba_def
def get_cba_default(**kwargs):
    logging.info('[ROUTER]: Getting cba default')
    try:
        output = CBADefaultService(kwargs['sanitized_params'])

    except AttributeError as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=e.message)

    return jsonify(serialize_response_default(output.execute())), 200

@aqueduct_analysis_endpoints_v1.route('/risk/widget/<widget_id>', strict_slashes=False, methods=['GET'])
@sanitize_parameters
@validate_params_risk
def get_risk_widget(widget_id, **kwargs):
    logging.info('[ROUTER]: Getting risk widget ' + widget_id)
    try:
        output = RiskService(kwargs['sanitized_params'])

    except AttributeError as e:
        logging.error('[ROUTER]: '+ str(e))
        return error(status=500, detail=str(e))
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail=str(e))

    ## shity code; to redo one day
    if 'format' in request.args and request.args.get("format")=='json':
        return jsonify(serialize_response_risk(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200, {'Content-Disposition': 'attachment', 'filename': '{0}.json'.format(widget_id)}
    elif 'format' in request.args and request.args.get("format")=='csv':
        return pd.DataFrame(output.get_widget(widget_id)['data']).to_csv() , 200, {'Content-Type':'text/csv', 'Content-Disposition': 'attachment', 'filename': '{0}.csv'.format(widget_id)}
    else:
        return jsonify(serialize_response_risk(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200




