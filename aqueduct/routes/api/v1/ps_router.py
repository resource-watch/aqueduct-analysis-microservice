"""API ROUTER"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import geojson as geoj
import pandas as pd
import re
from flask import jsonify, request, Blueprint, json

from aqueduct.errors import CartoError, DBError, CacheError
from aqueduct.middleware import get_geo_by_hash, sanitize_parameters
from aqueduct.routes.api import error
from aqueduct.serializers import serialize_response, serialize_response_geocoding, serialize_response_cba, \
    serialize_response_default, serialize_response_risk
from aqueduct.services.carto_service import CartoService
from aqueduct.services.cba_defaults_service import CBADefaultService
from aqueduct.services.cba_service import CBAEndService, CBAICache

from aqueduct.services.risk_service import RiskService
from aqueduct.validators import validate_params_cba, validate_params_cba_def, validate_params_risk, validate_wra_params

aqueduct_analysis_endpoints_v1 = Blueprint('aqueduct_analysis_endpoints_v1', __name__)

"""
WATER RISK ATLAS ENDPOINTS
"""

@aqueduct_analysis_endpoints_v1.route('/', strict_slashes=False, methods=['GET','POST'])
@sanitize_parameters
@validate_wra_params
@get_geo_by_hash
def analyze(**kwargs):
    """ Analyze water risk data
    ---
    get:
        summary: Allow  water risk atlas analysis. Pasing this params as 'application/json' on a Post 
        description: Get the water risk scores for the selectec params in the locations array 
        parameters:
            - name: wscheme
              in: query
              description: weight scheme as defined in 
              type: string
              required: true
            - name: indicator
              in: query
              description: a valid indicator that you want to analyse. The available list can be found [here]() 
              type: string
              required: true            
            - name: geostore
              in: query
              description: valid geostore
              type: string
              required: true           
            - name: analysis_type
              in: query
              description: Type of analysis to perform. Allowed values `annual`, `monthly`, `projected` or `custom`
              type: string
              required: true          
            - name: month
              in: query
              description: If we have selected `monthly` as *analyssis_type* we will need to specify a month `1..12` from January to December 
              type: integer
              required: false            
            - name: year
              in: query
              description: If we have selected `projected` as *analyssis_type* we will need to specify a year, one of `2030` or `2034`. Other values from *analyssis_type* will consider year as `baseline`
              type: string
              required: false
            - name: change_type
              in: query
              description: If we have selected `projected` as *analyssis_type* we will need to specify one of `change_from_baseline` or `future_value`.
              type: string
              required: false           
            - name: scenario
              in: query
              description: If we have selected `projected` as *analyssis_type* we will need to specify one of `optimistic`, `business_as_usual` or `pessimistic`.
              type: string
              required: false
            - name: locations
              in: query
              description: location list name. The text must be formater like `"[''Location A'',''Loccation B'']"`.
              type: string
              required: false
            - name: input_address
              in: query
              description: location list name as the result of the [geolocation function](). The text must be formater like `"[''Location A'',''Loccation B'']"`.
              type: string
              required: false            
            - name: match_address
              in: query
              description: location list name as the result of the [geolocation function](). The text must be formater like `"[''Location A'',''Loccation B'']"`.
              type: string
              required: false            
            - name: ids
              in: query
              description: Ids list name. The text must be formater like `"[''Location A'',''Loccation B'']"`
              type: string
              required: false
        responses:
            200:
                description: Foo object to be returned.
                schema: FooSchema
            404:
                description: Foo not found.
            500:
                description: Internal server error.
    """
    try:
        geometry = geoj.loads(geoj.dumps(kwargs["sanitized_params"]["geojson"]))
        
        if geometry["geometry"]["type"] != 'MultiPoint':
            return error(status=500, detail=f'Error: geostore must be of multipoint type, not {geometry["geometry"]["type"]}.')

        nPoints = len(geometry["geometry"]["coordinates"])
        
        if nPoints > 500:
            return error(status=500, detail=f'Error: Row number should be less or equal to 500, provided: {nPoints}')

        point_list = [f"\'\'Point({point[0]} {point[1]})\'\'" for point in geometry["geometry"]["coordinates"]]
        
        tmp = ", ".join(point_list)
        
        points = f"[{tmp}]"
        
        logging.info(f'[ROUTER] [ps_router.analyze]: points {points}')


        if kwargs["sanitized_params"]["locations"] == None:
            location_list = [f"null" for i in range(nPoints)]
            tmp = ", ".join(location_list)
            locations = f"[{tmp}]"
        else:
            locations = kwargs["sanitized_params"]["locations"]

        if kwargs["sanitized_params"]["input_address"] == None:
            address_list = [f"null" for i in range(nPoints)]
            tmp = ", ".join(address_list)
            input_address = f"[{tmp}]"
        else:
            input_address = kwargs["sanitized_params"]["input_address"]

        if kwargs["sanitized_params"]["match_address"] == None:
            maddress_list = [f"null" for i in range(nPoints)]
            tmp = ", ".join(maddress_list)
            match_address = f"[{tmp}]"
        else:
            match_address = kwargs["sanitized_params"]["match_address"]
        
        if kwargs["sanitized_params"]["ids"] == None:
            idsList = [str(i) for i in range(nPoints)]
            tmp = ", ".join(idsList)
            ids = f"[{tmp}]"
        else:
            ids = kwargs["sanitized_params"]["ids"]

        myexpr= r"(?!'')((?<=[a-zA-Z0-9+-]|\s|[\,\.\\])'(?=[a-zA-Z0-9+-]|\s|[\,\.\\]))|(\\)|(\/)"
        locations = re.sub(myexpr,"",locations)
        match_address = re.sub(myexpr,"",match_address)
        input_address = re.sub(myexpr,"",input_address)

        data, downloadUrl = CartoService.get_table(points, kwargs["sanitized_params"]["analysis_type"], kwargs["sanitized_params"]["wscheme"], kwargs["sanitized_params"]["month"], kwargs["sanitized_params"]["year"], kwargs["sanitized_params"]["change_type"], kwargs["sanitized_params"]["indicator"], kwargs["sanitized_params"]["scenario"], locations, input_address, match_address, ids)
    
    except CartoError as e:
        logging.error('[ROUTER]: ' + e.message)
        return error(status=500, detail=e.message)
    
    except Exception as e:
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail='Generic Error')

    data['analysis_type'] = kwargs["sanitized_params"]["analysis_type"]
    data['wscheme'] = kwargs["sanitized_params"]["wscheme"]
    data['month'] = kwargs["sanitized_params"]["month"]
    data['year'] = kwargs["sanitized_params"]["year"]
    data['change_type'] = kwargs["sanitized_params"]["change_type"]
    data['indicator'] = kwargs["sanitized_params"]["indicator"]
    data['scenario'] = kwargs["sanitized_params"]["scenario"]
    data['downloadUrl'] = downloadUrl
    return jsonify(serialize_response(data)), 200


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
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: Unknown error: ' + str(e))
        return error(status=500, detail=e.message)

    return jsonify({'status': 'saved'}), 200


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
        logging.error('[ROUTER]: ' + e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail=e.message)

    ## shity code; to redo one day
    if 'format' in request.args and request.args.get("format") == 'json':
        return jsonify(
            serialize_response_cba(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200, {
                   'Content-Disposition': 'attachment', 'filename': '{0}.json'.format(widget_id)}
    elif 'format' in request.args and request.args.get("format") == 'csv':
        return pd.DataFrame(output.get_widget(widget_id)['data']).to_csv(), 200, {'Content-Type': 'text/csv',
                                                                                  'Content-Disposition': 'attachment',
                                                                                  'filename': '{0}.csv'.format(
                                                                                      widget_id)}
    else:
        return jsonify(
            serialize_response_cba(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200


@aqueduct_analysis_endpoints_v1.route('/cba/default', strict_slashes=False, methods=['GET'])
@sanitize_parameters
@validate_params_cba_def
def get_cba_default(**kwargs):
    logging.info('[ROUTER, get_cba_default]: Getting cba default')
    try:
        output = CBADefaultService(kwargs['sanitized_params'])
        logging.debug('[ROUTER, get_cba_default]: output generated')

    except AttributeError as e:
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail=e)
    except Exception as e:
        logging.error('[ROUTER]: ' + str(e))
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
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail=str(e))
    except Exception as e:
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail=str(e))

    ## shity code; to redo one day
    if 'format' in request.args and request.args.get("format") == 'json':
        return jsonify(
            serialize_response_risk(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200, {
                   'Content-Disposition': 'attachment', 'filename': '{0}.json'.format(widget_id)}
    elif 'format' in request.args and request.args.get("format") == 'csv':
        return pd.DataFrame(output.get_widget(widget_id)['data']).to_csv(), 200, {'Content-Type': 'text/csv',
                                                                                  'Content-Disposition': 'attachment',
                                                                                  'filename': '{0}.csv'.format(
                                                                                      widget_id)}
    else:
        return jsonify(
            serialize_response_risk(json.loads(json.dumps(output.get_widget(widget_id), ignore_nan=True)))), 200
