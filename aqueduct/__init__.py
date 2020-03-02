"""The API MODULE"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys

from apispec import APISpec
from apispec_webframeworks.flask import FlaskPlugin
import CTRegisterMicroserviceFlask
from flask import Flask

from aqueduct.config import SETTINGS
from aqueduct.routes.api import error
from aqueduct.routes.api.v1 import aqueduct_analysis_endpoints_v1
from aqueduct.utils.files import load_config_json, write_json

formatter = logging.Formatter('%(asctime)s  - %(funcName)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s',
                              '%Y%m%d-%H:%M%p')

logging.basicConfig(
    level=SETTINGS.get('logging', {}).get('level'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y%m%d-%H:%M%p',
)

root = logging.getLogger()
root.setLevel(logging.DEBUG)

error_handler = logging.StreamHandler(sys.stderr)
error_handler.setLevel(logging.WARN)
error_handler.setFormatter(formatter)
root.addHandler(error_handler)

output_handler = logging.StreamHandler(sys.stdout)
output_handler.setLevel(SETTINGS.get('logging', {}).get('level'))
output_handler.setFormatter(formatter)
root.addHandler(output_handler)

# Flask App
app = Flask(__name__)

# Routing
app.register_blueprint(aqueduct_analysis_endpoints_v1, url_prefix='/api/v1/aqueduct/analysis')


# Generating documantation for the endpoints
# Create spec
spec = APISpec(
    openapi_version="3.0.2",
    swagger='1.0',
    title='Aqueduct Analisis API',
    host='staging-api.globalforestwatch.org',
    version='1.0.0',
    info=dict(
        title='Python Skeleton',
        description='Python Skeleton',
        version='1.0.0'
    ),
    schemes = [
        "https",
        "http"]
    ,
    basePath="/api/v1",
    produces=["application/vnd.api+json"],
    plugins=[
        FlaskPlugin()
    ]
)
myRoutes = [v  for k, v  in app.view_functions.items() if '.' in k]
with app.test_request_context():
    for route in myRoutes:
        spec.path(view=route)
# 
# We're good to go! Save this to a file for now.
write_json(spec.to_dict(), 'public-swagger')
# CT
# if micro exited with code 1 it means it couldn't register against CT check if the etc/hosts mymachine ip or the ip on .env (linux users) match with the machine ip
info = load_config_json('register')
swagger = load_config_json('swagger')
logging.info('swagger')

CTRegisterMicroserviceFlask.register(
    app=app,
    name='aqueduct',
    info=info,
    swagger=swagger,
    mode=CTRegisterMicroserviceFlask.AUTOREGISTER_MODE if os.getenv('CT_REGISTER_MODE') and os.getenv(
        'CT_REGISTER_MODE') == 'auto' else CTRegisterMicroserviceFlask.NORMAL_MODE,
    ct_url=os.getenv('CT_URL'),
    url=os.getenv('LOCAL_URL')
)


@app.errorhandler(403)
def forbidden(e):
    return error(status=403, detail='Forbidden')


@app.errorhandler(404)
def page_not_found(e):
    return error(status=404, detail='Not Found')


@app.errorhandler(405)
def method_not_allowed(e):
    return error(status=405, detail='Method Not Allowed')


@app.errorhandler(410)
def gone(e):
    return error(status=410, detail='Gone')


@app.errorhandler(500)
def internal_server_error(e):
    return error(status=500, detail='Internal Server Error')
