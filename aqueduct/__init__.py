"""The API MODULE"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys

import CTRegisterMicroserviceFlask
from flask import Flask

from aqueduct.config import SETTINGS
from aqueduct.routes.api import error
from aqueduct.routes.api.v1 import aqueduct_analysis_endpoints_v1
from aqueduct.utils.files import load_config_json

formatter = logging.Formatter('%(asctime)s  - %(funcName)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s',
                              '%Y%m%d-%H:%M%p')

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

# CT
info = load_config_json('register')
swagger = load_config_json('swagger')
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
