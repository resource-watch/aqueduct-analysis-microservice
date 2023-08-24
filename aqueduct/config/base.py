"""-"""

import os

SETTINGS = {
    'logging': {
        'level': os.getenv('LOGGER_LEVEL') or 'DEBUG'
    },
    'service': {
        'name': 'Aqueduct Analysis Microservice',
        'port': os.getenv('PORT')
    },
    'carto': {
        'service_account': os.getenv('CARTODB_USER'),
        'uri': 'carto.com/api/v2/sql'
    },
    'geopy': {
        'places_api_key': os.getenv('AQUEDUCT_GOOGLE_PLACES_PRIVATE_KEY')
    }
}
