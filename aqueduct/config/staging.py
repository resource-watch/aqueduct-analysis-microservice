import os

SETTINGS = {
    'logging': {
        'level': os.getenv('LOGGER_LEVEL') or 'INFO'
    }
}
