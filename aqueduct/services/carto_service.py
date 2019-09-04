"""CARTO SQL SERVICE"""
import logging
import requests

from aqueduct.config import SETTINGS
from aqueduct.errors import CartoError

PAnalysis="Select * from get_aqpoints(\'{weights_scheme}\',\'{coords_array}\')"


class CartoService(object):
    """."""
    @staticmethod
    def query(sql):
        carto = SETTINGS.get('carto')
        url = "https://{serviceAcc}.{uri}".format(serviceAcc=carto.get('service_account'),uri=carto.get('uri'))
        payload = {'q': sql}
        logging.info("[SERVICE] [carto_service]: carto url: {0}".format(url))
        try:
            r = requests.post(url, data=payload)
            data = r.json()
            if not data or len(data.get('rows')) == 0:
                raise CartoError(message='Carto Error')
        except Exception as e:
            raise e
        return data

    @staticmethod
    def get_table(wscheme,points):
        sql = PAnalysis.format(weights_scheme=str(wscheme), coords_array='[{0}]'.format(', '.join(points)))
        logging.info("[SERVICE] [carto_service]: query to be performed: {0}".format(sql))
        return CartoService.query(sql)