"""CARTO SQL SERVICE"""
import logging
import requests

from aqueduct.config import SETTINGS
from aqueduct.errors import CartoError

#PAnalysis="Select * from get_aqpoints_annual_custom(\'{weights_scheme}\',\'{coords_array}\')"

class CartoService(object):
    """."""
    @staticmethod
    def query(sql):
        carto = SETTINGS.get('carto')
        url = "https://{serviceAcc}.{uri}".format(serviceAcc=carto.get('service_account'),uri=carto.get('uri'))
        payload = {'q': sql}
        try:
            r = requests.post(url, data=payload)
            data = r.json()
            if not data or len(data.get('rows')) == 0:
                raise CartoError(message='Carto Error')
        except Exception as e:
            raise e
        return data

    @staticmethod
    def get_table(wscheme, points):
        sql=f"SELECT * FROM get_aqpoints_annual_custom({wscheme},'{points}')"
        logging.info(f"[SERVICE] [carto_service] query: {sql}")
        return CartoService.query(sql)