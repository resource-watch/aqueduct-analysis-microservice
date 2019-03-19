"""CARTO SQL SERVICE"""
import logging
import requests

from aqueduct.config import SETTINGS
from aqueduct.errors import CartoError

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
    def get_table(points, analysis_type, wscheme, month, year, change_type, indicator, scenario):
        sqltype = {'annual': f"SELECT * FROM get_aqpoints_annual('{points}')",
                   'monthly': f"SELECT * FROM get_aqpoints_monthly('{month}', '{points}')",
                   'projected': f"SELECT * FROM get_aqpoints_projected('{year}', '''{change_type}''', '''{indicator}''', '''{scenario}''', '{points}')",
                   'custom': f"SELECT * FROM get_aqpoints_annual_custom({wscheme},'{points}')"
                  }
 
        sql = sqltype[analysis_type]
        logging.info(f"[SERVICE] [carto_service] query: {sql}")
        return CartoService.query(sql)