"""CARTO SQL SERVICE"""
import logging

import requests

from aqueduct.config import SETTINGS
from aqueduct.errors import CartoError


class CartoService(object):
    """."""

    @staticmethod
    def query(sql, sql_download):
        carto = SETTINGS.get('carto')
        url = "https://wri-rw.{uri}".format(serviceAcc=carto.get('service_account'), uri=carto.get('uri'))
        payload = {'q': sql}
        downloadUrl = url + '?q=' + sql_download

        try:
            r = requests.post(url, data=payload)
            data = r.json()
            if r.status_code != 200:
              raise CartoError(message=r.text)
            if not data or len(data.get('rows')) == 0:
                raise CartoError(message='Carto Error')
        except Exception as e:
            raise e
        return data, downloadUrl

    @staticmethod
    def get_table(points, analysis_type, wscheme, month, year, change_type, indicator, scenario, 
                    locations, input_address, match_address, ids):
        sqltype = {'annual': f"SELECT * FROM get_aqpoints_annual_04('{ids}','{points}', '{locations}', '{input_address}', '{match_address}')",
                   'monthly': f"SELECT * FROM get_aqpoints_monthly_02('{month}', '{points}', '{locations}', '{input_address}', '{match_address}')",
                   'projected': f"SELECT * FROM get_aqpoints_projected_02('{year}', '''{change_type}''', '''{indicator}''', '''{scenario}''', '{points}', '{locations}', '{input_address}', '{match_address}')",
                   'custom': f"SELECT * FROM get_aqpoints_annual_custom_test('{ids}',{wscheme}, '{points}', '{locations}', '{input_address}', '{match_address}')"
                   }

        sqltype_download = {'annual': f"SELECT * FROM get_aqpoints_annual_04('{points}', '{locations}', '{input_address}', '{match_address}')",
                   'monthly': f"SELECT * FROM get_aqpoints_monthly_all('{points}', '{locations}', '{input_address}', '{match_address}')",
                   'projected': f"SELECT * FROM get_aqpoints_projected_all('{points}', '{locations}', '{input_address}', '{match_address}')",
                   'custom': f"SELECT * FROM get_aqpoints_annual_custom_test('{ids}',{wscheme}, '{points}', '{locations}', '{input_address}', '{match_address}')"
                   }

        sql = sqltype[analysis_type]
        sql_download = sqltype_download[analysis_type]
        logging.info(f"[SERVICE] [carto_service] query: {sql}")
        return CartoService.query(sql, sql_download)
