"""Geocode SERVICE"""
import logging
import time
from multiprocessing import Pool

import pandas as pd
from flask import request
from geopy.geocoders import GoogleV3

from aqueduct.config import SETTINGS
from aqueduct.errors import GeocodeError

ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])


def pd_read_csv(x):
    return pd.read_csv(x)


def pd_read_excel(x):
    return pd.read_excel(x)


def read_functions(extension):
    dic = {
        'csv': pd_read_csv,
        'xlsx': pd_read_excel
    }

    return dic[extension]


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


geopy = SETTINGS.get('geopy')
g = GoogleV3(api_key=geopy.get('places_api_key'))


def get_latlonraw(x):
    logging.debug(f'[GeoCode Service] get_latlonraw init:')
    index, row = x
    time.sleep(0.001)
    address = g.geocode(row['address'])
    logging.debug(f'[GeoCode Service] get_latlonraw address: {address}')
    try:
        return address.latitude, address.longitude, True
    except:
        return None, None, False


class GeocodeService(object):

    @staticmethod
    def geocoding(data):
        logging.debug(f'[GeoCode Service] Geo-encoding data: {data}')
        try:
            data.columns = map(str.lower, data.columns)
            logging.debug(f'[GeoCode Service] Geo-encoding columns: {data.columns}')
            if 'address' in data.columns:
                logging.debug(f'[GeoCode Service] "address" present in "data.columns":')
                data1 = pd.DataFrame(0.0, index=list(range(0, len(data))), columns=list(['lat', 'lon', 'match']))
                data = pd.concat([data, data1], axis=1)

                p = Pool()
                data[['lat', 'lon', 'match']] = p.map(get_latlonraw, data.iterrows())
                data.fillna('NaN',inplace=True)
            else:
                raise GeocodeError(message='Address column missing')
        except Exception as e:
            raise e
        return data


    @staticmethod
    def upload_file():
        try:
            if request.method == 'POST':
                logging.debug(f'[GeoCode Service] File keys detected: {list(request.files.keys())}')
                if 'file' not in request.files:
                    raise GeocodeError(message='No file provided')
                file = request.files['file']
                extension = file.filename.rsplit('.', 1)[1].lower()
                if file and allowed_file(file.filename):
                    data = read_functions(extension)(request.files.get('file'))
                    data.rename(columns={'Unnamed: 0': 'row'}, inplace=True)
                    if len(data) == 0:
                        raise GeocodeError(message='The file is empty')
                    if len(data) > 1000:
                        raise GeocodeError(message='Row number should be less or equal to 1000')
                else:
                    raise GeocodeError(message=f'{extension} is not an allowed file extension')
        except Exception as e:
            raise e
        logging.debug(f'[GeoCode Service] Data loaded: {data}')
        return GeocodeService.geocoding(data)
