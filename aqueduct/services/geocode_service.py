"""Geocode SERVICE"""
from flask import request
import pandas as pd
from geopy.geocoders import GoogleV3, Nominatim
from geopy.extra.rate_limiter import RateLimiter
from multiprocessing import Pool
import time
import logging

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
    index, row = x
    time.sleep(0.001)
    address = g.geocode(row['address'])        
    try:
        return address.latitude, address.longitude, True
    except:
        return None, None, False

class GeocodeService(object):

    @staticmethod
    def geocoding(data):

        try:
            data.columns = map(str.lower, data.columns)
            if 'address' in data.columns:
                data1 = pd.DataFrame(0.0, index=list(range(0,len(data))), columns=list(['lat','lon', 'match']))
                data = pd.concat([data,data1], axis=1)

                p = Pool()
                data[['lat', 'lon', 'match']] = p.map(get_latlonraw, data.iterrows())
            else:
                raise GeocodeError(message='Address column missing')

        except Exception as e:
            raise e
        return data

    @staticmethod
    def upload_file():
        try:
            if request.method == 'POST':
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
        return GeocodeService.geocoding(data)