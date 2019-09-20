"""Geocode SERVICE"""
import logging
import time
import asyncio
import aiohttp
from multiprocessing import Pool, TimeoutError

import pandas as pd
from flask import request
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
#from geopy.geocoders import GoogleV3

from aqueduct.config import SETTINGS
from aqueduct.errors import GeocodeError

ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])
GEOPY = SETTINGS.get('geopy')

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


#g = GoogleV3(api_key=geopy.get('places_api_key'), timeout=30)

def get_google_results(address):
    """
    Get geocode results from Google Maps Geocoding API.
    
    Note, that in the case of multiple google geocode reuslts, this function returns details of the FIRST result.
    
    @param address: String address as accurate as possible. For Example "18 Grafton Street, Dublin, Ireland"
    @param api_key: String API key if present from google. 
                    If supplied, requests will use your allowance from the Google API. If not, you
                    will be limited to the free usage of 2500 requests per day.
    @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                    is useful if you'd like additional location details for storage or parsing later.
    """
    # Set up your Geocoding url
    logging.info("[GEOCODER- GOOGLE URL]: init")
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
    "address":address,
    "key":GEOPY.get('places_api_key')
    }
    
    # Ping google for the reuslts:
    try:
        with requests.Session() as s:
            s.mount('https://',HTTPAdapter(max_retries=Retry(2,backoff_factor=0.001)))
            r = s.get(url=geocode_url, params=params, timeout=15)
        
        if r.status_code == requests.codes.ok:
            # Results will be in JSON format - convert to dict using requests functionality
            results = r.json()
            # if there's no results or an error, return empty results.
            if len(results['results']) == 0:
                output = {
                    "formatted_address" : None,
                    "latitude": None,
                    "longitude": None,
                    "matched": False
                }
            else:    
                answer = results['results'][0]
                output = {
                    "formatted_address" : answer.get('formatted_address'),
                    "latitude": answer.get('geometry').get('location').get('lat'),
                    "longitude": answer.get('geometry').get('location').get('lng'),
                    "matched":True
                }
        else:
            logging.error(f"[GEOCODER: Get google place]: {r.text}")
            logging.error(f"[GEOCODER- GOOGLE URL]: {r.status_code}")
            output = {
                "formatted_address" : None,
                "latitude": None,
                "longitude": None,
                "matched": False
            }
            
        # Append some other details:    
        output['input_string'] = address
        output['number_of_results'] = len(results['results'])
        output['status'] = results.get('status')
        
        return output
    except Exception as e:
        raise e

def get_latlonraw(x):
    index, row = x
    if pd.notna(row['address']) or (row['address'] in ('', ' ')):
        address = get_google_results(row['address'])
        return address["formatted_address"], address["latitude"], address["longitude"], address["matched"]
    else:
        return None, None, None, False

async def fetch_geocode_api(session, url):
    async with session.get(url) as response:
        assert response.status == 200
        return await response.read()

async def run_geocode(urls):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with aiohttp.ClientSession() as session:
        for i in range(len(urls)):
            task = asyncio.ensure_future(fetch_geocode_api(session, url))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # you now have all response bodies in this variable
        print(responses)

class GeocodeService(object):

    @staticmethod
    def geocoding(data):
        try:
            data.columns = map(str.lower, data.columns)
            #logging.debug(f'[GeoCode Service] Geo-encoding columns: {data.columns}')
            if 'address' in data.columns:
                #logging.debug(f'[GeoCode Service] "address" present in "data.columns":')
                # data1 = pd.DataFrame(0.0, index=list(range(0, len(data))), columns=list(['matched address', 'lat', 'lon', 'match']))
                # data = pd.concat([data, data1], axis=1)
                # with Pool(processes=4) as p:
                #     logging.info(f'[GeoCode Service] geocoding init:')
                #     #output = p.map_async(get_latlonraw, data.iterrows())
                #     #output.wait()
                #     data[['matched address', 'lat', 'lon', 'match']] = p.map(get_latlonraw, data.iterrows())
                #     data.fillna(None, inplace=True)

               
                urls = ["http://httpbin.org/get/{}", "http://httpbin.org/get/{}", "http://httpbin.org/get/{}"]
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(run_geocode(urls))
                loop.run_until_complete(future)
            else:
                raise GeocodeError(message='Address column missing')
        except Exception as e:
            pass
        return data


    @staticmethod
    def upload_file():
        try:
            if request.method == 'POST':
                logging.info(f'[GeoCode Service]: File keys detected: {list(request.files.keys())}')
                if 'file' not in request.files:
                    raise GeocodeError(message='No file provided')
                file = request.files['file']
                extension = file.filename.rsplit('.', 1)[1].lower()
                if file and allowed_file(file.filename):
                    data = read_functions(extension)(request.files.get('file'))
                    logging.info(f'[GeoCode Service] Data loaded: {data.columns}')
                    data.rename(columns={'Unnamed: 0': 'row'}, inplace=True)
                    data.dropna(axis=1, how='all', inplace=True)
                    logging.info(f'[GeoCode Service] Data loaded; columns cleaned: {data.columns}')
                    if not {'row', 'Row'}.issubset(data.columns):
                        #logging.debug(f'[GeoCode Service] Columns: {list(data.columns)}')
                        data.insert(loc=0, column='row', value=range(1, 1 + len(data)))
                        #logging.debug(f'[GeoCode Service] Columns: {list(data.columns)}')
                    if len(data) == 0:
                        raise GeocodeError(message='The file is empty')
                    if len(data) > 1000:
                        raise GeocodeError(message='Row number should be less or equal to 1000')
                else:
                    raise GeocodeError(message=f'{extension} is not an allowed file extension')
        except Exception as e:
            pass
            #raise e

        #logging.debug(f'[GeoCode Service] Data loaded: {data}')
        return GeocodeService.geocoding(data)
