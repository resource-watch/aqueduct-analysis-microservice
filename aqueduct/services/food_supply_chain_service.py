#!/usr/local/bin/python
"""
Name: AqFood_Supply_Chain_Analyzer.py
Project: Aqueduct Food Tool Enhancements
Description:
  This script reads in a user's supply chain (as coordinates, states, or
  country names) and returns a list of the watersheds or aquifers the user
  operates in.  The script then priorities those watersheds based on the
  user-defined desired condition
Created: October 2021
Author: Samantha Kuzma (samantha.kuzma@wri.org). Adapted by Todd Blackman (tblackman@greenriver.org)

Test With: curl -v -F 'data=@./aqueduct/services/supply_chain_data/template_supply_chain_v20210701_example2.xlsx'  http://localhost:5100/api/v1/aqueduct/analysis/food-supply-chain/bwd/0.25 > output.json
"""

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from fuzzywuzzy import process
import warnings
import ast
from itertools import chain
import time
import logging
from os.path import exists
import os
import redis
import json
import hashlib
from urllib.parse import urlparse
import boto3

warnings.filterwarnings('ignore')


class FoodSupplyChainService(object):

    # -----------------------
    # BACKGROUND DICTIONARIES
    # -----------------------
    # Fix crops names
    crop_fixes = {
        'corn': 'maiz',
        'canola': 'rape',
        'canola oil': 'rape',
        'flaxseed': 'ofib',
        'oats': 'ocer',
        'rye': 'ocer',
        'palm': 'oilp',
        'sorghum grain': 'sorg',
        'soy': 'soyb',
        'soya': 'soyb',
        'soyabean': 'soyb',
        'soybean meal': 'soyb',
        'soyabeans': 'soyb',
        'soybeans': 'soyb',
        'sugar cane': 'sugc',
        'tapioca': 'cass'
        }

    # Fix country names
    country_fixes = {
        "Ivory Coast": "Côte d'Ivoire",
        "US": "United States",
        "USA": "United States",
        "UK": "United Kingdom"
        }

    # Indicator codes
    # indicator_dict = {
    #         'Baseline Water Stress': 'bws_raw'
    #         'Baseline Water Depletion': 'bwd_raw'
    #         'Coastal Eutrophication Potential': 'cep_raw'
    #         'Access to Drinking Water': 'udw_raw'
    #         'Access to Sanitation': 'usa_raw'
    #         'Groundwater Table Decline': 'gtd_raw'
    #         }

    # payload is on the order of 100MB in the most useful format for
    # consumption by the front-end, so abbreviating keys
    output_lookup = {
      'Annual Spend': 'as',
      '* % Change Required': 'pcr',
      '* Desired Condition': 'dc',
      '* Raw Value': 'rv',
      '* Score': 's',
      'Country': 'cy',
      'Crop_Name': 'cn',
      'Error': 'e',
      'IFPRI_production_MT': 'ip',
      'Latitude': 'lat',
      'Location ID': 'lid',
      'Longitude': 'lng',
      'Material Type': 'mt',
      'Material Volume (MT)': 'mv',
      'Radius': 'ra',
      'Radius Unit': 'ru',
      'row': 'rn',
      'State/Province': 'st',
      'Watershed ID': 'wid',
      'Aquifer ID': 'aid'
      }

    def prepare_payload(self, payload):
        new_payload = {}

        for key in payload:
            new_key = self.output_lookup.get(key)
            value = payload[key]
            if pd.isna(value):
                value = None
            if new_key is None:
                if key.endswith("% Change Required"):
                    new_key = 'pcr'
                elif key.endswith("Desired Condition"):
                    new_key = 'dc'
                elif key.endswith("Raw Value"):
                    new_key = 'rv'
                elif key.endswith("Score"):
                    new_key = 's'
                else:
                    new_key = key
                new_payload[new_key] = value
            else:
                new_payload[new_key] = value
        return new_payload

    def __init__(self, user_input=None, user_indicator=None, user_threshold=None, job_token=None):
        self.analysis_time = time.time()
        # Inputs from User
        self.user_input = user_input  # Uploaded file
        self.user_indicator = user_indicator  # Indicator Selection (from blue panel on tool)
        self.user_threshold = user_threshold  # Desired State thresholds (from blue panel on tool)
        self.job_token = job_token

        # -----------
        # INPUT FILES
        # ----------
        self.croplist_path = 'aqueduct/services/supply_chain_data/inputs_ifpri_croplist.csv'  # Full crop names and SPAM_code ID
        self.adm_path = 'aqueduct/services/supply_chain_data/inputs_admin_names.csv'  # GADM v3.6 Administative names for levels 1 & 0

        # Aqueduct geospatial data. Replace with Carto
        self.aq_path = 'aqueduct/services/supply_chain_data/inputs_aqueduct30.csv'

        # INDICATOR SPECIFIC GEOMETRY (WATERSHEDS OR AQUIFERS)
        self.hybas_path = "aqueduct/services/supply_chain_data/Aqueduct30_{}.shp".format

        self.bucket = os.environ.get('S3_BUCKET_NAME')

        redis_url = os.environ.get('REDIS_URL')

        if redis_url:
            uri = urlparse(redis_url)
            redis_host = uri.hostname
            redis_port = uri.port
            logging.info("host:  {}".format(redis_host))
            if not redis_port:
                redis_port = 6379
            self.redis = redis.Redis(host=redis_host, port=int(redis_port), db=3)
        else:
            raise Exception("You must specify a redis url in the environment")

    def enqueue(self):
        content = open(self.user_input, mode='rb').read() # ', encoding='ascii-8bit').read()
        md5sum = hashlib.md5(content).hexdigest()
        self.job_token = '-'.join([md5sum, self.user_indicator, str(self.user_threshold)])
        logging.info("redis key is {}".format(self.job_token))

        self.redis.hmset(self.job_token, {"user_indicator": self.user_indicator, "user_threshold": self.user_threshold, "content": content, "status": "enqueued", "percent_complete": 5, "results": json.dumps({})})
        self.redis.expire(self.job_token, 60*60)
        self.redis.rpush("job_queue", self.job_token)
        # https://github.com/redis/redis-py
        # https://redis.io/commands/hmset
        return self.job_token

    def done(self):
        return self.ready() or self.failed()

    def ready(self):
        return self.current_status() == "ready"

    def failed(self):
        return self.current_status() == "failed"

    def current_status(self):
        return self.redis.hget(self.job_token, "status").decode('utf-8')

    def results(self):
        payload = {}
        payload['job_token'] = self.job_token
        payload['user_indicator'] = self.redis.hget(self.job_token, "user_indicator").decode('utf-8')
        payload['user_threshold'] = float(self.redis.hget(self.job_token, "user_threshold"))
        payload['queue_length'] = int(self.redis.llen("job_queue"))

        results = self.redis.hget(self.job_token, "results")

        if results:
            payload['results'] = json.loads(results)
            payload['status'] = self.current_status()
            # payload['base64_encoded_results'] = False
            # payload['gzip_compressed_results'] = False

            # if len(payload['results']) > 9500000:
            #     payload['base64_encoded_results'] = True
            #     payload['gzip_compressed_results'] = True
            #     compressed = bz2.BZ2Compressor().compress(payload['results'])
            #     encoded = base64.b64encode(compressed)
            #     payload['results'] = encoded

            payload['percent_complete'] = int(self.redis.hget(self.job_token, "percent_complete"))
        else:
            payload['results'] = {}
            payload['status'] = "invalid-job-token"
            payload['percent_complete'] = 0

        return payload

    def set_percent_complete(self, pct):
        self.redis.hset(self.job_token, "percent_complete", pct)

    def pop_and_do_work():
        service = FoodSupplyChainService()
        service.job_token = service.redis.rpop("job_queue")
        if service.job_token:
            logging.info("Working on job {}".format(service.job_token))
            service.run()

    def run(self):
        try:
            # b = os.path.getsize(self.user_input)
            # f = open(self.user_input, 'rb')
            # first_bytes = f.read(50)
            # message = "Excel file {} is {} bytes. First bytes are {}".format(self.user_input, b, first_bytes)
            # raise Exception(message)

            self.user_indicator = self.redis.hget(self.job_token, "user_indicator").decode('utf-8')
            self.user_threshold = float(self.redis.hget(self.job_token, "user_threshold"))

            content = self.redis.hget(self.job_token, "content")
            # serialized. Only one at a time.
            fout = open("data.xlsx", 'wb')
            fout.write(content)
            fout.close()

            self.redis.hset(self.job_token, "status", "running")
            self.set_percent_complete(6)

            df = pd.read_excel("data.xlsx", header=4, index_col=None)
            self.set_percent_complete(10)

            # Create a row index that matches excel files
            df['row'] = range(6, len(df)+6)
            df.set_index('row', inplace=True)

            # READ IN CARTO INPUTS
            # Placeholder to read in CARTO AQUEDUCT DATABASE. This will need to update
            # TDB: df_aq = gpd.read_file(self.aq_path, layer = "annual")
            df_aq = gpd.read_file(self.aq_path)
            self.set_percent_complete(15)

            # READ IN STANDARD INPUTS
            # IFPRI crop names and ID codes
            self.df_crops = pd.read_csv(self.croplist_path, index_col=0, header=0)
            self.set_percent_complete(20)

            # Read in GADM Admin 1 and 0 place names (encoding, should retain
            # non-english characters)
            self.df_admnames = pd.read_csv(self.adm_path, encoding='utf-8-sig')
            self.set_percent_complete(25)

            # Make sure lists are lists, not strings
            self.df_admnames['PFAF_ID'] = self.df_admnames['PFAF_ID'].apply(lambda x: ast.literal_eval(x))
            self.df_admnames['AQID'] = self.df_admnames['AQID'].apply(lambda x: ast.literal_eval(x))
            self.set_percent_complete(30)

            # ----------
            # CLEAN DATA
            # ----------
            # TRANSLATE USER SELECTIONS INTO ANALYSIS-READY INPUTS
            # Aqueduct Indicators
            #indicator_selection = self.indicator_dict.get(self.user_indicator)
            #indicator_abb = indicator_selection[0:3].upper()
            indicator_selection = self.user_indicator + "_raw"
            indicator_abb = self.user_indicator

            # Agriculture Irrigation Type (for now, always use all, but building in
            # ability to change in the future)
            self.irrigation_selection = "_a"

            # Crops (for now, use all crops in import file. But leaving the ability
            # to filter by crop in the future)
            crop_selection = sorted(self.df_crops['short_name'].tolist())
            self.set_percent_complete(35)

            # INDICATOR SPECIFIC
            if self.user_indicator == "gtd":  # Groundwater Table Decline
                water_unit = "AQID"
                water_name = "Aquifer ID"
            else:
                water_unit = "PFAF_ID"
                water_name = "Watershed ID"

            # REMOVE POTENTIAL WHITESPACE FROM TEXT FIELDS
            clean_columns = ['State/Province', 'Country', 'Radius Unit', 'Material Type']
            for c in clean_columns:
                # import pdb
                # pdb.set_trace()
                if str(df[c].dtype) == 'object':
                    df[c] = df[c].str.strip()  # Remove extra whitespaces
                df[c].replace('None', np.nan, inplace=True)  # Turn "None" into np.nan

            self.set_percent_complete(40)

            # CROP NAME LOOKUP TABLE

            # Create lookup dictionary of crop names to crop IDs using IFPRI
            # definitions
            crop_dict = self.df_crops.set_index('full_name')['short_name'].to_dict()
            self.set_percent_complete(45)

            # Add alternatives that might appear
            crop_dict.update(self.crop_fixes)

            # Match user crops to IFPRI crops
            # Create Crop ID using IFPRI crop name lookup dictionary
            df['SPAM_code'] = df['Material Type'].apply(lambda x: crop_dict.get(x.lower()))

            # Drop rows without crop IDs
            self.df_2 = df[df['SPAM_code'].isin(crop_selection)]

            # CREATE ERROR LOG
            # List of crops that failed
            self.df_cropfail = df[df['SPAM_code'].isna()]
            self.df_cropfail['Error'] = 'Invalid Material Type'
            self.df_cropfail.drop(['SPAM_code'], axis=1, inplace=True)
            self.df_cropfail["row"] = self.df_cropfail.index

            # ----------------------------------
            # FIND LOCATIONS BASED ON WATER UNIT
            # ----------------------------------
            # Categorize location type
            self.df_2['Select_By'] = self.df_2.apply(lambda x: self.find_selection_type(x), axis=1)
            self.set_percent_complete(50)

            # CREATE ERROR LOG
            self.df_locfail = self.df_2[self.df_2['Select_By'].isna()]
            self.df_locfail['Error'] = 'Missing Location'
            self.df_locfail.drop(['SPAM_code', 'Select_By'], axis=1, inplace=True)
            self.df_locfail["row"] = self.df_locfail.index
            self.set_percent_complete(55)

            loc_time = time.time()
            df_waterunits, df_errorlog = self.find_locations(water_unit)
            logging.info("Locations ready in {} seconds".format(time.time() - loc_time))
            loc_time = time.time()
            self.set_percent_complete(75)

            # --------------
            # FIND LOCATIONS
            # --------------
            # Create formated column names for output
            raw = '{} Raw Value'.format(indicator_abb)
            score = '{} Score'.format(indicator_abb)
            desired_con = '{} Desired Condition'.format(indicator_abb)
            change_req = '{} % Change Required'.format(indicator_abb)

            # Filter Aqueduct data by sourcing watersheds and selected indicator
            sourcing_watersheds = list(set(df_waterunits[water_unit].tolist()))
            string_sourcing_watersheds = [str(int(x)) for x in sourcing_watersheds]
            users_watersheds = df_aq[df_aq[water_unit.lower()].isin(string_sourcing_watersheds)]

            self.set_percent_complete(78)

            # Pull raw value and label
            users_watersheds = users_watersheds.filter([water_unit.lower(), indicator_selection, indicator_abb.lower() + "_label"])
            self.set_percent_complete(79)

            # rename raw and score columns
            users_watersheds.rename(columns={water_unit.lower(): water_unit,
                                             indicator_selection: raw,
                                             indicator_abb.lower() + "_label": score}, inplace=True)
            self.set_percent_complete(80)

            # Drop duplicates
            users_watersheds.drop_duplicates(inplace=True)

            # Create a column to hold threshold
            users_watersheds[desired_con] = self.user_threshold

            # interact
            # Calculate change required
            users_watersheds[raw] = users_watersheds[raw].astype(float)
            users_watersheds[desired_con] = users_watersheds[desired_con].astype(float)
            users_watersheds[change_req] = ((users_watersheds[raw] - users_watersheds[desired_con]) / users_watersheds[raw])
            users_watersheds[change_req] = np.where(users_watersheds[raw] < users_watersheds[desired_con], 0, users_watersheds[change_req])
            self.set_percent_complete(85)

            # Format columns
            users_watersheds[raw] = (users_watersheds[raw] * 100).astype(int)
            users_watersheds[desired_con] = (users_watersheds[desired_con] * 100).astype(int)
            users_watersheds[change_req] = (users_watersheds[change_req] * 100).astype(int)
            self.set_percent_complete(90)

            # Tried this to get rid of NaN values.
            # users_watersheds[change_req] = np.where(pd.isna(users_watersheds[change_req]), None, users_watersheds[change_req])

            # Merge with user's OG data
            df_waterunits[water_unit] = df_waterunits[water_unit].astype(int)
            users_watersheds[water_unit] = users_watersheds[water_unit].astype(int)
            df_successes = pd.merge(df_waterunits, users_watersheds, how='left', left_on=water_unit, right_on=water_unit)
            df_successes.rename(columns={water_unit: water_name}, inplace=True)
            self.set_percent_complete(95)

            df_successes['row'] = df_successes['row'].astype(int)
            if 'Watershed ID' in df_successes.columns:
              df_successes['Watershed ID'] = df_successes['Watershed ID'].astype(int)

            # create list of priority watersheds (exceed threshold)
            # priority_watersheds = list(set(df_successes[water_name][df_successes[change_req] > 0].tolist()))

            results = {}
            results['locations'] = list(map(self.prepare_payload, df_successes.to_dict('records')))
            results['errors'] = list(map(self.prepare_payload, df_errorlog.to_dict('records')))
            results['indicator'] = self.user_indicator
            # results['all_waterunits'] = sourcing_watersheds
            # results['priority_waterunits'] = priority_watersheds

            if self.bucket:
                self.upload_results(results)
            else:
                self.redis.hset(self.job_token, "results", json.dumps(results))

            self.redis.hset(self.job_token, "status", "ready")
            self.set_percent_complete(100)

            logging.info("Analysis Time: {} seconds".format(time.time() - self.analysis_time))
        except Exception as e:
            # logging.fatal(e.back)
            self.redis.hset(self.job_token, "status", "error")
            self.redis.hset(self.job_token, "results", json.dumps({"error": str(e)}))

    def upload_results(self, results):
            session = boto3.session.Session()
            s3 = None

            if os.environ.get("ENDPOINT_URL"):
                s3 = session.client(
                     service_name='s3',
                     aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                     aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                     endpoint_url=os.environ.get("ENDPOINT_URL")
                )
            else:
                s3 = session.client(
                     service_name='s3',
                     aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                     aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
                )

            key = "food-supply-chain/{}".format(self.job_token.decode('utf-8'))

            s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(results))

            # Generate the URL to get 'key-name' from 'bucket-name'
            s3_url = s3.generate_presigned_url(
                     ClientMethod='get_object',
                     ExpiresIn=3600,
                     Params={'Bucket': self.bucket, 'Key': key}
            )

            self.redis.hset(self.job_token, "results", json.dumps({"s3_url": s3_url}))


    # Define whether location will use point + radius, state, or country to
    # select watersheds
    def find_selection_type(self, row):
        """
        :param row: individual row
        :return: location type (point, state, country, none)
        """
        # If coordiantes exist, location type is point
        if isinstance(row['Latitude'], float) and np.isnan(row['Latitude'])==False:
            select_by = "point"
        # If state exists WITH country name, location type is state
        elif (isinstance(row['State/Province'], str) == True) & (isinstance(row['Country'], str) == True):
            select_by = "state"
        # If neither of those are true, and a country name exists, location type is country
        elif isinstance(row['Country'], str) == True:
            select_by = "country"
        # Else, no location type given. These will be dropped from the analysis
        else:
            select_by = np.nan
        return select_by

    # Clean up Radius buffer values. Remove 0's, convert to decimal degrees
    # (units of the analysis)
    def clean_buffer(self, row):
        """
        :param row: individual row (1 coordinate + material type)
        :return: buffer radius in decimal degrees
        """
        val = row.Radius  # Find the radius value
        unit = str(row['Radius Unit']).lower()  # Find the radius units
        try:
            float_val = float(val)  # Turn value to floast
            if float_val == 0.0:  # If radius is 0, set to NA
                new_val = np.nan
            elif unit in ['miles', 'mile']:  # If units are in miles, convert to KM (multiple by 1.609), then to degrees (divide by 111)
                new_val = float_val * 1.609 / 111.0
            elif unit in ['m', 'met', 'meter', 'meters']:  # If units are in meters, convert to KM then to degrees (divide by 111)
                new_val = (float_val / 1000) / 111.0
            elif unit in ['km', 'kilometer', 'kilometers']:  # If units are in kilometers, convert to degrees (divide by 111)
                new_val = float_val / 111.0
            else:  # Else, return Null radius, report as error
                new_val = np.nan
        except:
            new_val = np.nan
        return new_val

    # Create buffer (in decimal degrees) around point
    def buffer(self, row):
        """
        :param row: individual row (1 coordinate + material type)
        :return: circle polygon
        """
        return row.geometry.buffer(row.Buffer)

    # Match user-entered state and country names to GADM names
    def fuzzy_merge(self, df_1, df_2, key1, key2, threshold=90, limit=1):
        """
        source of function:
        https://stackoverflow.com/questions/13636848/is-it-possible-to-do-fuzzy-match-merge-with-python-pandas
        :param df_1: the left table to join
        :param df_2: the right table to join
        :param key1: key column of the left table
        :param key2: key column of the right table
        :param threshold: how close the matches should be to return a match, based on Levenshtein distance
        :param limit: the amount of matches that will get returned, these are sorted high to low
        :return: dataframe with boths keys and matches
        """
        s = df_2[key2].tolist()

        m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
        df_1['matches'] = m

        m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
        df_1['matches'] = m2

        return df_1

    # Explode sourcing locations by intersecting watersheds
    def explode_data(self, inDATA, user_id, pf_id):
        """
        source of function:
        https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows
        :param inDATA: table of user locations with a column containing list of watershed intersecting row's location
                        primary key = location + material
        :param user_id: row id column
        :param pf_id: Unique watershed ID
        :return: table where every location is repeated based on the number of watersheds it intersects with
                        primary key = watershed ID + location + material
        """
        # Copy data
        sc2 = inDATA.reset_index()
        # Final all watersheds
        vals = sc2[pf_id].values.tolist()
        # find number of watersheds to repeat per row
        rs = [len(r) for r in vals]
        # create repeating combo of field per watershed
        a = np.repeat(sc2[user_id], rs)
        explode = pd.DataFrame(np.column_stack((a, np.concatenate(vals))), columns=[user_id, pf_id])
        explode.drop_duplicates(subset=[pf_id, user_id], inplace=True)
        return explode

    # Perform a geospatial analysis and fuzzy name lookup to find locations
    def find_locations(self, water_unit):
        """
        :param water_unit: Geometry ID associated with the selected indicator. AQID for aquifers or PFAF_ID for watersheds
        :return: dataframe with that matches each supply location to its watersheds and crops (close to final product); dataframe with all errors logged
        """
        # INDICATOR SPECIFIC
        if water_unit == "AQID":
            ifpri_path = 'aqueduct/services/supply_chain_data/inputs_ifpri_production_aqid.csv'
        else:
            ifpri_path = 'aqueduct/services/supply_chain_data/inputs_ifpri_production_pfaf.csv'

        # ---------------
        # CROP PRODUCTION
        # ---------------
        # IFPRI crop production (make sure column names are lower case)
        df_ifpri = pd.read_csv(ifpri_path, index_col=0, header=0)
        df_ifpri.columns = [x.lower() for x in df_ifpri]
        # Filter by irrigation selection
        df_if = df_ifpri[[x for x in df_ifpri.columns if self.irrigation_selection in x]]
        # Remove irrigation suffix
        df_if.columns = [x.replace(self.irrigation_selection, "") for x in df_if]
        # Melt dataframe so every row is unique watershed + crop combo
        df_prod = pd.melt(df_if, ignore_index=False)
        # Rename columns
        df_prod.columns = ['SPAM_code', "IFPRI_production_MT"]
        # Create a binary variable. Crops are grown if at least 10 MT are produced
        df_prod['grown_yn'] = np.where(df_prod['IFPRI_production_MT'] >= 10, 1, 0)

        self.set_percent_complete(56)

        # ---------
        # COUNTRIES
        # ---------
        stime1 = time.time()
        # Select Country and State location types
        df_ad = self.df_2[(self.df_2['Select_By'] == 'country') | (self.df_2['Select_By'] == 'state')]
        # Apply automatic fix to select country names
        df_ad['Country'][df_ad['Country'].isin(self.country_fixes)] = df_ad['Country'][
            df_ad['Country'].isin(self.country_fixes)].apply(lambda x: self.country_fixes.get(x.strip()))
        # Create country-to-water lookup
        ad0hys = self.df_admnames.filter(['GID_0', water_unit])
        # Group GID_0 lists together
        ad0hys = ad0hys.groupby(['GID_0'])[water_unit].agg(list).to_frame()
        # Unnest the lists
        ad0hys[water_unit] = ad0hys[water_unit].apply(lambda x: list(chain.from_iterable(x)))
        # Create GADM country names lookup
        gdf0 = self.df_admnames.filter(['GID_0', 'NAME_0']).drop_duplicates()
        # - - - - - - - - - - - MATCH USER NAME TO GADM NAME - - - - - - - - - - - #
        df_adm = self.fuzzy_merge(df_ad.reset_index(), gdf0, 'Country', 'NAME_0', threshold=85)
        df_adm.rename(columns={'matches': 'Country_clean'}, inplace=True)
        df_adm = pd.merge(df_adm, gdf0, how='left', left_on='Country_clean', right_on='NAME_0')
        df_adm['Country_clean'].replace('', np.nan, inplace=True)
        # Filter by GID_0
        ad0_ids = df_adm[['row', 'GID_0']][
            (df_adm['Select_By'] == 'country') & (~df_adm['Country_clean'].isna())].set_index('row')
        # - - - - - - - - - - - LINK WATERSHED ID BASED ON GADM NAME- - - - - - - - - - - #
        ad0_basins = pd.merge(ad0_ids, ad0hys, how='left', left_on='GID_0', right_index=True)
        ad0_basins.drop(['GID_0'], axis=1, inplace=True)

        # # # - - - - - - - - - - - CREATE ERROR LOG - - - - - - - - - - - #
        df_ad0fail = df_adm[df_adm['Country_clean'].isna()]
        df_ad0fail['Error'] = 'Country name did not match lookup table'
        df_ad0fail.set_index('row', inplace=True)
        df_ad0fail.drop(['SPAM_code', 'Select_By', 'Country_clean', 'GID_0', 'NAME_0'], axis=1, inplace=True)
        df_ad0fail["row"] = df_ad0fail.index

        logging.info("Countries found in {} seconds".format(time.time() - stime1))
        self.set_percent_complete(57)

        # ------
        # STATES
        # ------
        stime1 = time.time()
        ad1hys = self.df_admnames.filter(['GID_1', water_unit]).set_index('GID_1')
        # Seperate out state location types
        df_ad1 = df_adm[df_adm['Select_By'] == 'state']
        # - - - - - - - - - - - CREATE STATE NAME (State, Country) - - - - - - - - - - - #
        # Create full state name include cleaned country name for match
        df_ad1['state_full'] = df_ad1['State/Province'] + ", " + df_ad1['Country_clean']
        # - - - - - - - - - - - MATCH USER NAME TO GADM NAME - - - - - - - - - - - #
        # Perform fuzzy match
        df_ad1m = self.fuzzy_merge(df_ad1, self.df_admnames, 'state_full', 'state_full', threshold=90)
        df_ad1m.rename(columns={'matches': 'State_clean'}, inplace=True)
        df_ad1m = pd.merge(df_ad1m, self.df_admnames.filter(['GID_1', 'state_full']), how='left',
                           left_on=['State_clean'], right_on=['state_full'])
        df_ad1m['State_clean'].replace('', np.nan, inplace=True)
        ad1_ids = df_ad1m[['row', 'GID_1']][(df_ad1m['Select_By'] == 'state') & (~df_ad1m['State_clean'].isna())].set_index(
            'row')
        # # - - - - - - - - - - - LINK WATERSHED ID - - - - - - - - - - - #
        ad1_basins = pd.merge(ad1_ids, ad1hys, how='left', left_on='GID_1', right_index=True)
        ad1_basins.drop(['GID_1'], axis=1, inplace=True)

        # # # - - - - - - - - - - - CREATE ERROR LOG - - - - - - - - - - - #
        df_ad1fail = df_ad1m[df_ad1m['State_clean'].isna()]
        df_ad1fail.set_index('row', inplace=True)
        df_ad1fail = df_ad1fail.iloc[:, 0:7]
        df_ad1fail['Error'] = 'State name did not match lookup table'
        df_ad1fail["row"] = df_ad1fail.index

        logging.info("States found in {} seconds".format(time.time() - stime1))
        self.set_percent_complete(58)

        # ------
        # POINTS
        # ------
        stime1 = time.time()

        # Read in water geometries

        if not exists(self.hybas_path(water_unit)):
            gz_filename = "{}.gz".format(self.hybas_path(water_unit))
            logging.info("Decompressing {}".format(gz_filename))
            import gzip
            import shutil
            with gzip.open(gz_filename, 'rb') as f_in:
                with open(self.hybas_path(water_unit), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        # - - - - - - - - - - - CHECK THAT ANALYSIS NEEDS POINTS - - - - - - - - - - - #
        # SELECT POINT LOCATIONS
        df_points = self.df_2[self.df_2['Select_By'] == 'point']

        self.set_percent_complete(59)

        print("There are {} points".format(len(df_points)))

        if len(df_points) > 0:
            gdf = gpd.read_file(self.hybas_path(water_unit))
            gdf = gdf[1:]
            gdf.rename(columns={water_unit.lower(): water_unit.upper()}, inplace=True)

            # CLEAN DATA
            # Make sure coordinates are floats. Drop rows where encoding fails
            df_points['Latitude'] = pd.to_numeric(df_points['Latitude'], errors='coerce')  # Make sure latitudes are floats
            df_points['Longitude'] = pd.to_numeric(df_points['Longitude'], errors='coerce')  # Make sure latitudes are floats

            # CREATE ERROR LOG
            df_ptfail = df_points[(df_points['Longitude'].isna()) | (df_points['Latitude'].isna())]
            df_ptfail['Error'] = 'Non-numeric coordinates'
            df_ptfail.drop(['SPAM_code', 'Select_By'], axis=1, inplace=True)
            df_ptfail["row"] = df_ptfail.index

            self.set_percent_complete(60)

            # DROP BAD COORDINATES - - - - - - - - - - - #
            df_points.dropna(subset=['Latitude', 'Longitude'], inplace=True)
            # For any point row with missing radius OR radius units, set radius = 100km
            df_points['Radius'][(df_points['Radius'].isna()) | (df_points['Radius Unit'].isna())] = 100
            df_points['Radius Unit'][(df_points['Radius Unit'].isna())] = 'km'

            # FIND WATERSHEDS
            # Convert Radius into decimal degree value
            df_points['Buffer'] = df_points.apply(lambda x: self.clean_buffer(x), axis=1)

            self.set_percent_complete(61)

            # Create XY from coordinates
            df_points['geometry'] = df_points.apply(lambda row: Point(float(row.Longitude), row.Latitude), axis=1)
            buffered = df_points.filter(["Buffer", 'geometry', 'id'])
            buffered['geometry'] = buffered.apply(lambda x: x.geometry.buffer(x.Buffer), axis=1)
            buffered = gpd.GeoDataFrame(buffered, geometry=buffered.geometry)

            # Find allbasins within every buffer
            pts_hy6 = gpd.sjoin(buffered, gdf, how="left", op='intersects')
            pts_basins = pts_hy6.groupby(['row'])[water_unit].agg(list).to_frame()

            self.set_percent_complete(62)

            # Set name to uppercase
            pts_basins.columns = [water_unit.upper()]
        # - - - - - - - - - - - IF NOT POINTS GIVEN, CREATE BLANKS - - - - - - - - - - - #
        else:
            self.set_percent_complete(63)
            pts_basins = pd.DataFrame(columns=ad0_basins.columns)
            df_ptfail = pd.DataFrame(columns=df_ad0fail.columns)
            df_ptfail["row"] = df_ptfail.index
            pts_basins["row"] = pts_basins.index

        logging.info("Points found in {} seconds".format(time.time() - stime1))

        # -------
        # COMBINE
        # -------
        # Combine all basins together
        df_basins = pd.concat([pts_basins, ad0_basins, ad1_basins])
        # # Explode data for every row has a unique row # + sourcing watershed ID
        df_basinsexplode = self.explode_data(df_basins, 'row', water_unit)
        self.set_percent_complete(67)
        # # Find cropped sourced in each watershed
        df_sourcing = pd.merge(df_basinsexplode, self.df_2.filter(['Location ID', 'SPAM_code']), how='left', left_on='row',
                               right_index=True)

        self.set_percent_complete(70)

        # Add IFPRI production data to see what's actually grown
        df_sourced = pd.merge(df_sourcing, df_prod, how='left', left_on=[water_unit, 'SPAM_code'],
                              right_on=[water_unit, 'SPAM_code'])
        df_sourced = df_sourced[df_sourced.grown_yn == 1]
        # Add full crop name
        df_sourced = pd.merge(df_sourced, self.df_crops.filter(["full_name", "short_name"]).set_index("short_name"), how='left',
                              left_on="SPAM_code", right_index=True)

        self.set_percent_complete(73)

        # Clean columns
        df_sourced.drop(['grown_yn', 'SPAM_code'], axis=1, inplace=True)
        df_sourced = df_sourced[['row', 'Location ID', water_unit, 'full_name', 'IFPRI_production_MT']]
        df_sourced.rename(columns={"full_name": "Crop_Name"}, inplace=True)

        self.set_percent_complete(75)

        df_fails = pd.concat([self.df_cropfail, self.df_locfail, df_ptfail, df_ad0fail, df_ad1fail])

        return df_sourced, df_fails


# docker-compose -f docker-compose-gr.yml run develop bash
# python3 ./aqueduct/services/food_supply_chain_service.py cep 0.5 test-bucket
if __name__ == '__main__':
    import sys
    import pdb

    logging.basicConfig(stream=sys.stdout, filemode="w", level=logging.INFO)

    if len(sys.argv) < 3:
        print("pass in indicator as first argument: bws, bwd, cep, udw, usa, gtd")
        print("pass in threshold as second arg")
        print("pass in bucket at third arg if you want to create it")
        exit()

    if sys.argv[3]:
        session = boto3.session.Session()
        s3 = session.client(
             service_name='s3',
             aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
             aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
             endpoint_url=os.environ.get("ENDPOINT_URL")
        )
        try:
            print("Making bucket {} if it doesn't exist".format(sys.argv[3]))
            s3.create_bucket(Bucket=sys.argv[3])
        except Exception as e:
            print(str(e))

    user_indicator = sys.argv[1]
    user_threshold = float(sys.argv[2])
    #user_input = 'aqueduct/services/supply_chain_data/template_supply_chain_v20210701_example2.xlsx'
    user_input = 'aqueduct/services/supply_chain_data/just.countries.xlsx'
    analyzer = FoodSupplyChainService(user_indicator=user_indicator, user_threshold=user_threshold, user_input=user_input)
    analyzer.enqueue()

    job_token = analyzer.results()['job_token']
    print("job_token = {}".format(job_token))

    # print("testing s3 upload")
    # analyzer.upload_results({"apples": "yum"})

    print("popping work")
    FoodSupplyChainService.pop_and_do_work()

    print("Getting results")
    analyzer2 = FoodSupplyChainService(job_token=job_token)

    print(analyzer2.results())
