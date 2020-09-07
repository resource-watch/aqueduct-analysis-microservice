import datetime
import logging
import os

import numpy as np
import pandas as pd
import sqlalchemy
from flask import json
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.dialects.postgresql import JSON

from aqueduct.errors import Error


class CBADef(object):
    def __init__(self, user_selections):
        ### DBConexion
        self.engine = sqlalchemy.create_engine(os.getenv('POSTGRES_URL'))
        self.metadata = sqlalchemy.MetaData(bind=self.engine)
        self.metadata.reflect(self.engine)
        ### BACKGROUND INTO 
        # self.flood = "Riverine"
        self.scenarios = {"business as usual": ['rcp8p5', 'ssp2', "bau"],
                          "pessimistic": ['rcp8p5', 'ssp3', "pes"],
                          "optimistic": ['rcp4p5', 'ssp2', "opt"],
                          "rcp4p5": ['rcp4p5', 'ssp2', "opt"]}
        ###  USER INPUTS 
        self.geogunit_unique_name = user_selections.get("geogunit_unique_name")
        self.scenario = self.scenarios.get(user_selections.get("scenario"))
        self.flood = user_selections.get("flood")  # Flood type
        self.sub_scenario = user_selections.get("sub_scenario")

    # @cached_property
    def default(self):
        fids, geogunit_name, geogunit_type = pd.read_sql_query(
            "SELECT fids, name, type FROM lookup_master where uniqueName = '{0}' ".format(self.geogunit_unique_name),
            self.engine).values[0]
        clim, socio, scen_abb = self.scenario
        rps = np.array([2, 5, 10, 25, 50, 100, 250, 500, 1000])
        ##prot
        sub_abb = "wtsub" if self.sub_scenario else "nosub"

        # DEFAULT DATA
        read_prot = 'precalc_agg_{0}_{1}_{2}'.format(self.flood, geogunit_type.lower(), sub_abb)
        col_prot = 'urban_damage_v2_2010_{0}_prot_avg'.format(scen_abb)
        df_prot = pd.read_sql_query(
            "SELECT {0} FROM {1} where id like '{2}'".format(col_prot, read_prot, geogunit_name),
            self.engine)
        prot_val = 0 if df_prot.empty else int(df_prot.values[0].tolist()[0])
        prot_val =1000 if geogunit_name in ['Noord-Brabant, Netherlands', 'Zeeland, Netherlands',
                                                   'Zeeuwse meren, Netherlands', 'Zuid-Holland, Netherlands',
                                                   'Drenthe, Netherlands', 'Flevoland, Netherlands',
                                                   'Friesland, Netherlands', 'Gelderland, Netherlands',
                                                   'Groningen, Netherlands', 'IJsselmeer, Netherlands',
                                                   'Limburg, Netherlands', 'Noord-Holland, Netherlands',
                                                   'Overijssel, Netherlands', 'Utrecht, Netherlands',
                                                   'Netherlands'] else prot_val
        ##costs
        con_itl = pd.read_sql_query(
            "SELECT avg(construction_cost_index*7) FROM lookup_construction_factors_geogunit_108 where fid_aque in ({0}) ".format(
                ', '.join(map(str, fids))), self.engine)
        prot_round = int(rps[np.where(rps >= prot_val)][0])
        logging.debug(f'[CBADef, default]: {df_prot}')
        return [{
            "existing_prot": prot_val,
            "existing_prot_r": prot_round,
            "prot_fut": prot_round,
            "estimated_costs": 0 if con_itl.empty else con_itl.values[0].tolist()[0]

        }]


class CBADefaultService(object):
    """
    this will have the next methods:
        * create cache table (only if the table doesn't exist)
        * check if a certain set of parameters exists on the table, if exists it will retrive cbaService data from the row selected
        * if not it will trigger the CBAService class to calculate it.
    """

    ### DBConexion
    def __init__(self, params):
        logging.info('[CBADCache, __init__]: creating db engine')
        self.engine = sqlalchemy.create_engine(os.getenv('POSTGRES_URL'))
        logging.info('[CBADCache, __init__]: db engine created, loading metadata')
        self.metadata = sqlalchemy.MetaData(bind=self.engine, reflect=True)
        logging.info('[CBADCache, __init__]: db engine metadata loaded')
        # self.metadata.reflect(self.engine)
        self.params = params

    @property
    def _generateKey(self):
        return '_'.join([str(value) for (key, value) in sorted(self.params.items())])

    def _createTable(self):
        """
        where key is a composition of the user selected params: 
        "geogunit_unique_name"_"existing_prot"_"scenario"_"prot_fut"_"implementation_start_"implementation_end"_"infrastructure_life"_"benefits_start"_"ref_year"_"estimated_costs"_"discount_rate"_"om_costs"_"user_urb_cost"_"user_rur_cost"
        """
        try:
            logging.info(f'[]:{datetime.datetime.now}')
            myCache = sqlalchemy.Table("cache_d_cba", self.metadata,
                                       Column('id', Integer, primary_key=True, unique=True),
                                       Column('key', Text, unique=True, index=True),
                                       Column('value', JSON),
                                       Column('last_updated', DateTime, default=datetime.datetime.now,
                                              onupdate=datetime.datetime.now)
                                       )
            myCache.create()
        except Exception as e:
            logging.error('[CBADCache, _createTable]: ' + str(e))
            raise Error(str(e))
        return myCache

    def checkParams(self):
        try:
            table = self.metadata.tables['cache_d_cba']
            logging.info('[CBADCache, checkParams]: check params...')
            # logging.info(self._generateKey)
            select_st = table.select().where(table.c.key == self._generateKey)
            res = self.engine.connect().execute(select_st).fetchone()
            logging.info(res)
            return res
        except Exception as e:
            logging.error('[CBADCache, checkParams]: ' + str(e))
            raise Error(str(e))

    def insertRecord(self, key, data):
        # insert data via insert() construct
        try:
            table = self.metadata.tables['cache_d_cba']
            ins = table.insert().values(
                key=key,
                value=data)
            conn = self.engine.connect()
            conn.execute(ins)

            return 200

        except Exception as e:
            logging.error('[CBADCache, insertRecord]: ' + str(e))
            raise Error(str(e))

    def updateRecord(self):
        """
        TODO:
        """
        return 0

    def cleanCache(self):
        try:
            table_drop = self.metadata.tables['cache_d_cba'].delete()
            conn = self.engine.connect()
            conn.execute(table_drop)
            return 200
        except Exception as e:
            logging.error('[CBAICache, cleanCache table]: ' + str(e))
            raise Error(message='Cache table drop has failed. \n'+ str(e))

    def execute(self):
        try:
            inspector = sqlalchemy.inspect(self.engine)
            logging.info('[CBADCache]: Getting cba default...')
            if 'cache_d_cba' in inspector.get_table_names():
                # It means we have the cache table, we will need to check the params
                logging.info('[CBADCache]: table_exists')
                checks = self.checkParams()
                if checks != None:
                    logging.info('[CBADCache]: table available; extracting data')
                    data = json.loads(checks[2])
                    logging.info(data.keys())
                    return data['data']  # we will give back the data in a way CBAEndService can use it

                else:  # we will execute the whole process and we will generate the output in a way  CBAEndService can use it
                    logging.info('[CBADCache]: data not available; generating data')
                    data_output = CBADef(self.params).default()
                    data = json.dumps({'data': data_output}, ignore_nan=True)
                    key = self._generateKey
                    self.insertRecord(key, data)
                    return data_output  # we will give back the data in a way CBAEndService can use it
            else:
                self._createTable()
                self.execute()
                # executes the cba code to get the table, inserts it into the database and we should be ready to go
        except Exception as e:
            logging.error('[CBADCache, _createTable]: ' + str(e))
            raise Error(str(e))
