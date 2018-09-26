import os
import numpy as np
import pandas as pd
import sqlalchemy
from cached_property import cached_property
import logging
from aqueduct.errors import DBError

class CBADefaultService(object):
    def __init__(self, user_selections):
        ### DBConexion
        self.engine = sqlalchemy.create_engine(os.getenv('POSTGRES_URL'))
        self.metadata = sqlalchemy.MetaData(bind=self.engine)
        self.metadata.reflect(self.engine)
        ### BACKGROUND INTO 
        #self.flood = "Riverine"
        self.scenarios = {"business as usual": ['rcp8p5', 'ssp2', "bau"],
                     "pessimistic": ['rcp8p5', 'ssp3', "pes"],
                     "optimistic": ['rcp4p5', 'ssp2', "opt"]}
        ###  USER INPUTS 
        self.geogunit_unique_name = user_selections.get("geogunit_unique_name")
        self.scenario = self.scenarios.get(user_selections.get("scenario"))

    
        
    
    #@cached_property
    def default(self):
        fids, geogunit_name, geogunit_type = pd.read_sql_query("SELECT fids, name, type FROM lookup_master where uniqueName = '{0}' ".format(self.geogunit_unique_name), self.engine).values[0]
        clim, socio, scen_abb = self.scenario
        rps = np.array([2, 5, 10, 25, 50, 100, 250, 500, 1000])
        ##prot
        read_prot = 'precalc_agg_riverine_{0}_nosub'.format(geogunit_type).lower()
        col_prot = 'urban_damage_v2_2010_{0}_prot_avg'.format(scen_abb)
        df_prot = pd.read_sql_query("SELECT {0} FROM {1} where id like '{2}'".format(col_prot, read_prot, self.geogunit_unique_name), self.engine)
        prot_val = 0 if df_prot.empty else df_prot.values[0].tolist()[0]
        
        ##costs
        con_itl = pd.read_sql_query("SELECT avg(construction_cost_index) FROM lookup_construction_factors_geogunit_108 where fid_aque in ({0}) ".format(', '.join(map(str, fids))), self.engine)

        return [{
            "existing_prot": prot_val,
            "prot_fut": int(rps[np.where(rps > prot_val)][0]),
            "estimated_costs": 0 if con_itl.empty else con_itl.values[0].tolist()[0]

        }]