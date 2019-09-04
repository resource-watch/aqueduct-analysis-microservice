import logging
import os

import numpy as np
import pandas as pd
import sqlalchemy
from cached_property import cached_property
from scipy.interpolate import interp1d


class RiskService(object):
    def __init__(self, user_selections):
        ### DBConexion
        logging.info('[RiskService - Setting up conexion to the DB]: ...')
        self.engine = sqlalchemy.create_engine(os.getenv('POSTGRES_URL'))
        self.metadata = sqlalchemy.MetaData(bind=self.engine)
        self.metadata.reflect(self.engine)
        logging.info('[RiskService - Setting up conexion to the DB]: Success')
        ###               BACKGROUND INTO  
        self.flood_types = ["riverine", "coastal"]
        self.exposures = ["gdpexp", "popexp", "urban_damage_v2"]
        self.geogunits = ["geogunit_103", "geogunit_108"]
        self.scenarios = {"business as usual": ['rcp8p5', 'ssp2', "bau"],
                          "pessimistic": ['rcp8p5', 'ssp3', "pes"],
                          "optimistic": ['rcp4p5', 'ssp2', "opt"]}
        self.models = {"riverine": ["gf", "ha", "ip", "mi", "nr"],
                       # "coastal": ["wt"]}
                       "coastal": ["95", "50", "05"]}
        self.years = [2010., 2030., 2050., 2080.]
        self.ys = [str(x)[0:4] for x in self.years]
        self.rps = [2, 5, 10, 25, 50, 100, 250, 500, 1000]
        self.rps_names = ["rp" + str(x).zfill(5) for x in self.rps]
        # MANDATORY USER INPUTS
        self.flood = user_selections.get("flood")  # Flood type
        self.exposure = user_selections.get("exposure")  # Exposure type
        self.geogunit_unique_name = user_selections.get("geogunit_unique_name")  # Unique geographical unit name
        self.sub_scenario = user_selections.get(
            "sub_scenario")  # Subsidence option (Will always be no for Riverine floods)
        self.existing_prot = user_selections.get(
            "existing_prot")  # User input for protection standard (triggers on-the-fly calculation)
        self.scenario = user_selections.get("scenario")
        self.geogunit, self.geogunit_name, self.geogunit_type, self.clim, self.socio, self.scen_abb, self.sub_abb, self.df_precalc, self.prot_pres, self.risk_analysis = self.user_selections()
        # Scenario abbrevation
        self.mods = self.models.get(self.flood)

    def user_selections(self):
        """
        Purpose: Gather all neccesary inputs to run any analysis
        Input:
            flood: Riverine of Coastal (User must select)
            Geogunit_unique_name: geographical unit name from website.  (User must select)
                Website should use list of unique names to avoid selecting more than one unit
            Scenario: Business as usual, Pessimistic,  Optimistic
            sub_scenario: Yes (defaul(t), No does the user want to consider subsidence? Only relavent for coastal)
            existing_prot: Default protection standard. User can input their own or, which will trigger on-the-fly calculations
        Output:
            geogunit unit - (geogunit_103 for cities, geogunit_108 for everything else)
            geogunit_name - original (ie non-unique) name
            geogunit_type - City, State, Country, Basin
            clim - rcp4p5, rcp8p4 (climate scenario associated with overall scenario)
            socio - base, ssp2, ssp3 (socioeconomic scenario associated with overall scenario)
            sub_scenario- Yes, No (Is subsidence included?)
            sub_abb - wtsub or nosub (code name for subsidence. wtsub = with sub)
            prot_pres - default protection standard for unit as a whole
            risk_analysis - can we use precalculated risk data, or do we need to calculate on-the-fly?
        """

        # GEOGUNIT INFO
        fids, geogunit_name, geogunit_type = pd.read_sql_query(
            "SELECT fids, name, type FROM lookup_master where uniqueName = '{0}' ".format(self.geogunit_unique_name),
            self.engine).values[0]

        geogunit = "geogunit_103" if geogunit_type.lower() == "city" else "geogunit_108"

        # IMPACT DRIVER INFO (climate and socioeconomc scenarios
        clim, socio, scen_abb = self.scenarios.get(self.scenario)
        # SUBSIDENCE INFO
        # Make sure subsidence is turned off for river floods
        sub_abb = "wtsub" if self.sub_scenario else "nosub"

        # DEFAULT DATA
        defaultfn = "precalc_agg_{0}_{1}_{2}".format(self.flood, geogunit_type.lower(), sub_abb)
        logging.info(f'[RISK - user_selection]: {str(defaultfn)}')
        df_precalc = pd.read_sql_query("SELECT * FROM {0} where id like '{1}'".format(defaultfn, geogunit_name),
                                       self.engine, index_col='id')
        # PROTECTION STANDARDS and RISK ANALYSIS TYPE
        if not self.existing_prot:

            risk_analysis = "precalc"
            # Hardwire in the protection standards for the Netherlands or Average prot standard for a whole unit (i.e. country)
            # here self.exposure should be allways urban_damage_v2
            prot_pres = (1000 if geogunit_name in ['Noord-Brabant, Netherlands', 'Zeeland, Netherlands',
                                                   'Zeeuwse meren, Netherlands', 'Zuid-Holland, Netherlands',
                                                   'Drenthe, Netherlands', 'Flevoland, Netherlands',
                                                   'Friesland, Netherlands', 'Gelderland, Netherlands',
                                                   'Groningen, Netherlands', 'IJsselmeer, Netherlands',
                                                   'Limburg, Netherlands', 'Noord-Holland, Netherlands',
                                                   'Overijssel, Netherlands', 'Utrecht, Netherlands',
                                                   'Netherlands'] else df_precalc[
                ["_".join(['urban_damage_v2', '2010', scen_abb, "prot_avg"])]])

        else:
            risk_analysis = "calc"
            prot_pres = self.existing_prot

        return geogunit, geogunit_name, geogunit_type.lower(), clim, socio, scen_abb, sub_abb, df_precalc, prot_pres, risk_analysis

    def lp_data(self):
        inFormat = 'raw_agg_{:s}_{:s}_{:s}'.format(self.flood, self.geogunit_type, self.exposure)

        cols = [
            '{0} as {1}'.format(col, col.replace(self.clim, 'lp').replace(self.socio + "_" + self.sub_abb + "_", ''))
            for col in sqlalchemy.Table(inFormat, self.metadata).columns.keys() if
            (self.clim in col) and (self.socio in col) and (self.sub_abb in col)]

        df_temp = pd.read_sql_query(
            "SELECT {0} FROM {1} where id like '{2}'".format(', '.join(cols), inFormat, self.geogunit_name),
            self.engine)
        df_lpcurve = df_temp.T
        df1 = df_lpcurve.reset_index().rename(columns={"index": "index", 0: "y"})
        df2 = df_lpcurve.reset_index()['index'].str.split('_', expand=True).rename(
            columns={0: "lp", 1: "c", 2: "year", 3: "x"})

        return pd.concat([df1, df2], axis=1, join_axes=[df1.index])[['c', 'year', 'y', 'x']].replace(self.rps_names,
                                                                                                     self.rps)

    def bench(self):
        defaultfn = "precalc_agg_{0}_{1}_{2}".format(self.flood, self.geogunit_type, self.sub_abb)
        print(defaultfn)

        # cols = ['{0} as {1}'.format(col, col.replace(self.exposure, 'bench').replace('urban_damage_v2', 'bench').replace("_"+ self.scen_abb, '')) for col in sqlalchemy.Table(defaultfn, self.metadata).columns.keys() if ((self.exposure in col) or ('urban_damage_v2' in col)) and (self.scen_abb in col) and ("cc" not in col) and ("soc" not in col) and ("sub" not in col) and ("avg" in col)]
        cols = ['{0} as {1}'.format(col,
                                    col.replace(self.exposure, 'bench').replace('urban_damage_v2', 'bench').replace(
                                        "_" + self.scen_abb, '')) for col in
                sqlalchemy.Table(defaultfn, self.metadata).columns.keys() if
                ((self.exposure in col) or ('prot' in col)) and (self.scen_abb in col) and ("cc" not in col) and (
                        "soc" not in col) and ("sub" not in col) and ("avg" in col)]

        benchData = pd.read_sql_query("SELECT id, {0} FROM {1}".format(', '.join(cols), defaultfn), self.engine,
                                      index_col='id')

        return benchData

    def format_risk(self, dataframe):
        datalist = ["tot_avg", "tot_min", "tot_max",
                    "ast", "prot_avg",
                    "per_avg", "per_min", "per_max",
                    "cc_avg", "cc_min", "cc_max",
                    "soc_avg", "sub_avg"]

        colNames = ["Annual_Damage_Avg", "Annual_Damage_Min", "Annual_Damage_Max",
                    "Asset_Value", "Flood_Protection",
                    "Percent_Damage_Avg", "Percent_Damage_Min", "Percent_Damage_Max",
                    "CC_Driver_Avg", "CC_Driver_Min", "CC_Driver_Max",
                    "Soc_Driver", "Sub_Driver"]

        df_final = pd.DataFrame(index=self.ys, columns=colNames)

        for d in range(0, len(datalist)):
            selData = dataframe[[col for col in dataframe.columns.tolist() if (datalist[d] in col)]]
            if len(selData.values[0]) == 3:
                df_final[colNames[d]][1:] = selData.values[0]
            else:
                df_final[colNames[d]] = selData.values[0]

        return df_final

    def find_assets(self):
        """
        Purpose: Find total asset value
        Output:
            df_aggregate = Annual impacts for each year for user-selected geographical unit
        """
        # Create term to filter out unnecessary results. Drop SSP2 data if scenario
        #     is pessemistic. Else, drop SSP3
        dropex = "ssp2" if self.scen_abb == "pes" else "ssp3"
        assts = self.df_precalc[[col for col in self.df_precalc.columns.tolist() if
                                 (self.exposure in col) and (self.scen_abb in col) and ("ast" in col) and (
                                         dropex not in col)]]

        return assts.reset_index(drop=True)

    def run_stats(self, dataframe):
        """
        Purpose: Finds the average, min, and max impact for all impact types
        Input:
            dataframe: Data associated with flood, geography, exposure type for all climate models
        Output:
            Dataframe with average impact data for each year for each impact type. Also includes min and max (uncertainity)
        """
        # Create dataframe to hold final data
        df_final = pd.DataFrame(index=dataframe.index)
        # Define column field name structure
        colFormat = '{:s}_{:s}_{:s}_{:s}_{:s}'.format
        # Run following analysis for each year and impact type
        for y in self.ys:
            for t in ["cc", "soc", "sub", "tot", "prot"]:
                df_filt = dataframe[[col for col in dataframe.columns if (t in col) and (y in col)]]
                df_final[colFormat(self.exposure, y, self.scen_abb, t, "avg")] = df_filt.mean(axis=1)
                if y != '2010' and t == "tot" or y != '2010' and t == 'cc':
                    df_final[colFormat(self.exposure, y, self.scen_abb, t, "min")] = df_filt.min(axis=1)
                    df_final[colFormat(self.exposure, y, self.scen_abb, t, "max")] = df_filt.max(axis=1)
        df_final.replace(np.nan, 0, inplace=True)

        return df_final

    def ratio_to_total(self, dataframe):
        """
        Purpose: Finds the impact attributed to climate change only, socioecon only, and subsidence only
        Input:
            inData:   Annual expected impact data (found using default_risk function)
            mods: All possible climate models
        Output:
            Dataframe with final impact data for each year for each impact type. Column name also specifies given model
        """
        # Create dataframe to hold final data
        df_final = pd.DataFrame(index=dataframe.index)

        # Run analysis for each climate model and each year past 2010
        colFormat = '{:s}_{:s}_{:s}_{:s}_{:s}'.format

        df_final[colFormat(self.exposure, "2010", self.scen_abb, "prot", "avg")] = dataframe[
            colFormat(self.exposure, "2010", self.scen_abb, "prot", "avg")]

        tot2010 = dataframe[colFormat(self.exposure, "2010", self.scen_abb, "tot", "avg")]
        df_final[colFormat(self.exposure, "2010", self.scen_abb, "tot", "avg")] = tot2010

        for y in self.ys[1:]:
            # Filter data year
            df_filt = dataframe[[col for col in dataframe.columns if (y in col)]]

            # Total impact for selected year is already calculated
            df_final[colFormat(self.exposure, y, self.scen_abb, "tot", "avg")] = dataframe[
                colFormat(self.exposure, y, self.scen_abb, "tot", "avg")]
            df_final[colFormat(self.exposure, y, self.scen_abb, "tot", "min")] = dataframe[
                colFormat(self.exposure, y, self.scen_abb, "tot", "min")]
            df_final[colFormat(self.exposure, y, self.scen_abb, "tot", "max")] = dataframe[
                colFormat(self.exposure, y, self.scen_abb, "tot", "max")]

            # Find the difference from each impact to the 2010 baseline data
            df_filt['tot_diff'] = dataframe[colFormat(self.exposure, y, self.scen_abb, "tot",
                                                      "avg")] - tot2010  # Total impact
            df_filt['cc_diff_avg'] = dataframe[colFormat(self.exposure, y, self.scen_abb, "cc",
                                                         "avg")] - tot2010  # Total impact
            df_filt['cc_diff_min'] = dataframe[colFormat(self.exposure, y, self.scen_abb, "cc",
                                                         "min")] - tot2010  # Total impact
            df_filt['cc_diff_max'] = dataframe[colFormat(self.exposure, y, self.scen_abb, "cc",
                                                         "max")] - tot2010  # Total impact
            df_filt['soc_diff'] = dataframe[colFormat(self.exposure, y, self.scen_abb, "soc",
                                                      "avg")] - tot2010  # Total impact#Soc only impact
            df_filt['sub_diff'] = dataframe[colFormat(self.exposure, y, self.scen_abb, "sub",
                                                      "avg")] - tot2010  # Total impact #Subsidence only impact

            # Correct for values if impact is less than 2010 baseline data
            df_filt['cc_diff_avg'] = np.where(df_filt['tot_diff'] > 0,
                                              np.where(df_filt['cc_diff_avg'] < 0, 0, df_filt['cc_diff_avg']),
                                              np.where(df_filt['cc_diff_avg'] > 0, 0, df_filt['cc_diff_avg']))
            df_filt['cc_diff_min'] = np.where(df_filt['tot_diff'] > 0,
                                              np.where(df_filt['cc_diff_min'] < 0, 0, df_filt['cc_diff_min']),
                                              np.where(df_filt['cc_diff_min'] > 0, 0, df_filt['cc_diff_min']))
            df_filt['cc_diff_max'] = np.where(df_filt['tot_diff'] > 0,
                                              np.where(df_filt['cc_diff_max'] < 0, 0, df_filt['cc_diff_max']),
                                              np.where(df_filt['cc_diff_max'] > 0, 0, df_filt['cc_diff_max']))
            df_filt['soc_diff'] = np.where(df_filt['tot_diff'] > 0,
                                           np.where(df_filt['soc_diff'] < 0, 0, df_filt['soc_diff']),
                                           np.where(df_filt['soc_diff'] > 0, 0, df_filt['soc_diff']))
            df_filt['sub_diff'] = np.where(df_filt['tot_diff'] > 0,
                                           np.where(df_filt['sub_diff'] < 0, 0, df_filt['sub_diff']),
                                           np.where(df_filt['sub_diff'] > 0, 0, df_filt['sub_diff']))

            if self.sub_abb == "nosub":
                df_filt['sub_diff'] = 0

            # Find the ratio of impact attributed to each impact cause ( use the difference from 2010, not the absolute impact)
            # Climate change only = (CC Only) / ( CC Only + Socio Only + Sub Only) * Total Impact
            df_final[colFormat(self.exposure, y, self.scen_abb, "cc", "avg")] = (df_filt['cc_diff_avg'] / (
                    df_filt['cc_diff_avg'] + df_filt['soc_diff'] + df_filt['sub_diff'] + .000000001)) * df_filt[
                                                                                    'tot_diff']
            df_final[colFormat(self.exposure, y, self.scen_abb, "cc", "min")] = (df_filt['cc_diff_min'] / (
                    df_filt['cc_diff_min'] + df_filt['soc_diff'] + df_filt['sub_diff'] + .000000001)) * df_filt[
                                                                                    'tot_diff']
            df_final[colFormat(self.exposure, y, self.scen_abb, "cc", "max")] = (df_filt['cc_diff_max'] / (
                    df_filt['cc_diff_max'] + df_filt['soc_diff'] + df_filt['sub_diff'] + .000000001)) * df_filt[
                                                                                    'tot_diff']

            # Socioecon change only = (Soc Only) / ( CC Only + Socio Only + Sub Only) * Total Impact
            df_final[colFormat(self.exposure, y, self.scen_abb, "soc", "avg")] = (df_filt['soc_diff'] / (
                    df_filt['cc_diff_avg'] + df_filt['soc_diff'] + df_filt['sub_diff'] + .000000001)) * df_filt[
                                                                                     'tot_diff']
            # Subsidence change only = (Sub Only) / ( CC Only + Socio Only + Sub Only) * Total Impact
            df_final[colFormat(self.exposure, y, self.scen_abb, "sub", "avg")] = (df_filt['sub_diff'] / (
                    df_filt['cc_diff_avg'] + df_filt['soc_diff'] + df_filt['sub_diff'] + .000000001)) * df_filt[
                                                                                     'tot_diff']
            df_final[colFormat(self.exposure, y, self.scen_abb, "prot", "avg")] = dataframe[
                colFormat(self.exposure, y, self.scen_abb, "prot", "avg")]
            # Replace any nulls with 0
            df_final.replace(np.nan, 0, inplace=True)
        return df_final

    @staticmethod
    def expected_value(values, RPs, RP_zero, RP_infinite):
        """
        Purpose: Annual expected image/damage for given time period
        Input:
            values: Impact per return period
                2D array MxN
                    M: several time periods
                    N: several return periods
            RPs: return periods (equal to length of N)
            RP_zero: return period at which to break the EP-curve to zero (i.e. protection standard)
            RP_infinite: return period close to the infinitely high return period
        Output:
            vector with expected values for each time period
        """
        # append the return period at which maximum impact occurs, normally this is set to 1e6 years

        RPs = np.append(np.array(RPs), RP_infinite)
        # derive the probabilities associated with return periods
        prob = 1. / RPs
        values = np.array(values)
        # append infinite impact (last value in array) to array. Simply copy the last value.
        values = np.append(values, values[-1])
        # now make a smooth function (function relates prob (on x) to projected future impact (y))
        values_func = interp1d(prob, values)
        # Returns 10,000 evenly spaced probabilities from most likely prob to most extreme
        prob_smooth = np.linspace(prob[0], prob[-1], 10000)
        # Insert these probabilites into "smooth function" to find their related impact
        values_smooth = values_func(prob_smooth)
        # Set all impacts above thres (protection standard) to zero
        values_smooth[prob_smooth > 1. / RP_zero] = 0.
        # compute expected values from return period values:
        # Integrate under curve to find sum of all impact
        exp_val = np.trapz(np.flipud(values_smooth), np.flipud(prob_smooth))
        # print "Values, RP, Exp Value", values,  RP_zero, exp_val,
        return exp_val

    @staticmethod
    def interp_value(x, y, x_i, min_x=-np.Inf, max_x=np.Inf):
        """
        Purpose:  Find impacts associated with given protection standard
        OR        Find probability associated with a given impact

        Allows for extrapolation to find new Y given user-defined X
        Do a linear inter/extrapolation of y(x) to find a value y(x_idx)
        """
        ### OLD CODE
        # Creates a function that relates X and Y and allows for extrapolation to find new Y given user-defined X
        # y_interp = extrap1d(interp1d(np.array(x), np.array(y), axis=0))
        # return y_interp(np.maximum(np.minimum(np.atleast_1d(x_i), max_x), min_x))
        # -#-#-#-#-#-#-#-#-#-#-#-#-#
        ### NEW CODE
        # interpolation only! return y min/max if out of bounds
        x = np.atleast_1d(x)
        y = np.atleast_1d(y)
        f = interp1d(x, y, fill_value=(y.min(), y.max()), bounds_error=False)
        y_new = f(x_i)
        return y_new

    @staticmethod
    def extrap1d(interpolator):
        """
        Purpose: Make an extrapolation function
        """
        xs = interpolator.x
        ys = interpolator.y

        def pointwise(x):
            # If new prob is smaller than smallest prob in function
            if x < xs[0]:
                return ys[0] + (x - xs[0]) * (ys[1] - ys[0]) / (xs[1] - xs[0])
            # If new prob is larger than largest prob in function
            elif x > xs[-1]:
                return ys[-1] + (x - xs[-1]) * (ys[-1] - ys[-2]) / (xs[-1] - xs[-2])
            # If prob falls within set range of prob in function
            else:
                return interpolator(x)

        def ufunclike(xs):
            return np.fromiter(map(pointwise, np.array(xs)))

        return ufunclike

    def compute_rp_change(self, ref_impact, target_impact, rp, min_rp=2, max_rp=1000):
        """
        Purpose: Compute how return period protection changes from one impact
        distribution to another (e.g. present to future)
        Input:
            rps: return periods of impacts
            ref_impact: set of reference impact
            target_impacts: impacts to which protection standard should be mapped
                            (i.e. year the flood protection should be valid in)
            rp, protection standard at reference impacts
        """
        ### NEW CODE
        if target_impact.sum() == 0:
            new_prot = np.nan
            
        else:
            # interpolate to estimate impacts at protection level 'rp'
            prot_impact = self.interp_value(self.rps, ref_impact, rp)
            new_prot = self.interp_value(target_impact, self.rps, prot_impact)
        return new_prot

    def find_impact(self, impact_cc, impact_soc, impact_sub, impact_cc_soc, impact_urb, model):
        """
        Purpose: Finds annual impacts for climate only, socio only, subsidence only, and all scenarios together
        Input:
            impact_cc: Climate change only impacts.Variable consists of 4 dataframes (one for each year)
            impact_soc: Socioecon change only impacts. Variable consists of 4 dataframes (one for each year)
            impact_sub: Subsidence only impacts. Variable consists of 4 dataframes (one for each year)
            impact_cc_sub: Total impacts. Variable consists of 4 dataframes (one for each year)
            impact_urb: Climate change only impacts to urban damage. Variable consists of 4 dataframes (one for each year)
            model = Climate change model associated with input data
        Output:
            Dataframe with raw annual impact data for each year for each impact type. Column name also specifies given model
        """
        # Create dataframes to hold expected impact (for each model and year)
        col = [model + x + j for x in ["_cc_", "_soc_", "_sub_", "_tot_", "_prot_"] for j in self.ys]
        model_imps = pd.DataFrame(index=[self.geogunit_name], columns=col)
    
        # Perform for each year we have impact data

        for y, imp_cc, imp_soc, imp_sub, imp_cc_soc, imp_urb in zip(self.ys, impact_cc, impact_soc, impact_sub,
                                                                    impact_cc_soc, impact_urb):
            # No transformation needed in 2010
            if y == '2010':
                prot_trans = self.prot_pres
            else:
                # Find how the flood protection changes over time

                prot_trans = self.compute_rp_change(impact_urb[0], imp_urb.values[0], self.prot_pres,
                                                    min_rp=min(self.rps), max_rp=max(self.rps))  # i.e. RP_zero
            # Find the annual expected damage with the new protection standard

            model_imps.loc[self.geogunit_name, [model + "_cc_" + y]] = self.expected_value(imp_cc.values[0], self.rps,
                                                                                           prot_trans, 1e5)
            model_imps.loc[self.geogunit_name, [model + "_soc_" + y]] = self.expected_value(imp_soc.values[0], self.rps,
                                                                                            prot_trans, 1e5)
            model_imps.loc[self.geogunit_name, [model + "_sub_" + y]] = self.expected_value(imp_sub, self.rps,
                                                                                            prot_trans, 1e5)
            model_imps.loc[self.geogunit_name, [model + "_tot_" + y]] = self.expected_value(imp_cc_soc.values[0],
                                                                                            self.rps, prot_trans, 1e5)
            model_imps.loc[self.geogunit_name, [model + "_prot_" + y]] = prot_trans

        return model_imps

    def select_projection_data(self, dataframe, climate, model, socioecon, year):
        """
        Purpose: Pull all historical (2010) raw data
        Input:
            dataframe:  Raw data associated with user-defined flood, geographic unit and exposure
            climate = Climate scenario
            model = Climate model
            socioecon = Socioeconomic scenario
            sub_scenario: Is subsidence considered? Yes or No
            year: 2030, 2050, or 2080
        Output:mpact data for each return period for given year
            Dataframe with raw ir
        """
        # Select data using year, subsidence type, climate scen, socioecon scen, model

        # CHANGEDIT
        selCol = climate + "_" + model + "_" + socioecon + "_" + self.sub_abb + "_" + year
        #logging.debug(selCol)
        # selData = dataframe[[col for col in dataframe.index.tolist() if selCol in col]]
        selData = dataframe[[col for col in dataframe.columns if (selCol in col) and ("rp00001" not in col)]]
        # selData = dataframe[[col for col in dataframe.columns if (model in col) and (socioecon in col) and (climate in col)  and (year in col) and ("rp00001" not in col)]]
        #logging.debug(f'[RISK SERVICE - select_projection_data]: {selData}')
        return selData

    def calc_risk(self):
        """
        Purpose: Runs analysis on the fly instead of using precalcuted results
        (For when users define current protection level, find annual impact themselves)
        Output:
            df_aggregate = aggregated annual impacts for each year
        """
        # READ IN DATA
        # File name format for raw data
        inFormat = 'raw_agg_{:s}_{:s}_{:s}'.format
        fn = inFormat(self.flood, self.geogunit_type, self.exposure)

        # URBAN DAMAGE DATA
        urbfn = inFormat(self.flood, self.geogunit_type, "urban_damage_v2")

        # Filter by geographic name
        df_raw = pd.read_sql_query("SELECT * FROM {0} where id = '{1}' ".format(fn, self.geogunit_name), self.engine,
                                   index_col='id')
        df_urb = pd.read_sql_query("SELECT * FROM {0} where id = '{1}' ".format(urbfn, self.geogunit_name), self.engine,
                                   index_col='id')
        logging.info(f'[RISK SERVICE - calc_risk]: urbfn => {urbfn}  fn => {fn}')
        # Find impact for each model
        model_impact = pd.DataFrame(index=[self.geogunit_name])
        # Find model options associated with flood type
        modsT = '95' if self.flood == 'coastal' else 'wt'
        for m in self.mods:
            cc_raw, soc_raw, sub_raw, cc_soc_raw, urb_raw = [], [], [], [], []
            for y in self.ys:
                dfsub_a = []
                # 2010 DATA
                if y == '2010':
                    # Pull historical raw data
                    histData = self.select_projection_data(df_raw, "histor", modsT, "base", y)
                    cc_raw.append(histData)
                    soc_raw.append(histData)
                    cc_soc_raw.append(histData)
                    urb_raw.append(self.select_projection_data(df_urb, "histor", modsT, "base", y))

                    dfsub = histData


                # 2030, 2050, 2080 DATA
                else:
                    cc_raw.append(
                        self.select_projection_data(df_raw, self.clim, m, "base", y))  # Add to climate change only list
                    soc_raw.append(self.select_projection_data(df_raw, "histor", modsT, self.socio,
                                                               y))  # Add to socieco change only list
                    cc_soc_raw.append(self.select_projection_data(df_raw, self.clim, m, self.socio,
                                                                  y))  # Add to subsid change only list
                    urb_raw.append(
                        self.select_projection_data(df_urb, self.clim, m, "base", y))  # Add data using urban data

                    dfsub = self.select_projection_data(df_raw, "histor", modsT, "base",
                                                        y)  # Add to socieco change only list

                #logging.debug(f'[RISK SERVICE - calc_risk]: {dfsub.columns}')
                
                if not dfsub.empty:
                    dfsub_a = pd.melt(dfsub, value_vars=dfsub.columns)
                    sub_raw.append(pd.Series(name=self.geogunit_name, index=self.rps, data=dfsub_a["value"].tolist()))
                    
                

                #logging.debug(f'[RISK SERVICE - calc_risk]: {sub_raw}')

            if self.sub_scenario == False:
                sub_raw = []
                dfsub = pd.Series(name=self.geogunit_name, index=self.rps, data=0)
                sub_raw.extend([dfsub for i in range(4)])

            #logging.debug(f'[RISK SERVICE - calc_risk]: {len(sub_raw)}, {len(sub_raw[0])}')
            #logging.debug(f'[RISK SERVICE - calc_risk]: {type(sub_raw[0])}')

            outData = self.find_impact(cc_raw, soc_raw, sub_raw, cc_soc_raw, urb_raw, m)
            model_impact = model_impact.join(outData)

        df_stats = self.run_stats(model_impact)
        df_ratio = self.ratio_to_total(df_stats)

        assets = self.find_assets()
        df_risk = df_ratio.loc[self.geogunit_name]
        df_risk = df_risk.append(assets.T)

        # 2010 data
        colFormat = '{:s}_{:s}_{:s}_{:s}_{:s}'.format

        ast = df_risk.loc[colFormat(self.exposure, '2010', self.scen_abb, "ast", "tot")]
        imp = df_risk.loc[colFormat(self.exposure, '2010', self.scen_abb, "tot", "avg")]
        per = np.where(ast < imp, np.nan, imp / ast * 100)
        df_risk = pd.concat(
            [df_risk, pd.Series(per, index=[colFormat(self.exposure, '2010', self.scen_abb, "per", "avg")])])
        for y in self.ys[1:]:
            ast = df_risk.ix[colFormat(self.exposure, y, self.scen_abb, "ast", "tot")]
            for t in ["avg", "min", "max"]:
                imp = df_risk.ix[colFormat(self.exposure, y, self.scen_abb, "tot", t)]
                per = np.where(ast < imp, np.nan, imp / ast * 100)
                df_risk = pd.concat(
                    [df_risk, pd.Series(per, index=[colFormat(self.exposure, y, self.scen_abb, "per", t)])])

        return df_risk.T

    def precalc_risk(self):

        # Filter by
        # we have set  self.exposure as urban Damage

        df_risk = self.df_precalc[
            [col for col in self.df_precalc.columns.tolist() if (self.exposure in col) and (self.scen_abb in col)]]

        if self.exposure != 'urban_damage_v2':
            df_prot = self.df_precalc[
                [col for col in self.df_precalc.columns.tolist() if ("prot" in col) and (self.scen_abb in col)]]
            columnsD = [col for col in self.df_precalc.columns.tolist() if ("urban_damage_v2" in col)]
            df_prot.rename(
                columns=dict(zip(columnsD, [cols.replace("urban_damage_v2", self.exposure) for cols in columnsD])),
                inplace=True)
            df_risk = pd.concat([df_risk, df_prot], axis=1, sort=False)

        return df_risk

    @cached_property
    def meta(self):
        return {"flood": self.flood,
                "geogunit_name": self.geogunit_name,
                "geogunit_type": self.geogunit_type,
                "Scenario": self.scenario,
                "Exposure": self.exposure,
                "Average Protection": self.prot_pres if isinstance(self.prot_pres, int) else self.prot_pres.values[0][0]
                }

    def getRisk(self):
        # Run risk data analysis based on user-inputs
        if self.risk_analysis == "precalc":
            risk_data = self.precalc_risk()
        else:
            risk_data = self.calc_risk()

        return self.format_risk(risk_data)

    def get_widget(self, argument):
        method_name = 'widget_' + str(argument)
        logging.info(f'[RiskService - get_widget]: Getting widget {method_name}')
        method = getattr(self, method_name, lambda: "Widget not found")
        return method()

    def widget_table(self):
        return {'widgetId': 'table', 'chart_type': 'table', 'meta': self.meta, 'data': self.getRisk().reset_index()[
            ['index', 'Annual_Damage_Avg', 'Asset_Value', 'Percent_Damage_Avg', 'Flood_Protection']].to_dict('records')}

    def widget_annual_flood(self):
        return {'widgetId': 'annual_flood', 'chart_type': 'annual_flood', 'meta': self.meta,
                'data': self.getRisk().reset_index()[
                    ['index', 'Annual_Damage_Avg', 'Annual_Damage_Min', 'Annual_Damage_Max', 'Percent_Damage_Avg',
                     'Percent_Damage_Min', 'Percent_Damage_Max']].to_dict('records')}

    def widget_flood_drivers(self):
        return {'widgetId': 'flood_drivers', 'chart_type': 'flood_drivers', 'meta': self.meta,
                'data': self.getRisk().reset_index()[
                    ['index', 'Annual_Damage_Avg', 'Annual_Damage_Min', 'Annual_Damage_Max', 'Percent_Damage_Avg',
                     'Percent_Damage_Min', 'Percent_Damage_Max', 'CC_Driver_Avg', 'CC_Driver_Min', 'CC_Driver_Max',
                     'Soc_Driver', 'Sub_Driver']].to_dict('records')}

    def widget_benchmark(self):
        benchData = self.bench().reset_index()
        per = pd.melt(benchData[['id', 'bench_2010_prot_avg', 'bench_2030_prot_avg', 'bench_2050_prot_avg',
                                 'bench_2080_prot_avg']], id_vars=['id'],
                      value_vars=['bench_2010_prot_avg', 'bench_2030_prot_avg', 'bench_2050_prot_avg',
                                  'bench_2080_prot_avg'], var_name='c', value_name='prot')
        per['year'] = per.c.str.split('_').str.get(1)
        tot = pd.melt(benchData[
                          ['id', 'bench_2010_tot_avg', 'bench_2030_tot_avg', 'bench_2050_tot_avg', 'bench_2080_tot_avg',
                           'bench_2010_per_avg', 'bench_2030_per_avg', 'bench_2050_per_avg', 'bench_2080_per_avg']],
                      id_vars=['id'], value_vars=['bench_2010_per_avg', 'bench_2030_per_avg', 'bench_2050_per_avg',
                                                  'bench_2080_per_avg', 'bench_2010_tot_avg', 'bench_2030_tot_avg',
                                                  'bench_2050_tot_avg', 'bench_2080_tot_avg'], var_name='c1',
                      value_name='value')
        tot['year'] = tot['c1'].str.split('_').str.get(1)
        tot['type'] = tot['c1'].str.split('_').str.get(2)
        fData = per.merge(tot, how='right', left_on=['id', 'year'], right_on=['id', 'year'])
        return {'widgetId': "benchmark", "chart_type": "benchmark", "meta": self.meta,
                "data": fData.reset_index()[['id', 'year', 'type', 'value', 'prot']].to_dict('records')}

    def widget_lp_curve(self):
        return {'widgetId': "lp_curve", "chart_type": "lp_curve", "meta": self.meta,
                "data": self.lp_data().to_dict('records')}
