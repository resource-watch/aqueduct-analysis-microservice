import datetime
import logging
import os
import sys, traceback


import numpy as np
import pandas as pd
import sqlalchemy
from flask import json
from scipy.interpolate import interp1d
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.dialects.postgresql import JSON

from aqueduct.errors import Error


class CBAService(object):
    def __init__(self, user_selections):
        ### DBConexion
        self.engine = sqlalchemy.create_engine(os.getenv('POSTGRES_URL'))
        self.metadata = sqlalchemy.MetaData(bind=self.engine)
        self.metadata.reflect(self.engine)
        ### BACKGROUND INTO
        # self.flood = "Riverine"
        self.exposures = ["gdpexp", "popexp", "urban_damage_v2"]
        self.geogunit = "geogunit_108"
        self.scenarios = {"business as usual": ['rcp8p5', 'ssp2', "bau"],
                          "pessimistic": ['rcp8p5', 'ssp3', "pes"],
                          "optimistic": ['rcp4p5', 'ssp2', "opt"],
                          "rcp8p5": ['rcp8p5', 'ssp3', "pes"],
                          "rcp4p5": ['rcp8p5', 'ssp2', "bau"]}
        self.sub_abb = "nosub"
        self.mods = ["gf", "ha", "ip", "mi", "nr"]
        self.years = [2010., 2030., 2050., 2080.]
        self.ys = [str(x)[0:4] for x in self.years]
        self.rps = [2, 5, 10, 25, 50, 100, 250, 500, 1000]
        self.rps_names = ["rp" + str(x).zfill(5) for x in self.rps]
        self.cba_types = ['pop_costs', "gdp_costs", "urb_benefits", "pop_benefits", "gdp_benefits", "prot_present",
                          "prot_future"]
        self.inAGGFormat = 'raw_agg_riverine_{:s}_{:s}'.format
        self.inRAWFormat = 'raw_riverine_{:s}_{:s}'.format
        ###  USER INPUTS
        self.geogunit_unique_name = user_selections.get("geogunit_unique_name")
        self.existing_prot = user_selections.get("existing_prot")
        self.scenario = user_selections.get("scenario")
        self.prot_futu = user_selections.get("prot_fut")
        self.implementation_start = user_selections.get("implementation_start")
        self.implementation_end = user_selections.get("implementation_end")
        self.infrastructure_life = user_selections.get("infrastructure_life")
        self.benefits_start = user_selections.get("benefits_start")
        self.ref_year = user_selections.get("ref_year")
        self.estimated_costs = user_selections.get("estimated_costs")
        self.discount_rate = user_selections.get("discount_rate")
        self.om_costs = user_selections.get("om_costs")
        self.user_urb_cost = user_selections.get("user_urb_cost")
        self.user_rur_cost = user_selections.get("user_rur_cost")
        self.cost_option = "geogunit_108"

        # DEFAULT VARIABLES
        self.geogunit_name, self.geogunit_type, self.fids, self.clim, self.socio, self.scen_abb, self.prot_pres, rpend, \
        self.build_start_end, self.year_range, self.benefit_increase, self.prot_idx_fut, self.risk_analysis, self.df_prot, self.prot_fut = self.user_selections()
        logging.info(self.prot_fut)
        # Define the time series of the analysis
        self.time_series = np.arange(self.year_range[0], self.year_range[1] + 1)
        self.year_array = np.arange(len(self.time_series)) + 1.
        self.costFormat = 'lookup_cost_urban_{:s}_{:s}_{:s}'.format
        # READ IN COST DATA
        self.df_urb_all = self.costFormat(self.scen_abb, str(self.ref_year), self.cost_option)
        # Raw data aggregated to unit type
        self.df_urb_agg = self.inAGGFormat(self.geogunit_type.lower(), "urban_damage_v2")
        # Raw
        self.df_pop = self.inRAWFormat(self.geogunit, "popexp")
        self.df_gdp = self.inRAWFormat(self.geogunit, "gdpexp")
        self.df_urb = self.inRAWFormat(self.geogunit, "urban_damage_v2")
        self.geogunit = "geogunit_103" if self.geogunit_type.lower() == "city" else "geogunit_108"
        self.filt_risk = pd.read_sql_query(
            "SELECT * FROM Precalc_Riverine_{0}_nosub where id in ({1})".format(self.geogunit,
                                                                                ', '.join(map(str, self.fids))),
            self.engine)
        self.estimated_costs = None

    ##---------------------------------------------------
    ### FUNCTIONS FOR BOTH RISK AND CBA TABS          ###
    ##---------------------------------------------------
    def user_selections(self):
        """
        Purpose: Gather all neccesary inputs to run any analysis

        Output:
            geogunit_name: original (ie non-unique) name
            geogunit_type: City, State, Country, Basin
            clim: rcp4p5, rcp8p4 (climate scenario associated with overall scenario)
            socio: base, ssp2, ssp3 (socioeconomic scenario associated with overall scenario)
            prot_pres: default protection standard for unit as a whole
            df_precalc: precalculated impact data
            risk_analysis - can we use precalculated risk data, or do we need to calculate on-the-fly?
        """
        #logging.debug('[CBA, user_selections]: start')
        # GEOGUNIT INFO

        fids, geogunit_name, geogunit_type = pd.read_sql_query(
            "SELECT fids, name, type FROM lookup_master where uniqueName = '{0}' ".format(self.geogunit_unique_name),
            self.engine).values[0]
        logging.info(geogunit_name)
        logging.info(self.geogunit_unique_name)
        # IMPACT DRIVER INFO (climate and socioeconomc scenarios)
        clim, socio, scen_abb = self.scenarios.get(self.scenario)

        read_prot = 'precalc_agg_riverine_{0}_nosub'.format(geogunit_type).lower()

        df_prot = pd.read_sql_query("SELECT id, {0} FROM {1}".format(
            ', '.join([col for col in sqlalchemy.Table(read_prot, self.metadata).columns.keys() if ("prot" in col)]),
            read_prot), self.engine, index_col='id')

        # PROTECTION STANDARDS and RISK ANALYSIS TYPE
        prot_name = "_".join(["Urban_Damage_v2", '2010', scen_abb, "PROT_avg"]).lower()
        prot_pres_check = df_prot.loc[geogunit_name, prot_name]

        if geogunit_name in ['Noord-Brabant, Netherlands', 'Zeeland, Netherlands', 'Zeeuwse meren, Netherlands',
                            'Zuid-Holland, Netherlands', 'Drenthe, Netherlands', 'Flevoland, Netherlands',
                            'Friesland, Netherlands', 'Gelderland, Netherlands', 'Groningen, Netherlands',
                            'IJsselmeer, Netherlands', 'Limburg, Netherlands', 'Noord-Holland, Netherlands',
                            'Overijssel, Netherlands', 'Utrecht, Netherlands', "Netherlands"]:
            prot_pres_check = 1000
            
        logging.info("****************************")
        logging.info("existing_prot")
        logging.info(self.existing_prot)
        logging.info("prot_pres_check")
        logging.info(prot_pres_check)
        logging.info("****************************")

        if int(prot_pres_check) == self.existing_prot:
            prot_pres = self.existing_prot
            risk_analysis = "precalc"
            
        else:
            prot_pres = self.existing_prot
            risk_analysis = "calc"

        if self.prot_futu == None:
            logging.info(prot_pres)
            prot_fut = min([x for x in self.rps if x >= prot_pres])
        else:
            prot_fut = self.prot_futu

        # prot_start_unit = min(rps, key=lambda x:abs(x-prot_pres))
        build_start_end = (self.implementation_start, self.implementation_end)
        year_range = (self.implementation_start, self.implementation_start + self.infrastructure_life)
        benefit_increase = (self.benefits_start, self.implementation_end)

        prot_idx_fut = self.years.index(self.ref_year)

        # Define the desired protection standard
        rpend = "endrp" + str(prot_fut).zfill(5)
        return geogunit_name, geogunit_type, fids, clim, socio, scen_abb, prot_pres, rpend, \
               build_start_end, year_range, benefit_increase, prot_idx_fut, risk_analysis, df_prot, prot_fut

    def run_stats(self, dataframe):
        """
        select col_min, col_max, col_avg from table
        """
        #logging.debug('[CBA, run_stats]: start')
        df_stats = pd.DataFrame(index=dataframe.index)
        for t in self.cba_types:
            df_filt = dataframe[[col for col in dataframe.columns if (t in col.lower())]]
            df_stats[t + "_avg"] = df_filt.mean(axis=1)
            df_stats[t + "_min"] = df_filt.min(axis=1)
            df_stats[t + "_max"] = df_filt.max(axis=1)
        return df_stats

    def compute_costs(self, m, input_total_cost, exposure):

        """
           Output:
            time series of costs without any discount rate included
        """
        #logging.debug('[CBA, compute_costs]: start')
        time_series = np.arange(self.year_range[0], self.year_range[1] + 1)  # list of years until horizon
        build_years = np.arange(
            self.build_start_end[1] - self.build_start_end[0]) + 1.  # list of build years (starting at 1)
        cum_costs_present = np.cumsum(float(input_total_cost) * np.ones(len(build_years)) / len(
            build_years))  # cumulative building costs until end of building period
        maintenance = cum_costs_present * self.om_costs  # maintenance costs
        costs_pa_present = np.diff(np.append(0.,
                                             cum_costs_present)) + maintenance  # compute annual costs without discount rate until end of building
        costs_horizon_present = np.zeros(len(time_series))
        costs_horizon_present[self.build_start_end[0] - self.year_range[0]: self.build_start_end[1] - self.year_range[
            0]] = costs_pa_present  # insert costs until end of build
        costs_horizon_present[self.build_start_end[1] - self.year_range[0]:] = maintenance[
            -1]  # add maintenance until horizon
        #     costs_horizon_present = np.append(costs_pa_present, np.ones(year_range[-1]-build_start_end[-1] + 1)*maintenance[-1])  # add maintenance until horizon
        c = costs_horizon_present / ((1 + self.discount_rate) ** (self.year_array))
        df_results = pd.DataFrame(index=self.time_series, columns=[m + "_" + exposure + "_Costs"], data=c)
        # logging.debug(m ," compute cost done:", time.time() - stime1)
        return df_results

    def compute_benefits(self, model, annual_risk_pres, annual_risk_fut, annual_pop_pres, annual_pop_fut,
                         annual_gdp_pres, annual_gdp_fut, annual_prot_pres, annual_prot_fut):
        #logging.debug('[CBA, compute_benefits]: start')
        diff_urb = np.where(annual_risk_pres - annual_risk_fut < 0, 0, annual_risk_pres - annual_risk_fut)  # difference is the potential yearly benefit
        diff_pop = np.where(annual_pop_pres - annual_pop_fut < 0, 0, annual_pop_pres - annual_pop_fut) # difference is the potential yearly benefit
        diff_gdp = np.where(annual_gdp_pres - annual_gdp_fut < 0, 0, annual_gdp_pres - annual_gdp_fut) # difference is the potential yearly benefit

        relative_benefit = np.maximum(
            np.minimum(self.extrap1d(interp1d(list(self.benefit_increase), [0., 1.]))(self.time_series), 1.), 0.)
        urb_benefits = relative_benefit * diff_urb
        pop_benefits = relative_benefit * diff_pop
        gdp_benefits = relative_benefit * diff_gdp

        # compute discount rate for costs and benefits

        urb_benefits_discounted = urb_benefits / (
                (1 + self.discount_rate) ** (self.year_array))  # add the annual discount rate
        gdp_benefits_discounted = gdp_benefits / ((1 + self.discount_rate) ** (self.year_array))

        d = {model + "_Urb_Benefits": urb_benefits_discounted,
             model + "_Pop_Benefits": pop_benefits, model + "_GDP_Benefits": gdp_benefits_discounted,
             model + "_Prot_Present": annual_prot_pres, model + "_Prot_Future": annual_prot_fut}
        df_results = pd.DataFrame(index=self.time_series, data=d)

        return df_results

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
        #logging.debug('[CBA, expected_value]: start')
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
        #logging.debug('[CBA, interp_value]: start')
        x = np.atleast_1d(x)
        y = np.atleast_1d(y)
        f = interp1d(x, y, fill_value=(y.min(), y.max()), bounds_error=False)
        return f(x_i)

    @staticmethod
    def extrap1d(interpolator):
        """
        Purpose: Make an extrapolation function
        """
        #logging.debug('[CBA, extrap1d]: start')
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
            return np.fromiter(map(pointwise, np.array(xs)), dtype=np.float)

        return ufunclike

    def compute_rp_change(self, rps, ref_impact, target_impact, rp, min_rp=2, max_rp=1000):
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
        #logging.debug('[CBA, compute_rp_change]: start')
        # interpolate to estimate impacts at protection level 'rp'
        # atleast1_1d = scalar inputs are converted to 1-D arrays. Arrays of higher dimensions are preserved
        p = 1. / np.atleast_1d(rps)
        # Find the target impact given user-defined protection standard (rp) by running the interp_values function
        if target_impact.sum() == 0:
            new_prot = np.array([rp])
        else:
            prot_impact = self.interp_value(rps, ref_impact, rp)
            new_prot = self.interp_value(target_impact, rps, prot_impact)
        # lookup what the protection standard is within the target impact list
        return new_prot

    def find_startrp(self, x):
        #logging.debug('[CBA, find_startrp]: start')
        if pd.isnull(x):
            rpstart = np.nan
        else:
            prot_start_unit = min(self.rps, key=lambda rp: abs(rp - x))
            rpstart = "startrp" + str(prot_start_unit).zfill(5)
        return rpstart

    def find_dimension_v2(self, m, df_lookup, df_cost, user_urb):
        #logging.debug('[CBA, find_dimension_v2]: start')
        uniStartRPList = [x for x in list(set(df_lookup['startrp'].values)) if pd.notnull(x)]
        erp = "endrp" + str(self.prot_fut).zfill(5)
        logging.info(erp)
        logging.info(str(self.prot_fut))
        uniSRPdfList = []
        targetColNameList = []
        for srp in uniStartRPList:
            df_lookup_temp = df_lookup[df_lookup['startrp'] == srp]
            uniSRPdfList.append(df_lookup_temp)
            targetColName = "_".join([self.scenarios.get(self.scenario)[0], m,
                                      self.scenarios.get(self.scenario)[1], str(self.ref_year), srp, erp])
            targetColNameList.append(targetColName)

        df = pd.DataFrame(np.transpose([uniStartRPList, targetColNameList, uniSRPdfList]),
                          columns=['startrp', 'tgtCol', 'df'])
        costList = []

        for itl in np.arange(0, len(df.index), 1):
            logging.info(itl)
            df_itl = df['df'].iloc[itl]
            tgtCol_itl = df['tgtCol'].iloc[itl]
            logging.info(tgtCol_itl)
            # if '00000' in tgtCol_itl:
            #     continue
            cost_itl = pd.read_sql_query("SELECT sum({0}) FROM {1} where id in ({2})".format(tgtCol_itl, df_cost,
                                                                                             ", ".join(map(str, df_itl[
                                                                                                 'FID'].values))),
                                         self.engine).values[0]
            ####-------------------------
            # NEW CODE
            
            if user_urb != None:
                ppp_itl, con_itl = pd.read_sql_query(
                    "SELECT avg(ppp_mer_rate_2005_index) mean_1, avg(construction_cost_index*7) mean_2 FROM lookup_construction_factors_geogunit_108 where fid_aque in ({0}) ".format(
                        ', '.join(map(str, self.fids))), self.engine).values[0]
                costList.append((cost_itl * user_urb)/ ppp_itl)
            ####-------------------------
        totalCost = sum(costList)
        return totalCost

    def find_construction(self, m, exposure, user_rur=None, user_urb=None):
        """
        Purpose: Calculate the total cost to construction the desired flood protection
        Inputs:
            m = model
            user_cost = user-defined cost per km per m
        Output:
            cost = total cost of dike
        """
        #logging.debug('[CBA, find_construction]: start')
        lookup_c = pd.read_sql_query(
            "SELECT * FROM lookup_{0} where {1} = '{2}' ".format(self.geogunit, self.geogunit_type, self.geogunit_name),
            self.engine, 'id')
        lookup_c["FID"] = lookup_c.index
        lookup_c["riverine"] = self.prot_pres
        logging.info(self.prot_pres)
        lookup_c["startrp"] = lookup_c["riverine"].apply(lambda x: self.find_startrp(x))
        urb_dimensions = self.find_dimension_v2(m, lookup_c, self.df_urb_all, user_urb)
        # Find the Purchasing Power Parity to Market value rate
        # Find the local cost to construct the dike ($/km/m)
        # If the user did not input a cost, use the local cost and PPP conversion to find total cost. 7 million is a standard factor cost

        if user_urb == None:
            cost = None
        else:
            cost_urb = urb_dimensions * 1e6
            cost = cost_urb
        return cost

    def average_prot(self, m, year, risk_data_input):
        #logging.debug('[CBA, average_prot]: start')
        idx = int(year) - self.implementation_start
        #logging.debug(f'[CBA, average_prot, idx]: {idx} ==> {year} {self.implementation_start}')
        clm = "histor" if year == '2010' else self.clim
        sco = "base" if year == '2010' else self.socio
        mdl = "wt" if year == '2010' else m
        test_rps = np.linspace(min(self.rps), max(self.rps), 999)
        try:
            assert (len(risk_data_input) >= idx), f"the infrastructure lifetime ({self.infrastructure_life}) MUST be between {2080 - self.implementation_start} - {2100 - self.implementation_start}"
        except AssertionError as e:
            raise Error(message='computation failed: '+ str(e), status=400)
        real_impact = risk_data_input[int(idx)]
        # READ IN REFERENCE IMPACT
        # READ IN RAW DATA
        if real_impact == 0:
            test = np.nan
        else:
            # PULL ONLY CURRENT DATA
            cols = [col for col in sqlalchemy.Table(self.df_urb_agg, self.metadata).columns.keys() if
                    (clm.lower() in col) and (mdl.lower() in col) and (sco.lower() in col) and (year in col)]
            # impact_present = df_urb_agg.loc[geogunit_name, [col for col in df_urb_agg.columns if (clm in col) and (mdl in col) and (sco in col) and (year in col)]]
            impact_present = pd.read_sql_query(
                "SELECT {0} FROM {1} where id ='{2}'".format(', '.join(cols), self.df_urb_agg, self.geogunit_name),
                self.engine).iloc[0]
            check = 1e25
            for test in test_rps:
                test_impact = self.expected_value(impact_present, self.rps, test, 1e5)
                diff = abs(real_impact - test_impact)
                if diff > check:
                    break
                check = diff
        return test

    def risk_evolution(self, impact_cc, impact_urb, impact_pop, impact_gdp, prot, prot_idx):
        """
        Creates a time series of how annual expected impact evolves through time, assuming given protection standard at some moment in time.
        The protection standard is transformed into protection standards at other moments in time by lookup of the associated
        impact at that protection standard using a climate change only scenario.
        Input:
            years: list of years [N]
            rps: list of return periods [M] (in years)
            impact_cc: list of lists: N lists containing M impacts (for each return period) with only climate change
            impact_cc_socio: list of lists: N lists containing M impacts (for each return period) with climate and socio change
            prot: protection standard at given moment (in years)
            prot_idx: index of year (in array years) at which prot is valid.
        """
        # determine risk evaolution
        risk_prot, pop_impact, gdp_impact, prot_levels = [], [], [], []
        #logging.debug('[CBA, risk_evolution]: start')
        for year, imp_cc, imp_urb, imp_pop, imp_gdp in zip(self.years, impact_cc, impact_urb, impact_pop, impact_gdp):
            prot_trans = self.compute_rp_change(self.rps, impact_cc[prot_idx], imp_cc, prot, min_rp=2,
                                                max_rp=1000)  # i.e. RP_zero
            # compute the expected value risk with the given protection standard
            risk_prot.append(self.expected_value(imp_urb, self.rps, prot_trans, 1e5))
            pop_impact.append(self.expected_value(imp_pop, self.rps, prot_trans, 1e5))
            gdp_impact.append(self.expected_value(imp_gdp, self.rps, prot_trans, 1e5))
            # prot_levels.append(prot_trans[0])
        # Interpolate annual expected risk to get estimate for every year in time series
        # print prot_levels
        risk_func = interp1d(self.years, risk_prot, kind='linear', bounds_error=False,
                             fill_value='extrapolate')  # define interpolation function/relationship
        pop_func = interp1d(self.years, pop_impact, kind='linear', bounds_error=False,
                            fill_value='extrapolate')  # define interpolation function/relationship
        gdp_func = interp1d(self.years, gdp_impact, kind='linear', bounds_error=False,
                            fill_value='extrapolate')  # define interpolation function/relationship
        # prot_func = extrap1d(interp1d(years, prot_levels))

        annual_risk = risk_func(self.time_series)  # Run timeseries through interpolation function
        annual_pop = pop_func(self.time_series)  # Run timeseries through interpolation function
        annual_gdp = gdp_func(self.time_series)  # Run timeseries through interpolation function
        # annual_prot = prot_func(time_series)
        return annual_risk, annual_pop, annual_gdp  # , annual_prot

    def calc_impact(self, m, pt, ptid):
        """this can be improved with threads and is where the leak happens, a more ammount of fids, the runtime increases"""
        annual_risk, annual_pop, annual_gdp = 0, 0, 0
        #logging.debug('[CBA, calc_impact]: start')
        # cba_raw = pd.read_sql_query("SELECT {0} FROM {1} where id = {2} ".format(', '.join(columns), inData, inName), self.engine)
        # impact_present = pd.read_sql_query("SELECT {0} FROM {1} where id = {2} ".format(', '.join(cols), inData, inName), self.engine).values[0]
        df_urb = pd.read_sql_query(
            "SELECT * FROM {0} where id in ({1}) ".format(self.df_urb, ', '.join(map(str, self.fids))), self.engine)
        df_pop = pd.read_sql_query("SELECT * FROM {1} where id in ({2}) ".format(', '.join(
            [col for col in sqlalchemy.Table(self.df_pop, self.metadata).columns.keys() if
             (self.clim in col) and (self.socio in col) and (m in col)]), self.df_pop, ', '.join(map(str, self.fids))),
                                   self.engine)
        df_gdp = pd.read_sql_query("SELECT * FROM {1} where id in ({2}) ".format(', '.join(
            [col for col in sqlalchemy.Table(self.df_gdp, self.metadata).columns.keys() if
             (self.clim in col) and (self.socio in col) and (m in col)]), self.df_gdp, ', '.join(map(str, self.fids))),
                                   self.engine)
        # Present data = 2010 data
        # impact_present = pd.read_sql_query("SELECT {0} FROM {1} where id = {2} ".format(', '.join(cols), inData, inName), self.engine).values[0]
        for f in self.fids:
            impact_cc = self.select_impact(m, df_urb, f, "base")
            impact_urb = self.select_impact(m, df_urb, f, self.socio)
            impact_pop = self.select_impact(m, df_pop, f, self.socio)
            impact_gdp = self.select_impact(m, df_gdp, f, self.socio)
            f_risk, f_pop, f_gdp = self.risk_evolution(impact_cc, impact_urb, impact_pop, impact_gdp, pt, ptid)
            annual_risk = annual_risk + f_risk
            annual_pop = annual_pop + f_pop
            annual_gdp = annual_gdp + f_gdp
        return annual_risk, annual_pop, annual_gdp

    def precalc_present_benefits(self, model):
        """
        Inputs:
            prot_start_unit = present_day protection standard (assumed to beong to 0th year in list of years)
        Output:
            time series of costs and benefits with discount rate included

        """
        # Find total costs without discount(over timeseries)
        # DEFAULT DATA
        #logging.debug('[CBA, precalc_present_benefits]: start')
        urb_imp = self.filt_risk[
            [col for col in self.filt_risk.columns if ("urban_damage" in col) and (self.scen_abb.lower() in col)
             and (model.lower() in col) and ("tot" in col)]].sum(axis=0)
        pop_imp = self.filt_risk[
            [col for col in self.filt_risk.columns if ("popexp" in col) and (self.scen_abb.lower() in col)
             and (model.lower() in col) and ("tot" in col)]].sum(axis=0)
        gdp_imp = self.filt_risk[
            [col for col in self.filt_risk.columns if ("gdpexp" in col) and (self.scen_abb.lower() in col)
             and (model.lower() in col) and ("tot" in col)]].sum(axis=0)
        prot_imp = self.df_prot.loc[self.geogunit_name, [col for col in self.df_prot.columns if
                                                        ("urban_damage" in col) and (self.scen_abb.lower() in col)
                                                        and ("prot_avg" in col)]]

        risk_func = interp1d(self.years, urb_imp, kind='linear', bounds_error=False,
                             fill_value='extrapolate')  # define interpolation function/relationship
        pop_func = interp1d(self.years, pop_imp, kind='linear', bounds_error=False,
                            fill_value='extrapolate')  # define interpolation function/relationship
        gdp_func = interp1d(self.years, gdp_imp, kind='linear', bounds_error=False,
                            fill_value='extrapolate')  # define interpolation function/relationship
        prot_func = interp1d(self.years, prot_imp, kind='linear', bounds_error=False,
                             fill_value='extrapolate')  # define interpolation function/relationship

        annual_risk = risk_func(self.time_series)  # Run timeseries through interpolation function
        annual_pop = pop_func(self.time_series)  # Run timeseries through interpolation function
        annual_gdp = gdp_func(self.time_series)  # Run timeseries through interpolation function
        annual_prot = prot_func(self.time_series)  # Run timeseries through interpolation function

        return annual_risk, annual_pop, annual_gdp, annual_prot

    def select_impact(self, m, inData, inName, socioecon):
        """
        Purpose: Pull raw data needed to perfrom CBA
        Inputs:
            m = climate model
            inData = dataset to filter through
            socioecono = socioeconomic data
        Output:
            List of dataframes for each year with impact estimates. Impact data for climate change only scenario and total change

        Time is being killed here on big selections
        """
        #logging.debug('[CBA, select_impact]: start')
        cba_raw = inData.set_index('id').loc[inName]
        # Present data = 2010 data
        impact_present = cba_raw.filter(like="_2010_", axis=0).values
        # For all years, pull the climate change only data (base signifies no socioeconomic pathway used)
        i_2030 = cba_raw.filter(like='2030', axis=0).filter(like=self.clim, axis=0).filter(like=socioecon,
                                                                                           axis=0).filter(like=m,
                                                                                                          axis=0).values
        i_2050 = cba_raw.filter(like='2050', axis=0).filter(like=self.clim, axis=0).filter(like=socioecon,
                                                                                           axis=0).filter(like=m,
                                                                                                          axis=0).values
        i_2080 = cba_raw.filter(like='2080', axis=0).filter(like=self.clim, axis=0).filter(like=socioecon,
                                                                                           axis=0).filter(like=m,
                                                                                                          axis=0).values
        impact = [impact_present, i_2030, i_2050, i_2080]

        return impact

    # @cached_property
    def analyze(self):
        # allStartTime = time.time()
        ##--------------------------------------------------------
        ###              ANALYSIS          ###
        ##--------------------------------------------------------
        logging.debug( "Analysis starting...")
        # IMPACT DATA BY MODEL

        # model_benefits = pd.DataFrame(index=time_series)
        try:
            model_benefits = pd.DataFrame(data=self.time_series, columns=['year']).set_index('year')
            # IMPACT DATA BY MODEL

            for m in self.mods:
                # start_time = time.time()
                logging.debug( "------------------   Model %s starting...  ---------------" %m)

                if self.risk_analysis == "precalc":
                    logging.debug( "------------------   precalc  ---------------" )
                    annual_risk_pres, annual_pop_pres, annual_gdp_pres, annual_prot_pres = self.precalc_present_benefits(
                        m)

                else:
                    logging.debug( "------------------   Calc  ---------------")
                    annual_risk_pres, annual_pop_pres, annual_gdp_pres = self.calc_impact(m, self.prot_pres, 0)
                    prot_pres_list = []
                    for y in self.ys:
                        prot_pres_list.append(self.average_prot(m, y, annual_risk_pres))
                    prot_func_pres = self.extrap1d(interp1d(self.years, prot_pres_list))
                    annual_prot_pres = prot_func_pres(self.time_series)  # Run timeseries through interpolation function
                # logging.debug( m, "present done", time.time() - start_time)

                # start_time = time.time()
                # sTimetl = time.time()
                logging.debug( "------------------   CALC2  ---------------" )
                annual_risk_fut, annual_pop_fut, annual_gdp_fut = self.calc_impact(m, self.prot_futu, self.prot_idx_fut)
                logging.debug( "------------------   CALC3  ---------------" )
                logging.debug(f'[CBA, {m}]')
                logging.debug(f'[CBA, {self.ys}]')
                logging.debug(f'[CBA, {annual_risk_fut}]')
                prot_fut_list = [self.average_prot(m, y, annual_risk_fut) for y in self.ys]
                logging.debug( "------------------   CALC4  ---------------" )
                prot_func_fut = self.extrap1d(interp1d(self.years, prot_fut_list))
                logging.debug( "------------------   CALC5  ---------------" )
                annual_prot_fut = prot_func_fut(self.time_series)  # Run timeseries through interpolation function

                # logging.debug( m, "future  done", time.time() - start_time)

                # start_time = time.time()
                df = self.compute_benefits(m, annual_risk_pres, annual_risk_fut, annual_pop_pres, annual_pop_fut,
                                           annual_gdp_pres, annual_gdp_fut, annual_prot_pres, annual_prot_fut)
                model_benefits = model_benefits.join(df)
                logging.debug( "------------------   CALC6  ---------------" )
                # logging.debug( m, "benefits done", time.time()-start_time )

                # start_time = time.time()

                pop_costs = self.find_construction(m, "POPexp", self.user_rur_cost, self.user_urb_cost)
                gdp_costs = self.find_construction(m, "Urban_Damage_v2", self.user_rur_cost, self.user_urb_cost)
                logging.debug( "------------------   CALC7  ---------------" )
                df_pc = self.compute_costs(m, pop_costs, "POP")
                model_benefits = model_benefits.join(df_pc)
                df_gc = self.compute_costs(m, gdp_costs, "GDP")
                model_benefits = model_benefits.join(df_gc)
                #logging.debug(m, "costs done", time.time()-start_time)
                #logging.debug("Model %s done..." % m)

            # start_time = time.time()
            df_final = self.run_stats(model_benefits)
            # check benefits or cost is 0
            if df_final.urb_benefits_avg.sum() == 0:
                df_final[[x for x in df_final.columns if "costs" in x]] = 0
            elif df_final.gdp_costs_avg.sum() == 0:
                df_final[[x for x in df_final.columns if "Benefits" in x]] = 0


            ### DETAILS
            details = {"geogunitName": self.geogunit_name,
                       "geogunitType": self.geogunit_type,
                       "scenario": self.scenario,
                       "averageProtection": self.prot_pres,
                       "startingProtection": min(self.rps, key=lambda rp: abs(rp - self.prot_pres)),
                       "futureProtection": self.prot_futu,
                       "referenceYear": self.ref_year,
                       "implementionStart": self.implementation_start,
                       "implementionEnd": self.implementation_end,
                       "infrastructureLifespan": self.infrastructure_life,
                       "estimatedCosts": self.estimated_costs,
                       "benefitsStart": self.benefits_start,
                       "discount": self.discount_rate,
                       "om": self.om_costs,
                       "gdpCosts": gdp_costs.tolist()}
            # df_final = model_benefits
            # print( "All done! Total computation time:", time.time() - allStartTime)
            return {
            "meta": details,
            "df": df_final
        }
        except Error as e:
            logging.error('[CBA analyze]: ' + str(e))
            raise e
        except Exception as e:
            logging.error('[CBA analyze]: ' + str(e))
            raise Error(message='computation failed: '+ str(e))
        finally:
            self.engine.dispose()


class CBAICache(object):
    """
    this will have the next methods:
        * create cache table (only if the table doesn't exist)
        * check if a certain set of parameters exists on the table, if exists it will retrive cbaService data from the row selected
        * if not it will trigger the CBAService class to calculate it.
    """

    ### DBConexion
    def __init__(self, params):
        self.engine = sqlalchemy.create_engine(os.getenv('POSTGRES_URL'))
        self.metadata = sqlalchemy.MetaData(bind=self.engine, reflect=True)
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
            myCache = sqlalchemy.Table("cache_cba", self.metadata,
                                       Column('id', Integer, primary_key=True, unique=True),
                                       Column('key', Text, unique=True, index=True),
                                       Column('value', JSON),
                                       Column('last_updated', DateTime, default=datetime.datetime.now,
                                              onupdate=datetime.datetime.now)
                                       )
            myCache.create()
        except Exception as e:
            logging.error('[CBAICache, _createTable]: ' + str(e))
            raise Error(message='cache table creation failed'+ str(e))
        return myCache

    def checkParams(self):
        try:
            table = self.metadata.tables['cache_cba']

            logging.info('[CBAICache, checkParams]: checking params...')
            # logging.info(self._generateKey)
            select_st = table.select().where(table.c.key == self._generateKey)
            res = self.engine.connect().execute(select_st).fetchone()
            logging.info(f'[CBAICache, checkParams - result]: {res}')
            return res
        except Exception as e:
            logging.error('[CBAICache, checkParams]: ' + str(e))
            raise Error(message='Checked params failed' + str(e))

    def insertRecord(self, key, data):
        # insert data via insert() construct
        try:
            table = self.metadata.tables['cache_cba']
            ins = table.insert().values(
                key=key,
                value=data)
            conn = self.engine.connect()
            conn.execute(ins)

            return 200

        except Exception as e:
            logging.error('[CBAICache, insertRecord]: ' + str(e))
            raise Error(message='insert record in cache table failed. \n'+ str(e))

    def updateRecord(self):
        return 0

    def cleanCache(self):
        try:
            table_drop = self.metadata.tables['cache_cba'].delete()
            conn = self.engine.connect()
            conn.execute(table_drop)
            return 200
        except Exception as e:
            logging.error('[CBAICache, cleanCache table]: ' + str(e))
            raise Error(message='Cache table drop has failed. \n'+ str(e))

    def execute(self):
        try:
            inspector = sqlalchemy.inspect(self.engine)
            logging.info('[CBAICache]: Getting cba default...')
            if 'cache_cba' in inspector.get_table_names():
                # It means we have the cache table, we will need to check the params
                checks = self.checkParams()
                if checks != None:
                    logging.info('[CBAICache]: table available; extracting data')
                    data = json.loads(checks[2])
                    logging.info(data.keys())
                    return {'meta': data['meta'], 'df': pd.DataFrame(data['data']).set_index(
                        'year')}  # we will give back the data in a way CBAEndService can use it

                else:  # we will execute the whole process and we will generate the output in a way  CBAEndService can use it
                    logging.info('[CBAICache]: data not available; generating data')
                    data_output = CBAService(self.params).analyze()
                    data = json.dumps(
                        {'meta': data_output['meta'], 'data': data_output['df'].reset_index().to_dict('records')},
                        ignore_nan=True)
                    key = self._generateKey
                    self.insertRecord(key, data)
                    return data_output  # we will give back the data in a way CBAEndService can use it
            else:
                self._createTable()
                self.execute()
                # executes the cba code to get the table, inserts it into the database and we should be ready to go
        except Exception as e:
            logging.error('[CBAICache, execute]: ')
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
            logging.error(repr(traceback.format_exception(exc_type, exc_value,
                                          exc_traceback)))
            raise e


class CBAEndService(object):
    def __init__(self, user_selections):
        # self.data = CBAService(user_selections).analyze()
        self.data = CBAICache(user_selections).execute()

    def get_widget(self, argument):
        method_name = 'widget_' + str(argument)
        method = getattr(self, method_name, lambda: "Widget not found")
        return method()

    # @cached_property
    def widget_table(self):
        fOutput = self.data['df'][['urb_benefits_avg', 'gdp_costs_avg', 'pop_benefits_avg', 'gdp_benefits_avg']]
        cumOut = fOutput.sum()

        # npv = None
        avoidedGdp = round(fOutput.loc[self.data['meta']['benefitsStart']:].gdp_benefits_avg.sum())
        avoidedPop = round(fOutput.loc[self.data['meta']['benefitsStart']:].pop_benefits_avg.sum())
        bcr = round((cumOut['gdp_costs_avg'] / cumOut['urb_benefits_avg']), 5   )

        return {'widgetId': 'table', 'chart_type': 'table', 'meta': self.data['meta'],
                'data': [{'bcr': bcr, 'avoidedPop': avoidedPop, 'avoidedGdp': avoidedGdp}]}

    # @cached_property
    def widget_annual_costs(self):
        """Urb_Benefits_avg / GDP_Costs_avg"""
        self.data['meta']["yAxisTitle"] = 'Cost and Benefits ($)'
        return {'widgetId': 'annual_costs', 'chart_type': 'multi-line', 'meta': self.data['meta'], 'data': pd.melt(
            self.data['df'].reset_index()[['year', 'urb_benefits_avg', 'gdp_costs_avg']].rename(index=str, columns={
                "urb_benefits_avg": "Benefits", "gdp_costs_avg": "Costs"}), id_vars=['year'],
            value_vars=['Benefits', 'Costs'], var_name='c', value_name='value').to_dict('records')}

    # @cached_property
    def widget_net_benefits(self):
        """Urb_Benefits_avg / GDP_Costs_avg --> net cummulative costs"""
        self.data['meta']["yAxisTitle"] = 'Cumulative Net Benefits ($)'
        fOutput = self.data['df'][['urb_benefits_avg', 'gdp_costs_avg']].cumsum()
        fOutput['value'] = fOutput['urb_benefits_avg'] - fOutput['gdp_costs_avg']

        return {'widgetId': 'net_benefits', 'chart_type': 'bar', 'meta': self.data['meta'],
                'data': fOutput.reset_index()[['year', 'value']].to_dict('records')}

    # @cached_property
    def widget_impl_cost(self):
        """GDP_Costs_avg"""
        self.data['meta']["yAxisTitle"] = 'Implementation Cost ($)'
        fOutput = self.data['df'].reset_index()[['year','gdp_costs_avg']]
        minY = fOutput['year'].min() - 1
        fOutput['value'] = (fOutput['gdp_costs_avg'] * (1 + self.data['meta']['discount']) ** (
                fOutput['year'] - minY)) / 10.1
        fOutput.loc[fOutput['year'] >= self.data['meta']['implementionEnd'], 'value'] = 0

        return {'widgetId': 'impl_cost', 'chart_type': 'bar', 'meta': self.data['meta'],
                'data': fOutput[['year', 'value']].to_dict('records')}

    # @cached_property
    def widget_mainteinance(self):
        self.data['meta']["yAxisTitle"] = 'Operation & Mainteinance Cost($)'

        fOutput = self.data['df'][['gdp_costs_avg']]

        impE = self.data['meta']['implementionEnd']
        impS = self.data['meta']['implementionStart']
        life = self.data['meta']['infrastructureLifespan']
        ## Review this to make it work
        cost = fOutput.loc[self.data['meta']['implementionEnd']]['gdp_costs_avg'] * ((1 + self.data['meta']['discount']) ** (impE - impS))
        om_costs = self.data['meta']['om']
        build_years = impE - impS
        years = list(range(impS, impS + life +1))
        mp = [1.0 / build_years * cost for i in range(build_years)]
        cum_costs_present = np.cumsum(mp)
        mains = cum_costs_present * om_costs

        maintenance = pd.Series(np.concatenate((mains, [mains[-1]] * (impS + life - impE + 1))), index=years)
        fOutput.insert(1,'costs', maintenance)

        result = fOutput.reset_index()
        result['value'] = result['costs'] / ((1 + self.data['meta']['discount']) ** (result['year'] - impS + 1))

        return {'widgetId': 'mainteinance', 'chart_type': 'bar', 'meta': self.data['meta'],
                'data': result[['year', 'value']].to_dict('records')}

    # @cached_property
    def widget_flood_prot(self):
        self.data['meta']["yAxisTitle"] = 'Protection level (Return period)'

        fOutput = self.data['df'].reset_index()[['year', 'gdp_costs_avg', 'prot_present_avg', 'prot_future_avg']]
        fn = lambda row: row.prot_present_avg if row.year <= self.data['meta'][
            "benefitsStart"] else row.prot_future_avg if row.year >= self.data['meta']["implementionEnd"] else None
        fOutput['value'] = fOutput.apply(fn, axis=1)

        return {'widgetId': 'flood_prot', 'chart_type': 'line', 'meta': self.data['meta'],
                'data': fOutput[['year', 'value']].to_dict('records')}

    # @cached_property
    def widget_export(self):
        return {'widgetId': '', 'meta': self.data['meta'], 'data': self.data['df'].reset_index().to_dict('records')}
