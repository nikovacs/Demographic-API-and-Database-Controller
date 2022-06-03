"""
This file contains the functions necessary for retrieving the time series data from the BLS API.
Specfically:
    State Unemployment Rate
    County Unemployment Rate
    County Workers Data

NOTE: The BLS API cannot request more than 50 series at a time. 

Author: Nikolas Kovacs
"""

from urllib.error import HTTPError
import requests
import json
import pandas as pd

def get_bls_key():
    """
    This function retrieves the bls_key from the keys.json file.
    The key is necessary for executing the queries.
    """
    with open('request_info.json') as f:
        request_info = json.load(f)
        bls_key = request_info['keys']['bls_key']
    return bls_key


def get_unemployment_data(state_codes=None, county_codes=None, start_year=None, end_year=None) -> dict:
    """
    This function retrieves the unemployment data for the specified states or counties.

    NOTE: If both state and county codes are provided, it will be assumed that county unemployment is being requested

    @param state_codes: A list of state codes
    @param county_codes: A list of county codes
    @param start_year: The start year for the unemployment data
    @param end_year: The end year for the unemployment data

    @return: list of json objects containing the unemployment data for the specified states or counties
    """
    if state_codes is None or len(state_codes) == 0:
        raise ValueError("state_codes argument must be specified as list")
    
    if start_year is None:
        raise ValueError("start_year argument must be specified")
    
    if end_year is None:
        raise ValueError("end_year argument must be specified")

    suffix = "CN"
    if county_codes is None:
        suffix = "ST"
    elif len(state_codes) > 1 or len(state_codes) == 0:
        raise ValueError("If county_codes is specified, state_codes must be a list of length 1")
    if len(state_codes) > 50 or (county_codes is not None and len(county_codes) > 50):
        raise ValueError("No more than 50 series can be requested at a time")

    def generate_state_county_codes(state_codes, county_codes) -> list:
        series_type = f"LAU{suffix}"
        if county_codes is None:
            county_codes = ["000"] * len(state_codes)
        if len(state_codes) < len(county_codes):
            state_codes = state_codes * len(county_codes)
        
        codes = []
        for st_code, cn_code in zip(state_codes, county_codes):
            codes.append(f"{series_type}{st_code}{cn_code}0000000003")
        return codes
    
    return requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
    json={
        "seriesid":generate_state_county_codes(state_codes, county_codes), 
        "startyear":f"{start_year}", "endyear":f"{end_year}",
        "catalog":False, "calculations":False, "annualaverage":False,"aspects":False,
        "registrationkey":get_bls_key()
        }
    ).json()

    

def get_county_workers(state_codes, county_codes, start_year, end_year):
    """
    This function retrieves the worker data for a county

    @param state_code: The state codse for the states of interest
    @param county_code: The county codes for the counties of interest
    @param start_year: The start year for the worker data
    @param end_year: The end year for the worker data

    @return: A dictionary(JSON) containing the worker data for a county
    """
    if len(state_codes) > 1:
        raise ValueError("Only one state code at a time can be specified in a list")
    
    state_codes = state_codes * len(county_codes)

    def generate_state_county_codes_for_workers(state_codes, county_codes) -> list:
        """
        This function generates the state and county codes for unemployment data

        @param state_codes: A list of state codes
        @param county_codes: A list of county codes

        @return  A list of state and county codes for unemployment data in proper LAUST#####0000000006 format 
        """
        state_county_codes_for_workers = []
        for state_code, county_code in zip(state_codes, county_codes):
            state_county_codes_for_workers.append(f"LAUCN{state_code}{county_code}0000000006")
        return state_county_codes_for_workers

    return requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
        json={
            "seriesid":generate_state_county_codes_for_workers(state_codes, county_codes), 
            "startyear":f"{start_year}", "endyear":f"{end_year}",
            "catalog":False, "calculations":False, "annualaverage":False,"aspects":False,
            "registrationkey":get_bls_key()
            }
        ).json()

def get_employment_data(for_, start_year, end_year, state_codes=None, county_codes_list=None):
    """
    This generator retrieves the data for the specified years and for the specified for.
    @param `for_` argument can be:
        'US'
        'States'
        'Counties'
    @param start_year: The start year for the unemployment data
    @param end_year: The end year for the unemployment data
    @param state_codes: The state codes for the states of interest
    @param county_codes_list: list of lists of county codes corresponding with the state codes

    @return: A dataframe containing the data for the specified years and for the specified for.
    """
    files = []
    if for_.lower() == "us":
        files.append("US000")
    elif for_.lower() == "states":
        if state_codes is None:
            raise ValueError("state_codes argument must be specified as list when for_=states")
        for code in state_codes:
            files.append(f"{code}000")
    elif for_.lower() == "counties":
        if not state_codes or not county_codes_list or len(state_codes) != len(county_codes_list):
            raise ValueError("state_codes and county_codes must be lists of the same length when for_=counties")
        for state_code, county_codes in zip(state_codes, county_codes_list):
            for county_code in county_codes:
                files.append(f"{state_code}{county_code}")
    else:
        raise ValueError("for_ argument must be one of: 'US', 'State', 'County'")

    for file in files:
        for year in range(start_year, end_year + 1):
            for qtr in range(1,5):
                print(f"Year: {year}, Quarter: {qtr}, File: {file}")
                try:
                    output = pd.read_csv(f"http://data.bls.gov/cew/data/api/{year}/{qtr}/area/{file}.csv")
                except HTTPError:
                    continue
                yield output
    



