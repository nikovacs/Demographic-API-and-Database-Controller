"""
This file contains functions necessary for gathering data from the bea API
Specifically:
    State GDP
    County GDP

Author: Nikolas Kovacs
"""
import json
import requests
import pandas as pd

def get_bea_user_id():
    with open("request_info.json", 'r') as f:
        request_info = json.load(f)
        return request_info['keys']['bea_user_id']

def get_gdp_data(for_):
    """
    This function gets the last 5 years of GDP data for either the states or counties as according
    to the tables and linecodes as specified in request_info.json

    @param for_: "STATE" or "COUNTY"
    """
    if not isinstance(for_, str):
        raise TypeError("for_ must be a string")
    for_ = for_.upper()
    if for_ not in ["STATE", "COUNTY"]:
        raise ValueError("for_ must be either 'STATE' or 'COUNTY'")

    user_id = get_bea_user_id()
    tables_linecodes = get_bea_tables_and_linecodes_combined()

    for table_linecode in tables_linecodes:
        url_table, url_line_code = table_linecode.split('_')
        url = f"https://apps.bea.gov/api/data/?&UserID={user_id}&method=GetData&datasetname=Regional&TableName={url_table}&LineCode={url_line_code}&GeoFIPS={for_}&Year=LAST5"
        response = requests.get(url)
        if response.status_code == 200:
            response = response.json()
            for data in response["BEAAPI"]["Results"]["Data"]:
                yield data


def get_bea_tables_and_linecodes_combined():
    tables, line_codes = get_bea_tables_and_linecodes()
    tables_linecodes = []
    for table, line_code_lst in zip(tables, line_codes):
        for table_, l_c in zip(table*len(line_code_lst), line_code_lst):
            tables_linecodes.append(f"{table_}_{l_c}")
    return tables_linecodes

def get_bea_tables_and_linecodes():
    with open("request_info.json", 'r') as f:
        config = json.load(f)
        tables = config["tables"]["bea_gdp"]["tables"]
        line_codes = config["tables"]["bea_gdp"]["line_codes"]
    return tables, line_codes


    
