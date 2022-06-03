"""
This file contains the function necessary for retrieving the time series data from the census API.
Specfically:
    County Data
    State Data
    County Poverty Data
    State Poverty Data
    School Districts Data
    ZipCodes Data

Author: Nikolas Kovacs
"""
import json
import requests

def get_census_timeseries(for_, type_, start_year, end_year):
    """
    This function requests time series data from the census API.

    @param for_: The type of data to be retrieved. ("States", "Counties")
    @param type_: The type of data to be retrieved. ("Poverty", None other at the moment)
    @param start_year: The start year for the data
    @param end_year: The end year for the data

    @return: A list (json) containing the requested data
    """
    if for_.lower() != "counties" and for_.lower() != "states":
        raise ValueError("for_ must be either 'states' or 'counties'")
    if type_.lower() != "poverty":
        raise ValueError("type_ must be poverty (None other added at the moment)")

    in_keyword = ""
    if for_.lower() == "states":
        for_ = "state"
    else:
        for_ = "county"
        in_keyword = get_in_keyword_val_for_states()

    with open("request_info.json", 'r') as f:
        request_info = json.load(f)
        tables = request_info['tables']["poverty_tables"]
        tables = ','.join([x.split(',')[0] for x in tables])
    
    output = []
    for year in range(start_year, end_year+1):
        api_url = f"https://api.census.gov/data/timeseries/poverty/saipe?get={tables},YEAR,NAME&for={for_}:*&time={year}{in_keyword}&key={get_census_key()}"
        response = requests.get(api_url)
        if response.status_code == 200:
            output.append(response.json())
    return output

def get_census_data(for_, start_year, end_year):
    """
    This method requests the non-timeseries data from the census API.

    @param for_: The type of data to be retrieved. ("states", "counties", "zipcodes", "school_districts")
    @param start_year: The start year for the data
    @param end_year: The end year for the data

    @return: A list(json) containing the requested data
    """
    # set up for_ and in_keyword variables
    in_keyword = ""
    if for_.lower() == "states":
        for_ = "state"
    elif for_.lower() == "counties":
        for_ = "county"
        in_keyword = get_in_keyword_val_for_states()
    elif for_.lower() == "school_districts":
        for_ = r"school%20district%20(unified)"
        in_keyword = get_in_keyword_val_for_states()
    elif for_.lower() == "zipcodes":
        for_ = "zip code tabulation area"
        in_keyword = get_in_keyword_val_for_states()
    else:
        raise ValueError("for_ must be either 'states' or 'counties' or 'school_districts' or 'zipcodes'")

    output = []
    for year in range(start_year, end_year + 1):
        api_url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get={get_appropriate_tables(for_)},NAME&for={for_}:*{in_keyword}&key={get_census_key()}"
        response = requests.get(api_url)
        if response.status_code == 200:
            response = response.json()
            # add year to the data
            response[0].append("year")
            for i in range(1, len(response)):
                response[i].append(year)
            output.append(response)
    return output
    

def get_appropriate_tables(for_):
    """
    This function retrieves the appropate tables for the api call

    @param for_: The type of data to be retrieved. Simply pass in caller for_ variable.

    @return: A string of tables to be used in the api call
    """
    with open('request_info.json', 'r') as f:
        request_info = json.load(f)
        if for_ == "state" or for_ == "county":
            tables = request_info['tables']["census_data_tables"]
        elif for_ == r"school%20district%20(unified)":
            tables = request_info['tables']["school_districts_tables"]
        else:
            tables = request_info['tables']["zipcode_tables"]

    # convert tables from list of str to str with each element separated by comma
    return ",".join([x.split(',')[0] for x in tables])


def get_census_key():
    """
    This function retrieves the census api key from the request_info.json file.
    """
    with open('request_info.json', 'r') as f:
        request_info = json.load(f)
        census_key = request_info['keys']['census_key']
    return census_key

def get_in_keyword_val_for_states():
        return "&in=state:*"
