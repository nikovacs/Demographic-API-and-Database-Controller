"""
Parent program for the API_DB_Mediator.
Use from command line (or .bat) to perform actions at scheduled times

Author: Nikolas Kovacs
"""
from api_db_mediator import API_DB_Mediator
import argparse

def main():
    db = API_DB_Mediator()

    table_args = {
        "initalize_all": [db.initialize_db],
        "states": [db._API_DB_Mediator__init_states_table],
        "counties": [db._API_DB_Mediator__init_counties_table],
        "county_unemployment": [db._API_DB_Mediator__init_county_unemployment_table],
        "state_unemployment": [db._API_DB_Mediator__init_state_unemployment_table],
        "county_workers": [db._API_DB_Mediator__init_county_workers_table],
        "county_employment": [db._API_DB_Mediator__init_employment_table, "COUNTY"],
        "state_employment": [db._API_DB_Mediator__init_employment_table, "STATE"],
        "us_employment": [db._API_DB_Mediator__init_employment_table, "US"],
        "county_data": [db._API_DB_Mediator__init_census_county_data_table],
        "state_data": [db._API_DB_Mediator__init_census_state_data_table],
        "county_poverty": [db._API_DB_Mediator__init_census_county_poverty_table],
        "state_poverty": [db._API_DB_Mediator__init_census_state_poverty_table],
        "school_districts": [db._API_DB_Mediator__init_census_school_districts_table],
        "zipcodes": [db._API_DB_Mediator__init_zipcodes_table],
        "county_gdp": [db._API_DB_Mediator__init_gdp_table, "COUNTY"],
        "state_gdp": [db._API_DB_Mediator__init_gdp_table, "STATE"],
    }

    parser = argparse.ArgumentParser(description="CLI for updating tables in the Demographic Database")
    parser.add_argument("table", type=str, choices=table_args.keys(), help="Enter a table to update")
    args = parser.parse_args()
    table_to_update = args.table
    
    method_and_args = table_args[table_to_update]
    if len(method_and_args) == 1: # method takes no args
        method_and_args[0]()
    else:
        method_and_args[0](method_and_args[1])


if __name__ == "__main__":
    main()