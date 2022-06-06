"""
This class serves as a bridge between the database and the APIs
"""

import pyodbc
from bls_data import get_unemployment_data, get_county_workers, get_employment_data
from census_data import get_census_timeseries, get_census_data
from bea_data import get_gdp_data, get_bea_tables_and_linecodes_combined
import json
import numpy as np
from datetime import datetime as dt
from datetime import date

class API_DB_Mediator:
    def __init__(self):
        # initalize connection to database
        with open("config.json", 'r') as f:
            config = json.load(f)
            server = config["server"]
            database = config["database"]
            username = config["username"]
            password = config["password"]
        self.__connection = pyodbc.connect('Driver={SQL Server};' + f'Server={server};Database={database};UID={username};PWD={password}')
        self.__connection.autocommit = True
        self.__cursor = self.__connection.cursor()
        self.__cursor.fast_executemany = True

        self.__insert_into_state_unemployment_skeleton = """
                        INSERT INTO dbo.state_unemployment_rate (state, year, period, value)
                        VALUES ('{st}', {yr}, '{prd}', {val});
                    """
        self.__insert_into_county_unemployment_skeleton = """
                        INSERT INTO dbo.county_unemployment_rate (state, county, year, period, value)
                        VALUES ('{st}', '{cnty}', {yr}, '{prd}', {val});
                    """
        self.__insert_into_county_workers_skeleton = """
                        INSERT INTO dbo.county_workers (state, county, year, period, value)
                        VALUES ('{st}', '{cnty}', {yr}, '{prd}', {val});
                    """


    def initialize_db(self):
        with open(f"db_logging_{date.today()}.txt", 'w') as f:
            print(f"Initializing database...({dt.now()})", file=f)
            print(" - ")
            print(f"Initializing states table...({dt.now()})", file=f)
            self.__init_states_table()
            print(f"Initializing counties table...({dt.now()})", file=f)
            self.__init_counties_table()

            print(f"Initializing state unemployment table...({dt.now()})", file=f)
            self.__init_state_unemployment_table()
            print(f"Initializing county unemployment table...({dt.now()})", file=f)
            self.__init_county_unemployment_table()
            print(f"Initializing county workers table...({dt.now()})", file=f)
            self.__init_county_workers_table()

            print(f"Initializing US employment table...({dt.now()})", file=f)
            self.__init_employment_table("us")
            print(f"Initializing state employment table...({dt.now()})", file=f)
            self.__init_employment_table("state")
            print(f"Initializing county employment table...({dt.now()})", file=f)
            self.__init_employment_table("state")

            print(f"Initializing census state data table...({dt.now()})", file=f)
            self.__init_census_state_data_table()
            print(f"Initializing census county data table...({dt.now()})", file=f)
            self.__init_census_county_data_table()

            print(f"Initializing state poverty table...({dt.now()})", file=f)
            self.__init_census_state_poverty_table()
            print(f"Initializing county poverty table...({dt.now()})", file=f)
            self.__init_census_county_poverty_table()

            print(f"Initializing school district table...({dt.now()})", file=f)
            self.__init_census_school_districts_table()
            print(f"Initializing zipcodes table...({dt.now()})", file=f)
            self.__init_zipcodes_table()

            print(f"Initializing state GDP table...({dt.now()})", file=f)
            self.__init_gdp_table("state")
            print(f"Initializing county GDP table...({dt.now()})", file=f)
            self.__init_gdp_table("county")

            print(f"Initialization complete.({dt.now()})", file=f)


    def __init_gdp_table(self, for_):
        """
        Uses the information for the BEA API provided in request_info.json to initalize the gdp table for either states or counties
        @param for_: "STATE" or "COUNTY"
        """
        if not isinstance(for_, str) or for_.upper() not in ["STATE", "COUNTY"]:
            raise TypeError("for_ must be str \"STATE\" or \"COUNTY\"")
        
        # Because this method is used both for state and county gdp data, the sql statements must work with both,
        # hence these if_county* variables
        if_county_create = "county VARCHAR(3) NOT NULL,"
        if_county_compare = "county = '{}' AND "
        if_county_column = "county, "
        if_county = "'{}',"
        if for_.upper() == "STATE":
            if_county_create, if_county_compare, if_county_column, if_county = "", "", "", ""
            
        for_ = for_.lower()

        tables_linecodes = get_bea_tables_and_linecodes_combined()

        # make gdp_table_description table
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.gdp_table_description') IS NOT NULL
                DROP TABLE dbo.gdp_table_description;
            CREATE TABLE dbo.gdp_table_description (
                table_linecode VARCHAR(20) NOT NULL,
                cl_unit VARCHAR(100),
                unit_mult smallint,
                PRIMARY KEY (table_linecode)
            );
        """)

        # make gdp table (county or state depending on for_)
        tables_linecodes_for_table_creation = " ".join([f"{x} BIGINT," for x in tables_linecodes])
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.{for_}_gdp') IS NOT NULL
                DROP TABLE dbo.{for_}_gdp;
            CREATE TABLE dbo.{for_}_gdp (
                state CHAR(2) NOT NULL,
                {if_county_create}
                year smallint NOT NULL,
                {tables_linecodes_for_table_creation}
                PRIMARY KEY (state, {if_county_column}year),
            );
        """)

        for values in get_gdp_data(for_):
            table_linecode = '_'.join(values["Code"].split('-'))
            cl_unit, unit_mult = values["CL_UNIT"], values["UNIT_MULT"]
            state, county, year = values["GeoFips"][:2], values["GeoFips"][2:], values["TimePeriod"]

            value = ''.join(values["DataValue"].split(','))
            value = "NULL" if not value.isdigit() else value

            # insert the data into the tables if and only if the sample does not already exist.
            self.__cursor.execute(f"""
                IF NOT EXISTS (SELECT TOP 1 * FROM dbo.gdp_table_description WHERE table_linecode = '{table_linecode}')
                BEGIN
                    INSERT INTO dbo.gdp_table_description (table_linecode, cl_unit, unit_mult)
                    VALUES ('{table_linecode}', '{cl_unit}', {unit_mult});
                END;
            """)

            self.__cursor.execute(f"""
                IF EXISTS (SELECT TOP 1 * from dbo.{for_}_gdp WHERE {if_county_compare.format(county)}state = '{state}' AND year = {year})
                begin
                    UPDATE dbo.{for_}_gdp SET {table_linecode} = {value} WHERE {if_county_compare.format(county)}state = '{state}' AND year = {year};
                end
                ELSE
                begin
                    INSERT INTO dbo.{for_}_gdp (state, {if_county_column}year, {table_linecode})
                    VALUES ('{state}', {if_county.format(county)}{year}, {value});
                end
            """)


    def __init_zipcodes_table(self):
        tables = self.__get_census_tables("zipcode_tables")
        # make table named census_zipcodes
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.census_zipcodes', 'U') IS NOT NULL
                DROP TABLE dbo.census_zipcodes;
            CREATE TABLE dbo.census_zipcodes (
                state varchar(2) NOT NULL,
                zipcode_tab_area char(5) NOT NULL,
                year int NOT NULL,
                name varchar(12),
                {tables},
                PRIMARY KEY (state, zipcode_tab_area, year)
            );
            """)

        curr_year = self.__get_curr_year()
        data = get_census_data("zipcodes", curr_year-3, curr_year)
        tables = self.__prepare_census_tables_for_query(tables)

        self.__cursor.executemany(f"""
            INSERT INTO dbo.census_zipcodes ({tables}, name, state, zipcode_tab_area, year)
            VALUES (
                {self.__generate_num_blanks(len(data[0][0]))}
            );
            """, self.__census_insert_generator(data))


    def __init_census_school_districts_table(self):
        tables = self.__get_census_tables("school_districts_tables")
        # make table named "census_school_districts"
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.census_school_districts', 'U') IS NOT NULL
                DROP TABLE dbo.census_school_districts;
            CREATE TABLE dbo.census_school_districts (
                state varchar(2) NOT NULL,
                sd_unified char(5) NOT NULL,
                year int NOT NULL,
                name varchar(82),
                {tables},
                PRIMARY KEY (state, sd_unified, year)
            );
            """)
        curr_year = self.__get_curr_year()
        data = get_census_data("school_districts", curr_year-3, curr_year)
        tables = self.__prepare_census_tables_for_query(tables)

        self.__cursor.executemany(f"""
            INSERT INTO dbo.census_school_districts ({tables}, name, state, sd_unified, year)
            VALUES (
                {self.__generate_num_blanks(len(data[0][0]))}
            );
            """, self.__census_insert_generator(data))


    def __init_census_state_poverty_table(self):
        tables = self.__get_census_tables("poverty_tables")
        # make table named "census_state_poverty"
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.census_state_poverty', 'U') IS NOT NULL
                DROP TABLE dbo.census_state_poverty;
            CREATE TABLE dbo.census_state_poverty (
                state char(2) NOT NULL,
                year int NOT NULL,
                {tables},
                PRIMARY KEY (state, year)
            );
        """)
        curr_year = self.__get_curr_year()
        data = get_census_timeseries("states", "poverty", curr_year-3, curr_year)

        tables = self.__prepare_census_tables_for_query(tables)

        exclude = []
        exclude.append(data[0][0].index('NAME'))
        exclude.append(data[0][0].index('time'))

        self.__cursor.executemany(f"""
            INSERT INTO dbo.census_state_poverty ({tables}, year, state)
            VALUES (
                {self.__generate_num_blanks(len(data[0][0]) - len(exclude))}
            );
        """, self.__census_insert_generator(data, exclude_indexes=exclude))


    def __init_census_county_poverty_table(self):
        tables = self.__get_census_tables("poverty_tables")
        # make table named "census_county_poverty"
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.census_county_poverty', 'U') IS NOT NULL
                DROP TABLE dbo.census_county_poverty;
            CREATE TABLE dbo.census_county_poverty (
                state varchar(2) NOT NULL,
                county varchar(3) NOT NULL,
                year int NOT NULL,
                {tables},
                PRIMARY KEY (state, county, year)
            );
        """)
        curr_year = self.__get_curr_year()
        data = get_census_timeseries("counties", "poverty", curr_year-3, curr_year)

        tables = self.__prepare_census_tables_for_query(tables)

        exclude = []
        exclude.append(data[0][0].index('NAME'))
        exclude.append(data[0][0].index('time'))

        self.__cursor.executemany(f"""
            INSERT INTO dbo.census_county_poverty ({tables}, year, state, county)
            VALUES (
                {self.__generate_num_blanks(len(data[0][0]) - len(exclude))}
            );
        """, self.__census_insert_generator(data, exclude_indexes=exclude))


    def __init_census_state_data_table(self):
        tables = self.__get_census_tables("census_data_tables")
        # make table named "census_county_data"
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.census_state_data', 'U') IS NOT NULL
                DROP TABLE dbo.census_state_data;
            CREATE TABLE dbo.census_state_data (
                state char(2) NOT NULL,
                year INT NOT NULL,
                {tables},
                PRIMARY KEY (state, year)
            );
        """)
        curr_year = self.__get_curr_year()
        data = get_census_data("states", curr_year-3, curr_year)

        tables = self.__prepare_census_tables_for_query(tables)

        exclude = []
        exclude.append(data[0][0].index('NAME'))

        self.__cursor.executemany(f"""
            INSERT INTO dbo.census_state_data ({tables}, state, year)
            VALUES (
                {self.__generate_num_blanks(len(data[0][0])-len(exclude))}
            );
        """, self.__census_insert_generator(data, exclude_indexes=exclude))


    def __init_census_county_data_table(self):
        tables = self.__get_census_tables("census_data_tables")
        # make table named "census_county_data"
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.census_county_data', 'U') IS NOT NULL
                DROP TABLE dbo.census_county_data;
            CREATE TABLE dbo.census_county_data (
                state char(2) NOT NULL,
                county char(3) NOT NULL,
                year INT NOT NULL,
                {tables},
                PRIMARY KEY (state, county, year)
            );
        """)
        curr_year = self.__get_curr_year()
        data = get_census_data("counties", curr_year-3, curr_year)

        tables = self.__prepare_census_tables_for_query(tables)

        # get index of 'NAME' in data[0][0]
        exclude = [data[0][0].index('NAME')]

        self.__cursor.executemany(f"""
            INSERT INTO dbo.census_county_data ({tables}, state, county, year)
            VALUES (
                {self.__generate_num_blanks(len(data[0][0])-len(exclude))}
            );
        """, self.__census_insert_generator(data, exclude_indexes=exclude))


    def __prepare_census_tables_for_query(self, tables):
        return ','.join([x.split()[0] for x in tables.split(',')])


    def __init_employment_table(self, for_):
        """
        @param for_: str "US", "STATE", or "COUNTY"
        """
        # check if param valid
        if not isinstance(for_, str):
            raise TypeError("for_ must be a string")
        
        for_ = for_.lower()

        if for_ not in ["us", "state", "county"]:
            raise ValueError("for_ must be one of 'US', 'STATE', or 'COUNTY'")

        # set up optional sql inserts
        state_create = "state CHAR(2) NOT NULL, "
        state_col = "state, "
        county_create = "county CHAR(3) NOT NULL, "
        county_col = "county, "

        state_codes = self.__get_all_state_fips()
        county_codes = [self.__get_all_county_fips(state) for state in state_codes]

        if for_ == "us":
            state_create, state_col, county_create, county_col = "", "", "", ""
        elif for_ == "state":
            county_create, county_col = "", ""
        
        self.__cursor.execute(f"""
            IF OBJECT_ID('dbo.{for_}_employment', 'U') IS NOT NULL
                DROP TABLE dbo.{for_}_employment;
            CREATE TABLE dbo.{for_}_employment (
                {state_create}{county_create}
                year SMALLINT NOT NULL,
                qtr TINYINT NOT NULL,
                own_code INT,
                industry_code VARCHAR(6),
                agglvl_code INT,
                size_code INT,
                disclosure_code varchar(5),
                qtrly_estabs BIGINT,
                month1_emplvl BIGINT,
                month2_emplvl BIGINT,
                month3_emplvl BIGINT,
                total_qtrly_wages BIGINT,
                taxable_qtrly_wages BIGINT,
                qtrly_contributions BIGINT,
                avg_wkly_wage BIGINT,
                PRIMARY KEY ({state_col}{county_col}own_code, industry_code, agglvl_code, year, qtr)
            );
        """)

        def insert_into_table(row):
            new_row = []
            fip = str(row[0])
            while len(fip) < 5:
                fip = "0" + fip

            if state_col:
                new_row.append(f"{fip[:2]}")
            if county_col:
                new_row.append(f"{fip[2:]}")
            new_row = (*new_row, *row[1:])

            self.__cursor.execute(f"""
                    INSERT INTO dbo.{for_}_employment ( 
                        {state_col}{county_col}own_code, industry_code, agglvl_code, size_code, year, qtr,
                        disclosure_code, qtrly_estabs, month1_emplvl, month2_emplvl, month3_emplvl, 
                        total_qtrly_wages, taxable_qtrly_wages, qtrly_contributions, avg_wkly_wage
                    )
                    VALUES (
                        {self.__generate_num_blanks(len(new_row))}
                    );
                """, *new_row)

        curr_year = self.__get_curr_year()

        for_param = "us"
        if for_ == "state":
            for_param = "states"
        elif for_ == "county":
            for_param = "counties"
        
        for emp_data in get_employment_data(for_param, curr_year-3, curr_year, state_codes, county_codes):
            emp_data = emp_data[emp_data.columns[:16]] # cut off unwanted columns
            emp_data = emp_data.replace(np.nan, '-')
            emp_data = np.array(emp_data.values)
            np.apply_along_axis(insert_into_table, 1, emp_data)


    def close_connection(self):
        self.__connection.close()


    def __init_states_table(self):
        # make table named "states"
        self.__cursor.execute("""
            IF OBJECT_ID('dbo.states', 'U') IS NOT NULL
                DROP TABLE dbo.states;
            CREATE TABLE dbo.states (
                FIP char(2) NOT NULL,
                state char(2) NOT NULL,
                PRIMARY KEY (FIP)
            );
        """)

        with open("states.csv", 'r') as f:
            # skip header
            next(f)
            # load data into table
            for line in f:
                fip, state = line.strip().split(',')
                self.__cursor.execute(f"""
                    INSERT INTO dbo.states (FIP, State)
                    VALUES ('{fip}', '{state}');
                """)


    def __init_counties_table(self):
        # make table named "counties"
        self.__cursor.execute("""
            IF OBJECT_ID('dbo.counties', 'U') IS NOT NULL
                DROP TABLE dbo.counties;
            CREATE TABLE dbo.counties (
                state char(2) NOT NULL,
                county char(3) NOT NULL,
                area_name varchar(50) NOT NULL,
                county_state varchar(50) NOT NULL,
                PRIMARY KEY (state, county)
            );
        """)

        with open("counties.csv", 'r') as f:
            # skip header
            next(f)
            # load data into table
            for line in f:
                cnty, st, a_n, cnty_st = [x.replace("'", "''") for x in line.strip().split(',')] # <- necesarry for words containing apostrophes
                self.__cursor.execute(f"""
                    INSERT INTO dbo.counties (state, county, area_name, county_state)
                    VALUES ('{st}', '{cnty}', '{a_n}', '{cnty_st}');
                """)


    def __init_state_unemployment_table(self):
        # make named table "state_unemployment"
        self.__cursor.execute("""
            IF OBJECT_ID('dbo.state_unemployment_rate', 'U') IS NOT NULL
                DROP TABLE dbo.state_unemployment_rate;
            CREATE TABLE dbo.state_unemployment_rate (
                state char(2) NOT NULL,
                year int NOT NULL,
                period char(3) NOT NULL,
                value float,
                PRIMARY KEY (state, year, period)
            );
        """)

        current_year = self.__get_curr_year()
        states = self.__get_all_state_fips()

        # call get_unemployment_data with 50 fips at a time
        for lower, upper in self.__bls_timeseries_index_generator(len(states)):
            unemp_data = get_unemployment_data(states[lower:upper], start_year=current_year-2, end_year=current_year)
            self.__iterate_over_timeseries_and_execute_query(unemp_data, self.__insert_into_state_unemployment_skeleton)


    def __init_county_unemployment_table(self):
        # make named table "county_unemployment_rate"
        self.__cursor.execute("""
            IF OBJECT_ID('dbo.county_unemployment_rate', 'U') IS NOT NULL
                DROP TABLE dbo.county_unemployment_rate;
            CREATE TABLE dbo.county_unemployment_rate (
                state char(2) NOT NULL,
                county char(3) NOT NULL,
                year int NOT NULL,
                period char(3) NOT NULL,
                value float,
                PRIMARY KEY (state, county, year, period)
            );
        """)

        current_year = self.__get_curr_year()
        states = self.__get_all_state_fips()

        for state in states:
            counties = [x[0] for x in self.__cursor.execute(f"select county from dbo.counties where state = '{state}';").fetchall()]
            for lower, upper in self.__bls_timeseries_index_generator(len(counties)):
                unemp_data = get_unemployment_data([state], counties[lower:upper], start_year=current_year-2, end_year=current_year)
                self.__iterate_over_timeseries_and_execute_query(unemp_data, self.__insert_into_county_unemployment_skeleton, include_county=True)


    def __init_county_workers_table(self):
        # make named table "county_workers"
        self.__cursor.execute("""
            IF OBJECT_ID('dbo.county_workers', 'U') IS NOT NULL
                DROP TABLE dbo.county_workers;
            CREATE TABLE dbo.county_workers (
                state char(2) NOT NULL,
                county char(3) NOT NULL,
                year int NOT NULL,
                period char(3) NOT NULL,
                value float,
                PRIMARY KEY (state, county, year, period)
            );
        """)

        current_year = self.__get_curr_year()
        states = self.__get_all_state_fips()

        for state in states:
            counties = [x[0] for x in self.__cursor.execute(f"select county from dbo.counties where state = '{state}';").fetchall()]
            for lower, upper in self.__bls_timeseries_index_generator(len(counties)):
                unemp_data = get_county_workers([state], counties[lower:upper], start_year=current_year-2, end_year=current_year)
                self.__iterate_over_timeseries_and_execute_query(unemp_data, self.__insert_into_county_workers_skeleton, include_county=True)


    def __bls_timeseries_index_generator(self, n):
        """
        One is only allowed to request 50 or less timeseries data from BLS at a time.
        This generator allows us to break up the indexes into groups of 50 or less

        @param n: total number of timeseries data (fips) to be requested

        @return: a generator that yields tuples of (lower, upper) indexes
        """
        for i in range(0, n, 50):
            lower = i
            upper = i + 50
            if upper > n:
                upper = n
            yield lower, upper


    def __iterate_over_timeseries_and_execute_query(self, timeseries_data, query, include_county=False):
        """
        This method iteratives over the timeseries data and executes the provided query

        NOTE: format your string as follows:
            {st} -> state, {cnty} -> county, {yr} -> year, {prd} -> period, {val} -> value
  

        @oaram timeseries_data: the timeseries data (json) returned from BLS
        @param query: the query to be executed
        @param include_county: whether or not to include the county in the query
        """
        for info in timeseries_data['Results']['series']:
                series_id, data_key = info.keys()
                series_id, data = info[series_id], info[data_key]
                state, county = series_id[5:7], series_id[7:10]
                for d in data:
                    year, period, value = d['year'], d['period'], d['value']
                    value = 'NULL' if value == '-' else value
            
                    if include_county:
                        query_to_exec = query.format(st=state, cnty=county, yr=year, prd=period, val=value)
                    else:
                        query_to_exec = query.format(st=state, yr=year, prd=period, val=value)
                    
                    self.__cursor.execute(query_to_exec)


    def __get_curr_year(self) -> int:
        return int(self.__cursor.execute("SELECT YEAR(GETDATE());").fetchone()[0])


    def __get_all_state_fips(self) -> list:
        states = self.__cursor.execute("SELECT FIP FROM dbo.states where FIP != '00';").fetchall()
        return [fip[0] for fip in states]


    def __get_all_county_fips(self, state_code) -> list:
        counties = self.__cursor.execute(f"SELECT county FROM dbo.counties where county != '000' and state = '{state_code}';").fetchall()
        return [county[0] for county in counties]


    def __del__(self):
        self.__connection.close()


    def __census_insert_generator(self, data, exclude_indexes=[]):
        """
        Generator method that excludes indexes

        @param data: the data to be inserted
        @param exclude_indexes: list of the indexes to be excluded
        """
        for d in data:
            for i, values in enumerate(d):
                if i != 0: # <- excludes headers
                    yield [values[j] for j in range(len(values)) if j not in exclude_indexes]
    

    def __generate_num_blanks(self, n_blanks):
        return ', '.join(['?'] * n_blanks)


    def __get_census_tables(self, tables):
        with open("request_info.json", 'r') as f:
            request_info = json.load(f)
            tables = request_info['tables'][tables]
        return ','.join([' '.join(x.split(',')) for x in tables])

    # def debugger(self, arg):
    #     return self.__cursor.execute(arg)