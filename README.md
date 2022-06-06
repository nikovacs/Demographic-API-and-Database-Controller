# Demographic Database Updater
### by: Nikolas Kovacs

#### This program retrieves the following information from:
* BLS.gov
  * State Unemployment Rate
  * County Unemployment Rate
  * County Workers
  * County Employment
  * State Employment
  * US Employment
  
* Census.gov
  * County Data
  * State Data
  * County Poverty
  * State Poverty
  * School Districts
  * ZipCodes
  
* BEA.gov
  * County GDP
  * State GDP

For most of the data, the last 3-5 years are requested. This can be changed by changing the `start_year` argument for the data requests in `api_db_mediator.py`

#### In order for for this program to run, it requires two files:
* config.json (formatting as follows):
```
{
"server": "<server address here>",
"database": "<Name of Database>",
"username": "<username>",
"password": "<password>"
}
```

* request_info.json
```
{ "keys":
    {
        "bls_key": "<bls api key>",
        "census_key": "<census api key>",
        "bea_user_id": "<bea user id>"
    },

    "tables":
    {
        "census_data_tables": [
            
            FOR EXAMPLE: v

            "DP02_0001E,INT","DP05_0086E,INT","DP02_0016E,FLOAT","DP02_0017E,FLOAT","DP02_0002E,INT",
            "DP02_0004E,INT","DP03_0119PE,FLOAT","DP04_0090PE,INT","DP04_0047PE,FLOAT","DP04_0003PE,FLOAT",
            "DP04_0089E,INT","DP02_0060PE,FLOAT","DP02_0061PE,FLOAT","DP02_0062PE,FLOAT","DP02_0063PE,FLOAT","DP02_0064PE,FLOAT",
            "DP02_0068PE,FLOAT","DP02_0068E,INT","DP03_0088E,INT","DP03_0062E,INT","DP03_0063E,INT","DP04_0091PE,FLOAT",
            "DP05_0001E,INT","DP05_0002E,INT","DP05_0003E,INT","DP05_0005E,INT","DP05_0006E,INT","DP05_0007E,INT",
            "DP05_0008E,INT","DP05_0009E,INT","DP05_0010E,INT","DP05_0011E,INT","DP05_0012E,INT","DP05_0013E,INT",
            "DP05_0014E,INT","DP05_0015E,INT","DP05_0016E,INT","DP05_0017E,INT"

        ],
        "poverty_tables": [
            
            FOR EXAMPLE: v
            
            "SAEPOVRT0_17_PT,FLOAT","SAEPOVRT5_17R_PT,FLOAT","SAEPOVRTALL_PT,FLOAT","SAEMHI_PT,INT","SAEPOVALL_PT,INT"
        ],
        "school_districts_tables": [
            
            FOR EXAMPLE: v
            
            "DP02_0001E,INT","DP02_0016E,FLOAT","DP02_0017E,FLOAT","DP02_0002E,INT","DP02_0004E,INT",
            "DP03_0119PE,FLOAT","DP05_0086E,INT","DP04_0090PE,INT","DP04_0047PE,FLOAT","DP04_0003PE,FLOAT","DP04_0089E,INT",
            "DP02_0060PE,FLOAT,","DP02_0061PE,FLOAT","DP02_0062PE,FLOAT","DP02_0063PE,FLOAT","DP02_0064PE,FLOAT","DP02_0068PE,FLOAT",
            "DP02_0068E,INT","DP03_0088E,INT","DP03_0062E,INT","DP03_0063E,INT","DP04_0091PE,FLOAT","DP05_0001E,INT",
            "DP05_0002E,INT","DP05_0003E,INT","DP05_0005E,INT","DP05_0006E,INT","DP05_0007E,INT","DP05_0008E,INT",
            "DP05_0009E,INT","DP05_0010E,INT","DP05_0011E,INT","DP05_0012E,INT","DP05_0013E,INT","DP05_0014E,INT",
            "DP05_0015E,INT","DP05_0016E,INT","DP05_0017E,INT","DP05_0018E,FLOAT"
        ],
        "zipcode_tables": [
            
            FOR EXAMPLE: v
            
            "DP02_0001E,INT", "DP02_0016E,FLOAT", "DP02_0017E,FLOAT", "DP02_0002E,INT", "DP02_0004E,INT", "DP03_0119PE,FLOAT", 
            "DP05_0086E,INT", "DP04_0090PE,INT", "DP04_0047PE,FLOAT", "DP04_0003PE,FLOAT", "DP04_0089E,INT", "DP02_0060PE,FLOAT",
            "DP02_0061PE,FLOAT", "DP02_0062PE,FLOAT", "DP02_0063PE,FLOAT", "DP02_0064PE,FLOAT", "DP02_0068PE,FLOAT", "DP02_0068E,INT", 
            "DP03_0088E,INT", "DP03_0062E,INT", "DP03_0063E,INT", "DP04_0091PE,FLOAT", "DP05_0001E,INT", "DP05_0002E,INT", 
            "DP05_0003E,INT", "DP05_0005E,INT", "DP05_0006E,INT", "DP05_0007E,INT", "DP05_0008E,INT", "DP05_0009E,INT", 
            "DP05_0010E,INT", "DP05_0011E,INT", "DP05_0012E,INT", "DP05_0013E,INT", "DP05_0014E,INT", "DP05_0015E,INT", 
            "DP05_0016E,INT", "DP05_0017E,FLOAT"
        ],
        "bea_gdp": {
            
            The lists of corresponding indexes in tables and line_codes match. 
            FOR EXAMPLE: resulting table_linecodes combinations CAGDP9-1 through CAGDP9-83 
            
            "tables": [
                ["CAGDP9"]
            ],
            "line_codes": [
                ["1", "2", "3", "6", "10", "11", "12", "34", "35", "36", "45", "50", "59", "68", "75", "82", "83"]
            ]
        }
    }
}
```

## USAGE
* This program can be used in two ways
  * Command Line
    * Must provide one keyword arg
    * ```py db_updater -h``` for help/possible arguments
    * ```py db_updater -t <table_name>``` to initialize the specified table.
  * GUI
    * To use the GUI, run db_updater.py either on the command line (```py db_updater.py```) with no arguments or double click.

### Requirements
* All the requirements (more or less depending on whether/how you choose to use/distribute the program) are located inside requirements.txt 
* To install the requirements in a virtual environment, run the following:
  * NOTE: The virtual environment is important in order to avoid conflicts with other libraries or (especially) if you want to use PyInstaller to turn the program into a .exe or .app since it will result in a smaller file/dir.

For Windows:
```
py -m venv .venv
.venv\Scripts\activate
py -m pip install -r requirements.txt
```

For Linux/Mac:
```
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```