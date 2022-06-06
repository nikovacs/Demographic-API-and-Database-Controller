"""
Parent program for the API_DB_Mediator.
Use from command line to initialize specific tables (or all) (py db_updater.py -t [table_name]) 
Double click or run with no args for GUI interface.

Author: Nikolas Kovacs
"""
from api_db_mediator import API_DB_Mediator
import argparse
import threading

def update(tables_to_update, table_args, ui=None):
    def update_table(method_args, event_to_set):
        if len(method_args) == 1: # method takes no args
            method_args[0]()
        else:
            method_args[0](method_args[1])
        event_to_set.set()

    def update_ui(ui, table_to_update, stop, end_thread_event):
        # time_estimates = get_estimate_times_for_updates()
        still_updating = True
        progress_bar = 0
        while still_updating and not stop():
            if progress_bar < 100:
                ui.progressChanged.emit(progress_bar + 1) # temporary solution until we have estimated times
                progress_bar += 1
                time.sleep(0.2)
            else:
                still_updating = False
            ui.update_label(f"Updating {table_to_update}... Do not close this window.")
        end_thread_event.set()
    


    for table_to_update in tables_to_update:
        ui_stop_thread_event = threading.Event()
        ui_thread_ended_event = threading.Event()
        ui_stop_thread = False

        if ui:
            ui_updater_thread = threading.Thread(target=update_ui, args=(ui, table_to_update, lambda: ui_stop_thread, ui_thread_ended_event), daemon=True, name="ui_updater_thread")
            ui_updater_thread.start()

        update_table(table_args[table_to_update], ui_stop_thread_event)
        
        ui_stop_thread_event.wait()
        ui_stop_thread = True
        ui_thread_ended_event.wait()
    
    if ui:
        ui.progressChanged.emit(100)
        time.sleep(0.5)
        ui.update_label("Done updating. You may close this window.")


def get_estimate_times_for_updates():
    pass

def init_args(db):
    return {
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

if __name__ == "__main__":
    db = API_DB_Mediator()
    table_args = init_args(db)

    parser = argparse.ArgumentParser(description="CLI for updating tables in the Demographic Database\n"\
        "Run with no arguments to open GUI"\
        "Run with single table argument to update that table"\
        "./db_updater.exe -h for help")
    parser.add_argument("-t", "--table", type=str, choices=table_args.keys(), help="Enter a table to update")
    args = parser.parse_args()
    table_to_update = args.table

    # if table argument provided, update that table
    # otherwise, open gui and let user select table(s) to update
    if table_to_update:
        update([table_to_update], table_args)
    else:
        import sys
        import time
        from db_updater_gui import GUI
        from PyQt5 import QtWidgets
        app = QtWidgets.QApplication(sys.argv)
        MainWindow = QtWidgets.QMainWindow()
        ui = GUI(MainWindow)
        MainWindow.show()

        def try_to_get_tables_and_update():
            tables_to_update = None
            while not tables_to_update:
                tables_to_update = ui.get_tables_to_update()
                time.sleep(0.5)
            update(tables_to_update, table_args, ui)

        thread = threading.Thread(target=try_to_get_tables_and_update, daemon=True)
        thread.start()

        sys.exit(app.exec_())
