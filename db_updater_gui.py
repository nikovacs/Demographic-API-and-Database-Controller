"""
An exntension of the ui.py output from pyuic5

This class contains the logic and methods necessary for the simple checkbox-based gui to function and be 
updated by the db_updater.py script.

Author: Nikolas Kovacs
"""


from PyQt5 import QtCore, QtGui, QtWidgets
from ui import Ui_MainWindow


class GUI(Ui_MainWindow): 
    progressChanged = QtCore.pyqtSignal(int)

    def __init__(self, MainWindow):
        super().__init__()
        self.setupUi(MainWindow)
        self.progressChanged.connect(self.update_progress)

        self.set_of_check_boxes = set()
        self.set_of_checked_check_boxes = set()
        self.update_button_pressed = False

        self.set_of_check_boxes.add(self.checkBox_initialize_all)
        self.checkBox_initialize_all.clicked.connect(self.check_all)
        self.set_of_check_boxes.add(self.checkBox_states)
        self.checkBox_states.clicked.connect(lambda: self.checked(self.checkBox_states))
        self.set_of_check_boxes.add(self.checkBox_counties)
        self.checkBox_counties.clicked.connect(lambda: self.checked(self.checkBox_counties))
        self.set_of_check_boxes.add(self.checkBox_county_unemployment)
        self.checkBox_county_unemployment.clicked.connect(lambda: self.checked(self.checkBox_county_unemployment))
        self.set_of_check_boxes.add(self.checkBox_state_unemployment)
        self.checkBox_state_unemployment.clicked.connect(lambda: self.checked(self.checkBox_state_unemployment))
        self.set_of_check_boxes.add(self.checkBox_county_workers)
        self.checkBox_county_workers.clicked.connect(lambda: self.checked(self.checkBox_county_workers))
        self.set_of_check_boxes.add(self.checkBox_county_data)
        self.checkBox_county_data.clicked.connect(lambda: self.checked(self.checkBox_county_data))
        self.set_of_check_boxes.add(self.checkBox_state_data)
        self.checkBox_state_data.clicked.connect(lambda: self.checked(self.checkBox_state_data))
        self.set_of_check_boxes.add(self.checkBox_county_poverty)
        self.checkBox_county_poverty.clicked.connect(lambda: self.checked(self.checkBox_county_poverty))
        self.set_of_check_boxes.add(self.checkBox_state_poverty)
        self.checkBox_state_poverty.clicked.connect(lambda: self.checked(self.checkBox_state_poverty))
        self.set_of_check_boxes.add(self.checkBox_school_districts)
        self.checkBox_school_districts.clicked.connect(lambda: self.checked(self.checkBox_school_districts))
        self.set_of_check_boxes.add(self.checkBox_zipcodes)
        self.checkBox_zipcodes.clicked.connect(lambda: self.checked(self.checkBox_zipcodes))
        self.set_of_check_boxes.add(self.checkBox_county_gdp)
        self.checkBox_county_gdp.clicked.connect(lambda: self.checked(self.checkBox_county_gdp))
        self.set_of_check_boxes.add(self.checkBox_state_gdp)
        self.checkBox_state_gdp.clicked.connect(lambda: self.checked(self.checkBox_state_gdp))
        self.set_of_check_boxes.add(self.checkBox_county_employment)
        self.checkBox_county_employment.clicked.connect(lambda: self.checked(self.checkBox_county_employment))
        self.set_of_check_boxes.add(self.checkBox_state_employment)
        self.checkBox_state_employment.clicked.connect(lambda: self.checked(self.checkBox_state_employment))
        self.set_of_check_boxes.add(self.checkBox_us_employment)
        self.checkBox_us_employment.clicked.connect(lambda: self.checked(self.checkBox_us_employment))

        self.update_button.clicked.connect(self.update_button_clicked)

    def update_label(self, message):
        self.current_job_label.setText(message)


    def update_progress(self, value):
        self.progressBar.setValue(value)


    def checked(self, checkbox):
        if checkbox.isChecked():
            self.set_of_checked_check_boxes.add(checkbox)
        else:
            self.set_of_checked_check_boxes.discard(checkbox)


    def check_all(self):
        if self.checkBox_initialize_all.isChecked():
            for check_box in self.set_of_check_boxes:
                check_box.setChecked(True)
                self.set_of_checked_check_boxes.add(check_box)
        else:
            for check_box in self.set_of_check_boxes:
                check_box.setChecked(False)
                self.set_of_checked_check_boxes.discard(check_box)


    def update_button_clicked(self):
        self.update_button_pressed = True

    
    def get_tables_to_update(self):
        if self.update_button_pressed:
            return ['_'.join(x.objectName().split('_')[1:]) for x in self.set_of_checked_check_boxes]