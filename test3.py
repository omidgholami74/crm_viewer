import sys
import sqlite3
import pandas as pd
import re
import logging
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar, QMessageBox,
    QFileDialog, QLabel
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QPixmap
from pyqtgraph import PlotWidget, mkPen
from qfluentwidgets import (
    ComboBox, LineEdit, PrimaryPushButton, CheckBox, CardWidget,
    setTheme, Theme, FluentIcon, TitleLabel
)
from persiantools.jdatetime import JalaliDate
import numpy as np
from pathlib import Path
from PIL import Image

# Setup logging
log_file = Path("crm_visualizer.log").resolve()
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger()
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

def normalize_crm_id(crm_id):
    """Extract numeric part from CRM ID (e.g., 'CRM 258b' → '258')."""
    match = re.match(r'(\d+)', str(crm_id).strip())
    return match.group(1) if match else None

def validate_jalali_date(date_str):
    """Validate Jalali date string (YYYY/MM/DD)."""
    try:
        year, month, day = map(int, date_str.split('/'))
        JalaliDate(year, month, day)
        return True
    except (ValueError, TypeError):
        return False

def validate_percentage(text):
    """Validate percentage input (must be positive float)."""
    try:
        value = float(text)
        return value > 0
    except (ValueError, TypeError):
        return False

class DataLoaderThread(QThread):
    data_loaded = pyqtSignal(pd.DataFrame, pd.DataFrame)
    error_occurred = pyqtSignal(str)
    progress_updated = pyqtSignal(int)

    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path

    def run(self):
        try:
            logging.debug(f"Loading data from {self.db_path}")
            self.progress_updated.emit(20)
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query("SELECT * FROM crm_data", conn)
            conn.close()
            self.progress_updated.emit(60)

            df['date'] = df['file_name'].apply(self.extract_date)
            df = df.dropna(subset=['date'])
            df['year'] = df['date'].apply(lambda x: x.year)
            df['month'] = df['date'].apply(lambda x: x.month)
            df['day'] = df['date'].apply(lambda x: x.day)
            self.progress_updated.emit(80)

            crm_df = df[df['crm_id'] != 'BLANK'].copy()
            blank_df = df[df['crm_id'] == 'BLANK'].copy()
            crm_df['norm_crm_id'] = crm_df['crm_id'].apply(normalize_crm_id)
            self.progress_updated.emit(100)
            logging.debug(f"Loaded {len(crm_df)} CRM records and {len(blank_df)} BLANK records from {self.db_path}")
            self.data_loaded.emit(crm_df, blank_df)
        except Exception as e:
            logging.error(f"Data loading error: {str(e)}")
            self.error_occurred.emit(f"Failed to load data: {str(e)}")

    def extract_date(self, file_name):
        """Extract Jalali date from file name (e.g., '1404-01-01')."""
        try:
            match = re.match(r'(\d{4}-\d{2}-\d{2})', file_name)
            if match:
                date_str = match.group(1)
                year, month, day = map(int, date_str.split('-'))
                return JalaliDate(year, month, day)
            return None
        except Exception:
            return None

class ImportFileThread(QThread):
    import_completed = pyqtSignal(pd.DataFrame)
    error_occurred = pyqtSignal(str)
    progress_updated = pyqtSignal(int)

    def __init__(self, file_path, db_path):
        super().__init__()
        self.file_path = file_path
        self.db_path = db_path

    def run(self):
        try:
            self.progress_updated.emit(20)
            ext = Path(self.file_path).suffix.lower()
            if ext not in ['.csv', '.rep']:
                raise ValueError("Unsupported file format. Only CSV and .rep are allowed.")
            
            df = pd.read_csv(self.file_path)
            self.progress_updated.emit(50)
            
            required_columns = ['id', 'crm_id', 'solution_label', 'element', 'value', 'file_name', 'folder_name']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"CSV/.rep file must contain columns: {required_columns}")

            conn = sqlite3.connect(self.db_path)
            df.to_sql('crm_data', conn, if_exists='append', index=False)
            conn.close()
            self.progress_updated.emit(100)
            logging.info(f"Imported {len(df)} records from {self.file_path} to {self.db_path}")
            self.import_completed.emit(df)
        except Exception as e:
            logging.error(f"Import error: {str(e)}")
            self.error_occurred.emit(f"Failed to import file: {str(e)}")

class FilterThread(QThread):
    filtered_data = pyqtSignal(pd.DataFrame, pd.DataFrame)
    progress_updated = pyqtSignal(int)

    def __init__(self, crm_df, blank_df, filters):
        super().__init__()
        self.crm_df = crm_df
        self.blank_df = blank_df
        self.filters = filters

    def run(self):
        filtered_crm_df = self.crm_df.copy()
        filtered_blank_df = self.blank_df.copy()
        logging.debug(f"Applying filters: {self.filters}")
        self.progress_updated.emit(20)
        
        if self.filters['device'] != "All Devices":
            filtered_crm_df = filtered_crm_df[filtered_crm_df['folder_name'].str.contains(self.filters['device'], case=False, na=False)]
            filtered_blank_df = filtered_blank_df[filtered_blank_df['folder_name'].str.contains(self.filters['device'], case=False, na=False)]
        self.progress_updated.emit(40)
        
        if self.filters['element'] != "All Elements":
            base_element = self.filters['element']
            filtered_crm_df = filtered_crm_df[filtered_crm_df['element'].str.startswith(base_element + ' ', na=False) | (filtered_crm_df['element'] == base_element)]
            filtered_blank_df = filtered_blank_df[filtered_blank_df['element'].str.startswith(base_element + ' ', na=False) | (filtered_blank_df['element'] == base_element)]
        self.progress_updated.emit(60)
        
        if self.filters['crm'] != "All CRM IDs":
            filtered_crm_df = filtered_crm_df[filtered_crm_df['norm_crm_id'] == self.filters['crm']]
        
        if self.filters['from_date']:
            filtered_crm_df = filtered_crm_df[filtered_crm_df['date'] >= self.filters['from_date']]
            filtered_blank_df = filtered_blank_df[filtered_blank_df['date'] >= self.filters['from_date']]
        if self.filters['to_date']:
            filtered_crm_df = filtered_crm_df[filtered_crm_df['date'] <= self.filters['to_date']]
            filtered_blank_df = filtered_blank_df[filtered_blank_df['date'] <= self.filters['to_date']]
        
        self.progress_updated.emit(100)
        logging.debug(f"Filtered {len(filtered_crm_df)} CRM records and {len(filtered_blank_df)} BLANK records")
        self.filtered_data.emit(filtered_crm_df, filtered_blank_df)

class CRMDataVisualizer(QMainWindow):
    def __init__(self):
        super().__init__()
        setTheme(Theme.LIGHT)  # Set Fluent light theme
        self.setWindowTitle("CRM Data Visualizer")
        self.setGeometry(100, 100, 1400, 900)

        # Initialize data
        self.crm_df = pd.DataFrame()
        self.blank_df = pd.DataFrame()
        self.crm_db_path = self.get_db_path("crm_blank.db")
        self.ver_db_path = self.get_db_path("crm_data.db")
        self.filtered_crm_df_cache = None
        self.filtered_blank_df_cache = None
        self.plot_df_cache = None
        self.updating_filters = False
        self.verification_cache = {}
        self.plot_data_items = []
        self.logo_path = Path("logo.png")

        # Main widget and layout
        self.main_widget = QWidget()
        self.setCentralWidget(self.main_widget)
        self.main_layout = QVBoxLayout(self.main_widget)
        self.main_layout.setSpacing(16)
        self.main_layout.setContentsMargins(20, 20, 20, 20)

        # Button section (boxed with CardWidget)
        self.button_card = CardWidget()
        self.button_card.setStyleSheet("""
            CardWidget {
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                box-shadow: 0px 4px 12px rgba(0, 0, 0, 0.15);
            }
        """)
        self.button_layout = QHBoxLayout()
        self.button_layout.setSpacing(12)
        self.button_layout.setContentsMargins(15, 10, 15, 10)
        self.import_button = PrimaryPushButton("Import File", self, FluentIcon.DOWNLOAD)
        self.export_button = PrimaryPushButton("Export Table", self, FluentIcon.SAVE)
        self.plot_button = PrimaryPushButton("Plot Data", self)
        self.save_button = PrimaryPushButton("Save Plot", self, FluentIcon.SAVE)
        self.reset_button = PrimaryPushButton("Reset Filters", self, FluentIcon.SYNC)
        self.button_layout.addWidget(self.import_button)
        self.button_layout.addWidget(self.export_button)
        self.button_layout.addWidget(self.plot_button)
        self.button_layout.addWidget(self.save_button)
        self.button_layout.addWidget(self.reset_button)
        self.button_layout.addStretch()
        self.button_card.setLayout(self.button_layout)
        self.main_layout.addWidget(self.button_card)

        # Filter and logo section
        self.filter_logo_layout = QHBoxLayout()
        self.filter_logo_layout.setSpacing(16)

        # Filter section (boxed with CardWidget)
        self.filter_card = CardWidget()
        self.filter_card.setStyleSheet("""
            CardWidget {
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                box-shadow: 0px 4px 12px rgba(0, 0, 0, 0.15);
            }
        """)
        self.filter_layout = QVBoxLayout()
        self.filter_layout.setSpacing(12)
        self.filter_layout.setContentsMargins(15, 15, 15, 15)

        # Title for filter card
        self.filter_title = TitleLabel("Filter Controls")
        self.filter_title.setStyleSheet("""
            TitleLabel {
                color: #000000;
                font-size: 18px;
                font-weight: bold;
                padding: 8px 0;
            }
        """)
        self.filter_layout.addWidget(self.filter_title)

        # Controls in one row (except checkboxes)
        self.controls_layout = QHBoxLayout()
        self.controls_layout.setSpacing(12)
        self.device_label = QLabel("Device:")
        self.device_combo = ComboBox()
        self.element_label = QLabel("Element:")
        self.element_combo = ComboBox()
        self.crm_label = QLabel("CRM ID:")
        self.crm_combo = ComboBox()
        self.from_date_label = QLabel("From Date:")
        self.from_date_edit = LineEdit()
        self.from_date_edit.setPlaceholderText("YYYY/MM/DD")
        self.from_date_edit.setFixedWidth(120)
        self.to_date_label = QLabel("To Date:")
        self.to_date_edit = LineEdit()
        self.to_date_edit.setPlaceholderText("YYYY/MM/DD")
        self.to_date_edit.setFixedWidth(120)
        self.percentage_label = QLabel("Control Range (%):")
        self.percentage_edit = LineEdit()
        self.percentage_edit.setPlaceholderText("%")
        self.percentage_edit.setFixedWidth(80)
        self.percentage_edit.setText("10")
        self.controls_layout.addWidget(self.device_label)
        self.controls_layout.addWidget(self.device_combo)
        self.controls_layout.addWidget(self.element_label)
        self.controls_layout.addWidget(self.element_combo)
        self.controls_layout.addWidget(self.crm_label)
        self.controls_layout.addWidget(self.crm_combo)
        self.controls_layout.addWidget(self.from_date_label)
        self.controls_layout.addWidget(self.from_date_edit)
        self.controls_layout.addWidget(self.to_date_label)
        self.controls_layout.addWidget(self.to_date_edit)
        self.controls_layout.addWidget(self.percentage_label)
        self.controls_layout.addWidget(self.percentage_edit)
        self.controls_layout.addStretch()
        self.filter_layout.addLayout(self.controls_layout)

        # Checkboxes in one column
        self.checkbox_layout = QVBoxLayout()
        self.checkbox_layout.setSpacing(8)
        self.best_wl_check = CheckBox("Select Best Wavelength")
        self.best_wl_check.setChecked(True)
        self.apply_blank_check = CheckBox("Apply Blank Correction")
        self.apply_blank_check.setChecked(False)
        self.checkbox_layout.addWidget(self.best_wl_check)
        self.checkbox_layout.addWidget(self.apply_blank_check)
        self.checkbox_layout.addStretch()
        self.filter_layout.addLayout(self.checkbox_layout)

        # Tooltips
        self.device_combo.setToolTip("Select a device to filter data")
        self.element_combo.setToolTip("Select an element to plot")
        self.crm_combo.setToolTip("Select a CRM ID to filter")
        self.from_date_edit.setToolTip("Enter start date in Jalali format (YYYY/MM/DD)")
        self.to_date_edit.setToolTip("Enter end date in Jalali format (YYYY/MM/DD)")
        self.percentage_edit.setToolTip("Enter control range percentage (e.g., 10 for ±10%)")
        self.best_wl_check.setToolTip("Select the best wavelength based on verification value")
        self.apply_blank_check.setToolTip("Subtract the best BLANK value from CRM data")

        # Populate combo boxes
        self.device_combo.addItem("All Devices")
        self.element_combo.addItem("All Elements")
        self.crm_combo.addItem("All CRM IDs")

        # Set layout to filter card
        self.filter_card.setLayout(self.filter_layout)

        # Logo section (boxed with CardWidget)
        self.logo_card = CardWidget()
        self.logo_card.setStyleSheet("""
            CardWidget {
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                box-shadow: 0px 4px 12px rgba(0, 0, 0, 0.15);
            }
        """)
        self.logo_layout = QVBoxLayout()
        self.logo_layout.setContentsMargins(10, 10, 10, 10)
        self.logo_label = QLabel()
        self.logo_label.setFixedSize(100, 50)
        self.logo_layout.addWidget(self.logo_label)
        self.logo_card.setLayout(self.logo_layout)
        self.logo_card.setFixedWidth(120)

        # Add filter and logo to layout
        self.filter_logo_layout.addWidget(self.filter_card, stretch=1)
        self.filter_logo_layout.addWidget(self.logo_card)
        self.main_layout.addLayout(self.filter_logo_layout)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(100)
        self.progress_bar.setVisible(False)
        self.main_layout.addWidget(self.progress_bar)

        # Plot widget
        self.plot_widget = PlotWidget()
        self.plot_widget.setTitle("CRM Data Plot", color='#000000', size='14pt')
        self.plot_widget.setLabel('left', 'Value', color='#000000')
        self.plot_widget.setLabel('bottom', 'Observation', color='#000000')
        self.plot_widget.addLegend(offset=(10, 10))
        self.main_layout.addWidget(self.plot_widget, stretch=2)

        # Tooltip label
        self.tooltip_label = QLabel("", self.plot_widget)
        self.tooltip_label.setStyleSheet("""
            background-color: #FFFFFF;
            color: #000000;
            border: 1px solid #0078D4;
            padding: 8px;
            border-radius: 4px;
            font-family: 'Segoe UI';
            box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.2);
        """)
        self.tooltip_label.setVisible(False)
        self.tooltip_label.setFont(QFont("Segoe UI", 10))

        # Table widget
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(8)
        self.table_widget.setHorizontalHeaderLabels(["ID", "CRM ID", "Solution Label", "Element", "Value", "File Name", "Folder Name", "Date"])
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.main_layout.addWidget(self.table_widget, stretch=1)

        # Status label
        self.status_label = QLabel("Loading data...")
        self.main_layout.addWidget(self.status_label)

        # Connect signals
        self.device_combo.currentTextChanged.connect(self.on_filter_changed)
        self.element_combo.currentTextChanged.connect(self.on_filter_changed)
        self.crm_combo.currentTextChanged.connect(self.on_filter_changed)
        self.from_date_edit.textChanged.connect(self.on_filter_changed)
        self.to_date_edit.textChanged.connect(self.on_filter_changed)
        self.percentage_edit.textChanged.connect(self.on_filter_changed)
        self.best_wl_check.stateChanged.connect(self.on_filter_changed)
        self.apply_blank_check.stateChanged.connect(self.on_filter_changed)
        self.import_button.clicked.connect(self.import_file)
        self.export_button.clicked.connect(self.export_table)
        self.plot_button.clicked.connect(self.plot_data)
        self.save_button.clicked.connect(self.save_plot)
        self.reset_button.clicked.connect(self.reset_filters)
        self.plot_widget.scene().sigMouseClicked.connect(self.on_mouse_clicked)
        self.plot_widget.scene().sigMouseMoved.connect(self.on_mouse_moved)

        # Apply styles and load logo
        self.apply_styles()
        self.load_default_logo()

        # Start data loading
        logging.debug("Initializing CRMDataVisualizer")
        self.load_data_thread()

    def get_db_path(self, name):
        """Get absolute path for database file."""
        return Path(__file__).parent / name

    def load_default_logo(self):
        """Load logo image if exists."""
        if self.logo_path.exists():
            pixmap = QPixmap(str(self.logo_path))
            self.logo_label.setPixmap(pixmap.scaled(100, 50, Qt.KeepAspectRatio))
            logging.info(f"Default logo loaded: {self.logo_path}")
        else:
            logging.warning(f"Default logo not found at: {self.logo_path}")

    def load_data_thread(self):
        """Start thread to load data from database."""
        self.progress_bar.setVisible(True)
        self.loader_thread = DataLoaderThread(self.crm_db_path)
        self.loader_thread.data_loaded.connect(self.on_data_loaded)
        self.loader_thread.error_occurred.connect(self.on_data_error)
        self.loader_thread.progress_updated.connect(self.progress_bar.setValue)
        self.loader_thread.finished.connect(lambda: self.progress_bar.setVisible(False))
        self.loader_thread.start()

    def import_file(self):
        """Open file dialog to import CSV or REP file."""
        fname, _ = QFileDialog.getOpenFileName(self, "Import File", "", "CSV or REP Files (*.csv *.rep)")
        if fname:
            self.progress_bar.setVisible(True)
            self.import_thread = ImportFileThread(fname, self.crm_db_path)
            self.import_thread.import_completed.connect(self.on_import_completed)
            self.import_thread.error_occurred.connect(self.on_data_error)
            self.import_thread.progress_updated.connect(self.progress_bar.setValue)
            self.import_thread.finished.connect(lambda: self.progress_bar.setVisible(False))
            self.import_thread.start()

    def on_import_completed(self, df):
        """Handle completion of file import."""
        self.load_data_thread()
        self.status_label.setText(f"Imported {len(df)} records successfully")
        logging.info(f"Imported {len(df)} records successfully")

    def on_data_loaded(self, crm_df, blank_df):
        """Process loaded data and populate filters."""
        allowed_crms = ['258', '252', '906', '506', '233', '255', '263', '269']
        crm_df = crm_df[crm_df['norm_crm_id'].isin(allowed_crms)].dropna(subset=['norm_crm_id'])
        self.crm_df = crm_df
        self.blank_df = blank_df
        logging.info(f"Loaded {len(crm_df)} CRM records and {len(blank_df)} BLANK records after normalization")
        self.populate_filters()
        self.status_label.setText("Data loaded successfully")

    def on_data_error(self, error_message):
        """Handle data loading errors."""
        self.crm_df = pd.DataFrame()
        self.blank_df = pd.DataFrame()
        self.status_label.setText(error_message)
        logging.error(error_message)
        QMessageBox.critical(self, "Error", error_message)
        self.populate_filters()

    def on_filter_changed(self):
        """Trigger filter update when filter inputs change."""
        if self.updating_filters:
            return
        logging.debug("Filter changed, updating filters")
        self.update_filters()

    def is_valid_crm_id(self, crm_id):
        """Check if CRM ID is in allowed list."""
        norm = normalize_crm_id(crm_id)
        allowed = ['258', '252', '906', '506', '233', '255', '263', '269']
        return norm in allowed

    def extract_device_name(self, folder_name):
        """Extract device name, allowing only 'mass', 'oes 4ac', or 'oes fire'."""
        if not folder_name or not isinstance(folder_name, str):
            # logging.warning(f"Invalid or empty folder_name: {folder_name}")
            return None
        allowed_devices = {'mass', 'oes 4ac', 'oes fire'}
        # Normalize folder_name to lowercase for case-insensitive comparison
        normalized_name = folder_name.strip().lower()
        if normalized_name in allowed_devices:
            # logging.debug(f"Valid device found: {normalized_name}")
            return normalized_name
        # logging.warning(f"folder_name '{folder_name}' is not an allowed device, skipping")
        return None

    def populate_filters(self):
        """Populate filter dropdowns with hardcoded device names and other unique values."""
        if self.crm_df.empty:
            logging.warning("No CRM data available to populate filters")
            return

        self.device_combo.blockSignals(True)
        self.element_combo.blockSignals(True)
        self.crm_combo.blockSignals(True)

        self.device_combo.clear()
        self.element_combo.clear()
        self.crm_combo.clear()

        # Hardcode allowed device names
        self.device_combo.addItem("All Devices")
        self.device_combo.addItems(['mass', 'oes 4ac', 'oes fire'])

        elements = sorted(set(el.split()[0] for el in self.crm_df['element'].unique() if isinstance(el, str)))
        crms = sorted(self.crm_df['norm_crm_id'].unique())

        # logging.debug(f"Devices hardcoded: ['mass', 'oes 4ac', 'oes fire']")
        # logging.debug(f"Elements: {elements}")
        # logging.debug(f"Valid CRM IDs: {crms}")

        self.element_combo.addItem("All Elements")
        self.element_combo.addItems(elements)
        self.crm_combo.addItem("All CRM IDs")
        self.crm_combo.addItems(crms)

        self.device_combo.blockSignals(False)
        self.element_combo.blockSignals(False)
        self.crm_combo.blockSignals(False)

        self.update_filters()

    def update_filters(self):
        """Apply filters to data and start filter thread."""
        if self.updating_filters:
            return
        self.updating_filters = True

        try:
            if self.crm_df.empty:
                self.table_widget.setRowCount(0)
                self.status_label.setText("No CRM data available")
                logging.warning("No CRM data available for filtering")
                self.updating_filters = False
                return

            from_date = None
            if validate_jalali_date(self.from_date_edit.text()):
                y, m, d = map(int, self.from_date_edit.text().split('/'))
                from_date = JalaliDate(y, m, d)

            to_date = None
            if validate_jalali_date(self.to_date_edit.text()):
                y, m, d = map(int, self.to_date_edit.text().split('/'))
                to_date = JalaliDate(y, m, d)

            filters = {
                'device': self.device_combo.currentText(),
                'element': self.element_combo.currentText(),
                'crm': self.crm_combo.currentText(),
                'from_date': from_date,
                'to_date': to_date
            }
            logging.debug(f"Updating filters: {filters}")

            self.progress_bar.setVisible(True)
            self.filter_thread = FilterThread(self.crm_df, self.blank_df, filters)
            self.filter_thread.filtered_data.connect(self.on_filtered_data)
            self.filter_thread.progress_updated.connect(self.progress_bar.setValue)
            self.filter_thread.finished.connect(lambda: self.progress_bar.setVisible(False))
            self.filter_thread.start()

        finally:
            self.updating_filters = False

    def on_filtered_data(self, filtered_crm_df, filtered_blank_df):
        """Update table and cache filtered data."""
        self.filtered_crm_df_cache = filtered_crm_df
        self.filtered_blank_df_cache = filtered_blank_df
        QApplication.processEvents()
        self.update_table(filtered_crm_df)
        self.status_label.setText(f"Loaded {len(filtered_crm_df)} CRM records, {len(filtered_blank_df)} BLANK records")
        logging.info(f"Filtered {len(filtered_crm_df)} CRM records and {len(filtered_blank_df)} BLANK records")

    def update_table(self, df):
        """Populate table with filtered data."""
        self.table_widget.setRowCount(len(df))
        for i, row in df.iterrows():
            QApplication.processEvents()
            self.table_widget.setItem(i, 0, QTableWidgetItem(str(row['id'])))
            self.table_widget.setItem(i, 1, QTableWidgetItem(row['crm_id']))
            self.table_widget.setItem(i, 2, QTableWidgetItem(row['solution_label']))
            self.table_widget.setItem(i, 3, QTableWidgetItem(row['element']))
            self.table_widget.setItem(i, 4, QTableWidgetItem(str(row['value'])))
            self.table_widget.setItem(i, 5, QTableWidgetItem(row['file_name']))
            self.table_widget.setItem(i, 6, QTableWidgetItem(row['folder_name']))
            self.table_widget.setItem(i, 7, QTableWidgetItem(row['date'].strftime("%Y/%m/%d")))

    def export_table(self):
        """Export table data to CSV."""
        if self.plot_df_cache is None or self.plot_df_cache.empty:
            QMessageBox.warning(self, "Warning", "No data to export")
            return
        fname, _ = QFileDialog.getSaveFileName(self, "Save CSV", "", "CSV (*.csv)")
        if fname:
            try:
                self.plot_df_cache.to_csv(fname, index=False)
                self.status_label.setText("Table exported successfully")
                logging.info(f"Table exported to {fname}")
            except Exception as e:
                logging.error(f"Error exporting table: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to export table: {str(e)}")

    def get_verification_value(self, crm_id, element):
        """Retrieve verification value from reference database."""
        cache_key = f"{crm_id}_{element}"
        if cache_key in self.verification_cache:
            logging.debug(f"Retrieved verification value from cache for {cache_key}: {self.verification_cache[cache_key]}")
            return self.verification_cache[cache_key]

        if not self.is_valid_crm_id(crm_id):
            logging.warning(f"Invalid CRM ID format: {crm_id}")
            self.verification_cache[cache_key] = None
            return None

        try:
            conn = sqlite3.connect(self.ver_db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            table_name = "oreas_hs j" if re.match(r'(?i)oreas', crm_id) else "pivot_crm"
            if table_name not in tables:
                logging.error(f"Table {table_name} does not exist in database")
                conn.close()
                QMessageBox.critical(self, "Error", f"Table {table_name} does not exist")
                self.verification_cache[cache_key] = None
                return None

            cursor.execute(f"PRAGMA table_info({table_name})")
            cols = [x[1] for x in cursor.fetchall()]
            if 'CRM ID' not in cols:
                logging.error(f"Column 'CRM ID' not found in {table_name}")
                conn.close()
                QMessageBox.critical(self, "Error", f"Column 'CRM ID' not found")
                self.verification_cache[cache_key] = None
                return None

            element_base = element.split()[0] if ' ' in element else element
            target_element = element if element in cols else element_base
            m = re.search(r'(?i)(?:CRM|OREAS)?\s*(\w+)(?:\s*par)?', crm_id)
            crm_id_part = m.group(1) if m else crm_id
            query = f"SELECT * FROM {table_name} WHERE [CRM ID] LIKE ?"
            cursor.execute(query, (f"%{crm_id_part}%",))
            crm_data = cursor.fetchall()

            if not crm_data:
                logging.warning(f"No CRM data found for {crm_id}")
                conn.close()
                self.verification_cache[cache_key] = None
                return None

            for row in crm_data:
                row_dict = {cols[i]: row[i] for i in range(len(cols))}
                label = str(row_dict['CRM ID']).strip().upper()
                if label.find(crm_id_part.upper()) != -1:
                    value = row_dict.get(target_element)
                    if value is not None and not pd.isna(value):
                        try:
                            value = float(value)
                            self.verification_cache[cache_key] = value
                            logging.debug(f"Verification value for CRM {crm_id}, Element {element}: {value}")
                            return value
                        except (ValueError, TypeError):
                            logging.warning(f"Invalid value for {target_element}: {value}")
                            continue

            logging.warning(f"No valid value for {target_element} in {table_name}")
            self.verification_cache[cache_key] = None
            return None
        except Exception as e:
            logging.error(f"Error querying database: {str(e)}")
            QMessageBox.critical(self, "Error", f"Error querying database: {str(e)}")
            self.verification_cache[cache_key] = None
            return None
        finally:
            if 'conn' in locals():
                conn.close()

    def select_best_blank(self, crm_row, blank_df, ver_value):
        """Select the best BLANK that brings CRM value closest to verification value."""
        if blank_df.empty or ver_value is None:
            return None, crm_row['value']
        
        relevant_blanks = blank_df[
            (blank_df['file_name'] == crm_row['file_name']) &
            (blank_df['folder_name'] == crm_row['folder_name']) &
            (blank_df['element'] == crm_row['element'])
        ]
        
        if relevant_blanks.empty:
            return None, crm_row['value']
        
        best_blank_value = None
        best_diff = float('inf')
        corrected_value = crm_row['value']
        
        for _, blank_row in relevant_blanks.iterrows():
            blank_value = blank_row['value']
            if pd.notna(blank_value):
                corrected = crm_row['value'] - blank_value
                diff = abs(corrected - ver_value)
                if diff < best_diff:
                    best_diff = diff
                    best_blank_value = blank_value
                    corrected_value = corrected
        
        return best_blank_value, corrected_value

    def plot_data(self):
        """Plot filtered data with control lines and optional blank correction."""
        self.plot_widget.clear()
        self.plot_data_items = []
        filtered_crm_df = self.filtered_crm_df_cache if self.filtered_crm_df_cache is not None else self.crm_df
        filtered_blank_df = self.filtered_blank_df_cache if self.filtered_blank_df_cache is not None else self.blank_df

        if filtered_crm_df.empty:
            self.status_label.setText("No CRM data to plot")
            logging.info("No CRM data to plot due to empty filtered dataframe")
            self.plot_df_cache = pd.DataFrame()
            self.update_table(self.plot_df_cache)
            return

        percentage = 10.0
        if validate_percentage(self.percentage_edit.text()):
            percentage = float(self.percentage_edit.text())
        else:
            logging.warning(f"Invalid percentage value: {self.percentage_edit.text()}, using default 10%")
            self.percentage_edit.setText("10")

        filtered_crm_df = filtered_crm_df.sort_values('date')
        current_element = self.element_combo.currentText()
        current_crm = self.crm_combo.currentText()
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEEAD', '#D4A5A5', '#9B59B6']
        plot_df = pd.DataFrame()

        crm_ids = [current_crm] if current_crm != "All CRM IDs" else filtered_crm_df['norm_crm_id'].unique()
        logging.debug(f"Plotting for CRM IDs: {crm_ids}")

        for idx, crm_id in enumerate(crm_ids):
            crm_df = filtered_crm_df[filtered_crm_df['norm_crm_id'] == crm_id]
            if crm_df.empty:
                logging.debug(f"No data for CRM ID {crm_id}")
                continue

            ver_value = self.get_verification_value(crm_id, current_element) if current_element != "All Elements" else None

            if current_element != "All Elements" and self.best_wl_check.isChecked() and ver_value is not None:
                def select_best(group):
                    group['diff'] = abs(group['value'] - ver_value)
                    return group.loc[group['diff'].idxmin()]
                crm_df = crm_df.groupby(['year', 'month', 'day']).apply(select_best).reset_index(drop=True)

            if self.apply_blank_check.isChecked() and current_element != "All Elements" and ver_value is not None:
                crm_df = crm_df.copy()
                crm_df['original_value'] = crm_df['value']
                crm_df['blank_value'] = None
                for i, row in crm_df.iterrows():
                    blank_value, corrected_value = self.select_best_blank(row, filtered_blank_df, ver_value)
                    crm_df.at[i, 'value'] = corrected_value
                    crm_df.at[i, 'blank_value'] = blank_value

            indices = np.arange(len(crm_df))
            values = crm_df['value'].values
            date_labels = [d.strftime("%Y/%m/%d") for d in crm_df['date']]
            logging.debug(f"CRM {crm_id}: {len(indices)} points, values range: {min(values, default=0):.2f} - {max(values, default=0):.2f}")

            pen = mkPen(color=colors[idx % len(colors)], width=2)
            plot_item = self.plot_widget.plot(indices, values, pen=pen, symbol='o', symbolSize=8, name=f"CRM {crm_id}")
            self.plot_data_items.append((plot_item, crm_df, indices, date_labels))
            logging.debug(f"Plotted {len(crm_df)} points for CRM ID {crm_id}")

            if current_element != "All Elements" and current_crm != "All CRM IDs":
                ver_value = self.get_verification_value(crm_id, current_element)
                if ver_value is not None and not pd.isna(ver_value):
                    x_range = [0, max(indices, default=0)]
                    delta = ver_value * (percentage / 100) / 3
                    self.plot_widget.plot(x_range, [ver_value * (1 - percentage / 100)] * 2, pen=mkPen('#FF6B6B', width=2, style=Qt.DotLine), name="LCL")
                    self.plot_widget.plot(x_range, [ver_value - 2 * delta] * 2, pen=mkPen('#4ECDC4', width=1, style=Qt.DotLine), name="-2LS")
                    self.plot_widget.plot(x_range, [ver_value - delta] * 2, pen=mkPen('#4ECDC4', width=1, style=Qt.DotLine), name="-1LS")
                    self.plot_widget.plot(x_range, [ver_value] * 2, py=mkPen('#000000', width=3, style=Qt.DashLine), name=f"Ref Value ({ver_value:.3f})")
                    self.plot_widget.plot(x_range, [ver_value + delta] * 2, pen=mkPen('#45B7D1', width=1, style=Qt.DotLine), name="1LS")
                    self.plot_widget.plot(x_range, [ver_value + 2 * delta] * 2, pen=mkPen('#45B7D1', width=1, style=Qt.DotLine), name="2LS")
                    self.plot_widget.plot(x_range, [ver_value * (1 + percentage / 100)] * 2, pen=mkPen('#FF6B6B', width=2, style=Qt.DotLine), name="UCL")
                    logging.info(f"Plotted control lines for CRM {crm_id}, Element {current_element}")

        self.plot_df_cache = pd.concat([plot_df, crm_df], ignore_index=True) if not crm_df.empty else plot_df
        self.update_table(self.plot_df_cache)
        self.plot_widget.showGrid(x=True, y=True)
        self.status_label.setText(f"Plotted {len(self.plot_df_cache)} records")
        logging.info(f"Plotted {len(self.plot_df_cache)} records")

    def on_mouse_clicked(self, event):
        """Show point info on click, including BLANK data."""
        if event.button() == Qt.LeftButton:
            pos = self.plot_widget.getViewBox().mapSceneToView(event.scenePos())
            x, y = pos.x(), pos.y()
            logging.debug(f"Click at view coordinates: x={x:.2f}, y={y:.2f}")
            closest_dist = float('inf')
            closest_info = None

            for plot_item, crm_df, indices, date_labels in self.plot_data_items:
                for i, (idx, value, date) in enumerate(zip(indices, crm_df['value'], date_labels)):
                    dist = ((idx - x) ** 2 + (value - y) ** 2) ** 0.5
                    logging.debug(f"Point {i}: index={idx}, value={value:.2f}, dist={dist:.2f}")
                    if dist < 10:
                        closest_dist = dist
                        element = crm_df.iloc[i]['element']
                        file_name = crm_df.iloc[i]['file_name']
                        folder_name = crm_df.iloc[i]['folder_name']
                        solution_label = crm_df.iloc[i]['solution_label']
                        blank_value = crm_df.iloc[i].get('blank_value')
                        original_value = crm_df.iloc[i].get('original_value', value)

                        blank_info = ""
                        if not self.filtered_blank_df_cache.empty:
                            relevant_blanks = self.filtered_blank_df_cache[
                                (self.filtered_blank_df_cache['file_name'] == file_name) &
                                (self.filtered_blank_df_cache['folder_name'] == folder_name) &
                                (self.filtered_blank_df_cache['element'] == element)
                            ]
                            if not relevant_blanks.empty:
                                blank_info = "\nBLANK Data:\n"
                                for _, blank_row in relevant_blanks.iterrows():
                                    blank_info += f"  - Solution Label: {blank_row['solution_label']}, Value: {blank_row['value']:.2f}\n"

                        closest_info = (
                            f"Element: {element}\n"
                            f"File: {file_name}\n"
                            f"Date: {date}\n"
                            f"Solution Label: {solution_label}\n"
                            f"Value: {value:.2f}\n"
                            f"Original Value: {original_value:.2f}\n" if blank_value is not None else f"Value: {value:.2f}\n"
                            f"Blank Value Applied: {blank_value:.2f}\n" if blank_value is not None else ""
                            f"{blank_info}"
                        )

            if closest_info:
                QMessageBox.information(self, "Point Info", closest_info)
                logging.debug(f"Clicked point info: {closest_info}")
            else:
                logging.debug("No point found near click position")

    def on_mouse_moved(self, pos):
        """Show tooltip on mouse hover, including BLANK info."""
        pos = self.plot_widget.getViewBox().mapSceneToView(pos)
        x, y = pos.x(), pos.y()
        closest_dist = float('inf')
        closest_info = None

        for plot_item, crm_df, indices, date_labels in self.plot_data_items:
            for i, (idx, value, date) in enumerate(zip(indices, crm_df['value'], date_labels)):
                dist = ((idx - x) ** 2 + (value - y) ** 2) ** 0.5
                if dist < 1:
                    closest_dist = dist
                    file_name = crm_df.iloc[i]['file_name']
                    folder_name = crm_df.iloc[i]['folder_name']
                    crm_id = crm_df.iloc[i]['norm_crm_id']
                    element = crm_df.iloc[i]['element']
                    solution_label = crm_df.iloc[i]['solution_label']
                    blank_value = crm_df.iloc[i].get('blank_value')
                    original_value = crm_df.iloc[i].get('original_value', value)

                    blank_info = ""
                    if not self.filtered_blank_df_cache.empty:
                        relevant_blanks = self.filtered_blank_df_cache[
                            (self.filtered_blank_df_cache['file_name'] == file_name) &
                            (self.filtered_blank_df_cache['folder_name'] == folder_name) &
                            (self.filtered_blank_df_cache['element'] == element)
                        ]
                        if not relevant_blanks.empty:
                            blank_info = "\nBLANK Data:\n"
                            for _, blank_row in relevant_blanks.iterrows():
                                blank_info += f"  - {blank_row['solution_label']}: {blank_row['value']:.2f}\n"

                    closest_info = (
                        f"CRM ID: {crm_id}\n"
                        f"Element: {element}\n"
                        f"Date: {date}\n"
                        f"Value: {value:.2f}\n"
                        f"Original Value: {original_value:.2f}\n" if blank_value is not None else f"Value: {value:.2f}\n"
                        f"Blank Value Applied: {blank_value:.2f}\n" if blank_value is not None else ""
                        f"Solution Label: {solution_label}\n"
                        f"File: {file_name}\n"
                        f"{blank_info}"
                    )

        if closest_info:
            self.tooltip_label.setText(closest_info)
            self.tooltip_label.adjustSize()
            self.tooltip_label.move(int(pos.x() * 10 + 10), int(pos.y() * 10 + 10))
            self.tooltip_label.setVisible(True)
        else:
            self.tooltip_label.setVisible(False)

    def save_plot(self):
        """Save plot as PNG with logo."""
        try:
            import pyqtgraph.exporters
            temp_file = 'temp_crm_plot.png'
            exporter = pyqtgraph.exporters.ImageExporter(self.plot_widget.getPlotItem())
            exporter.parameters()['width'] = 1200
            exporter.export(temp_file)
            im = Image.open(temp_file)
            if self.logo_path.exists():
                logo = Image.open(self.logo_path)
                logo = logo.resize((100, 100))
                box = (im.width - 110, 10)
                if logo.mode == 'RGBA':
                    im.paste(logo, box, logo)
                else:
                    im.paste(logo, box)
                im.save('crm_plot.png')
                import os
                os.remove(temp_file)
                self.status_label.setText("Plot saved as crm_plot.png")
                logging.info("Plot saved as crm_plot.png")
        except Exception as e:
            logging.error(f"Error saving plot: {str(e)}")
            self.status_label.setText("Failed to save plot")
            QMessageBox.critical(self, "Error", f"Failed to save plot: {str(e)}")

    def reset_filters(self):
        """Reset all filter inputs."""
        if self.updating_filters:
            return
        self.device_combo.setCurrentText("All Devices")
        self.element_combo.setCurrentText("All Elements")
        self.crm_combo.setCurrentText("All CRM IDs")
        self.from_date_edit.clear()
        self.to_date_edit.clear()
        self.percentage_edit.setText("10")
        self.best_wl_check.setChecked(True)
        self.apply_blank_check.setChecked(False)
        logging.debug("Filters reset")
        self.update_filters()

    def apply_styles(self):
        """Apply minimal custom styles, leveraging qfluentwidgets' built-in Fluent styling."""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #F5F6FA;
                font-family: 'Segoe UI', Arial, sans-serif;
            }
            QTableWidget {
                background-color: #FFFFFF;
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                gridline-color: #E0E0E0;
            }
            QTableWidget::item {
                padding: 8px;
            }
            QHeaderView::section {
                background-color: #0078D4;
                color: #FFFFFF;
                border: 1px solid #E0E0E0;
                padding: 8px;
                font-weight: bold;
                font-size: 14px;
            }
            QProgressBar {
                border: 1px solid #E0E0E0;
                border-radius: 4px;
                text-align: center;
                background-color: #FFFFFF;
                color: #000000;
            }
            QProgressBar::chunk {
                background-color: #0078D4;
                border-radius: 4px;
            }
            QLabel {
                color: #000000;
                font-size: 14px;
                font-family: 'Segoe UI';
            }
        """)
        self.plot_widget.setBackground('#FFFFFF')
        self.plot_widget.getAxis('bottom').setPen('#000000')
        self.plot_widget.getAxis('left').setPen('#000000')
        self.plot_widget.getAxis('bottom').setTextPen('#000000')
        self.plot_widget.getAxis('left').setTextPen('#000000')

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CRMDataVisualizer()
    window.show()
    sys.exit(app.exec_())