import pandas as pd
import csv
import os
import re
import sqlite3
from pathlib import Path
import shutil
from tqdm import tqdm
from persiantools.jdatetime import JalaliDate
import logging
import math
from functools import reduce

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crm_processing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Placeholder for oxide_factors (define as needed)
oxide_factors = {}  # Example: {'Si': ('SiO2', 2.1393), 'Al': ('Al2O3', 1.8895), ...}

# Function to split element name
def split_element_name(element):
    """Split element name like 'Ce140' into 'Ce 140'."""
    if not isinstance(element, str):
        return element
    match = re.match(r'^([A-Za-z]+)(\d+\.?\d*)$', element.strip())
    if match:
        symbol, number = match.groups()
        return f"{symbol} {number}"
    return element

# Function to extract date from file_name
def extract_date(file_name):
    """Extract date from file_name like '1404-01-01' or '1404-01-1'."""
    try:
        match = re.match(r'(\d{4}-\d{2}-\d{1,2})', file_name)
        if match:
            date_str = match.group(1)
            year, month, day = map(int, date_str.split('-'))
            date_str = f"{year:04d}-{month:02d}-{day:02d}"
            year, month, day = map(int, date_str.split('-'))
            logger.debug(f"Extracted and normalized date from {file_name}: {date_str}")
            return JalaliDate(year, month, day).strftime("%Y/%m/%d")
        logger.warning(f"No valid date found in filename: {file_name}")
        return None
    except Exception as e:
        logger.error(f"Error extracting date from {file_name}: {str(e)}")
        return None

# Function to check if a value is numeric
def is_numeric(value):
    """Check if a value can be converted to float."""
    if value is None or str(value).strip() == "":
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

# Function to extract device name from folder path
def extract_device_name(folder_path):
    """Extract device name from folder path like 'New folder\oes 4ac\1'."""
    try:
        parts = str(folder_path).split(os.sep)
        if len(parts) >= 2:
            device_name = parts[-2]  # Get the second-to-last part (e.g., 'oes 4ac')
            logger.debug(f"Extracted device name: {device_name} from {folder_path}")
            return device_name
        logger.warning(f"Could not extract device name from {folder_path}")
        return str(folder_path)
    except Exception as e:
        logger.error(f"Error extracting device name from {folder_path}: {str(e)}")
        return str(folder_path)

# Function to sanitize table name
def sanitize_table_name(name):
    """Sanitize device name to be a valid SQL table name."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()

# Function to load Excel/CSV files
def load_excel(file_path):
    """Load and parse Excel/CSV file and return DataFrame."""
    file_path = Path(file_path)
    logger.info(f"Processing file: {file_path}")
    try:
        is_new_format = False
        if file_path.suffix.lower() == '.csv':
            with open(file_path, 'r', encoding='utf-8') as f:
                preview_lines = [f.readline().strip() for _ in range(10)]
            is_new_format = any("Sample ID:" in line for line in preview_lines) or \
                            any("Net Intensity" in line for line in preview_lines)
            logger.info(f"File {file_path.name} detected as {'new' if is_new_format else 'old'} format")
        else:
            engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
            preview = pd.read_excel(file_path, header=None, nrows=10, engine=engine)
            is_new_format = any(preview[0].str.contains("Sample ID:", na=False)) or \
                            any(preview[0].str.contains("Net Intensity", na=False))
            logger.info(f"File {file_path.name} detected as {'new' if is_new_format else 'old'} format")
        
        data_rows = []
        current_sample = None

        if is_new_format:
            if file_path.suffix.lower() == '.csv':
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = list(csv.reader(f, delimiter=',', quotechar='"'))
                    for idx, row in enumerate(reader):
                        if idx == len(reader) - 1:
                            continue
                        if not row or all(cell.strip() == "" for cell in row):
                            continue
                        if row[0].startswith("Sample ID:"):
                            current_sample = row[1].strip() if len(row) > 1 else "Unknown_Sample"
                            continue
                        if row[0].startswith("Method File:") or row[0].startswith("Calibration File:"):
                            continue
                        if current_sample is None:
                            current_sample = "Unknown_Sample"
                        element = split_element_name(row[0].strip())
                        intensity = float(row[1]) if len(row) > 1 and is_numeric(row[1]) else None
                        concentration = float(row[5]) if len(row) > 5 and is_numeric(row[5]) else None
                        if intensity is not None or concentration is not None:
                            type_value = "Blk" if "BLANK" in current_sample.upper() else "Sample"
                            data_rows.append({
                                "Solution Label": current_sample,
                                "Element": element,
                                "Int": intensity,
                                "Corr Con": concentration,
                                "Type": type_value
                            })
            else:
                engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                raw_data = pd.read_excel(file_path, header=None, engine=engine)
                for index, row in raw_data.iterrows():
                    if index == raw_data.shape[0] - 1:
                        continue
                    row_list = row.tolist()
                    if any("No valid data found in the file" in str(cell) for cell in row_list):
                        continue
                    if isinstance(row[0], str) and row[0].startswith("Sample ID:"):
                        current_sample = row[0].split("Sample ID:")[1].strip()
                        continue
                    if isinstance(row[0], str) and (row[0].startswith("Method File:") or row[0].startswith("Calibration File:")):
                        continue
                    if current_sample and pd.notna(row[0]):
                        element = split_element_name(str(row[0]).strip())
                        intensity = float(row[1]) if pd.notna(row[1]) and is_numeric(row[1]) else None
                        concentration = float(row[5]) if pd.notna(row[5]) and is_numeric(row[5]) else None
                        if intensity is not None or concentration is not None:
                            type_value = "Blk" if "BLANK" in current_sample.upper() else "Sample"
                            data_rows.append({
                                "Solution Label": current_sample,
                                "Element": element,
                                "Int": intensity,
                                "Corr Con": concentration,
                                "Type": type_value
                            })
        else:
            if file_path.suffix.lower() == '.csv':
                temp_df = pd.read_csv(file_path, header=None, nrows=1, on_bad_lines='skip')
                df = pd.read_csv(file_path, header=1 if temp_df.iloc[0].notna().sum() == 1 else 0, on_bad_lines='skip')
            else:
                engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                temp_df = pd.read_excel(file_path, header=None, nrows=1, engine=engine)
                df = pd.read_excel(file_path, header=1 if temp_df.iloc[0].notna().sum() == 1 else 0, engine=engine)
            df = df.iloc[:-1]
            column_mapping = {"Sample ID": "Solution Label"}
            df.rename(columns=column_mapping, inplace=True)
            expected_columns = ["Solution Label", "Element", "Int", "Corr Con"]
            if not all(col in df.columns for col in expected_columns):
                missing_cols = set(expected_columns) - set(df.columns)
                raise ValueError(f"Required columns missing: {', '.join(missing_cols)}")
            df['Element'] = df['Element'].apply(split_element_name)
            if 'Type' not in df.columns:
                df['Type'] = df['Solution Label'].apply(lambda x: "Blk" if "BLANK" in str(x).upper() else "Sample")
            df = df[df['Corr Con'].apply(is_numeric)].copy()
            df['Corr Con'] = df['Corr Con'].astype(float)
        
        if not data_rows and is_new_format:
            raise ValueError("No valid data found in the file")
        elif is_new_format:
            df = pd.DataFrame(data_rows, columns=["Solution Label", "Element", "Int", "Corr Con", "Type"])
            df['Element'] = df['Element'].apply(split_element_name)
            df = df[df['Corr Con'].apply(is_numeric)].copy()
            df['Corr Con'] = df['Corr Con'].astype(float)
        
        logger.info(f"Successfully processed {file_path.name} with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        return None

# Function to initialize SQLite database
def init_db(db_path):
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Initialized database at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

# Function to create device-specific table if not exists
def create_device_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_type TEXT,
            crm_id TEXT,
            solution_label TEXT,
            file_name TEXT,
            folder_name TEXT,
            date TEXT,
            analysis_method TEXT
        )
    ''')
    conn.commit()
    logger.info(f"Created or verified table {table_name}")

# Function to validate CRM ID
def is_valid_crm_id(crm_id):
    """Validate CRM ID: 3 digits or 3 digits + optional letter (e.g., 258, 258b, 258 b)."""
    pattern = r'^(?:\s*CRM\s*)?(\d{3}(?:\s*[a-zA-Z])?)$'
    match = re.match(pattern, str(crm_id).strip(), re.IGNORECASE)
    is_valid = bool(match)
    logger.debug(f"Checking CRM ID: {crm_id}, Valid: {is_valid}")
    return is_valid, match.group(1) if match else None

# Function to create pivot table for CRM or blank data
def create_pivot(df, use_intensity=False, use_oxide=False):
    """Create pivot table from DataFrame, aligned with PivotCreator logic."""
    try:
        df_filtered = df.copy()
        if df_filtered.empty:
            logger.warning("No data found after filtering!")
            return None, None

        df_filtered['original_index'] = df_filtered.index
        df_filtered = df_filtered.reset_index(drop=True)

        def calculate_set_size(solution_label, df_subset):
            counts = df_subset['Element'].value_counts().values
            if len(counts) > 0:
                g = reduce(math.gcd, counts)
                total_rows = len(df_subset)
                if g > 0 and total_rows % g == 0:
                    most_common_size = total_rows // g
                else:
                    most_common_size = total_rows
            else:
                most_common_size = 1
            return most_common_size

        most_common_sizes = {}
        for solution_label in df_filtered['Solution Label'].unique():
            df_subset = df_filtered[df_filtered['Solution Label'] == solution_label]
            most_common_sizes[solution_label] = calculate_set_size(solution_label, df_subset)

        df_filtered['set_size'] = df_filtered['Solution Label'].map(most_common_sizes)
        element_counts = df_filtered.groupby(['Solution Label', df_filtered.groupby('Solution Label').cumcount() // df_filtered['set_size'], 'Element']).size().reset_index(name='count')
        has_repeats = (element_counts['count'] > 1).any()

        def clean_label(label):
            m = re.search(r'(\d+)', str(label).replace(' ', ''))
            if m:
                return f"{label.split()[0]} {m.group(1)}"
            return label

        if not has_repeats:
            df_filtered['Element'] = df_filtered['Element'].str.split('_').str[0]
            df_filtered['unique_id'] = df_filtered.groupby(['Solution Label', 'Element']).cumcount()
            solution_label_order = sorted(df_filtered['Solution Label'].drop_duplicates().apply(clean_label).unique().tolist())
            element_order = df_filtered['Element'].drop_duplicates().tolist()

            value_column = 'Int' if use_intensity else 'Corr Con'
            if value_column not in df_filtered.columns:
                logger.error(f"Column '{value_column}' not found in data!")
                return None, None

            pivot_df = df_filtered.pivot_table(
                index=['Solution Label', 'unique_id'],
                columns='Element',
                values=value_column,
                aggfunc='first',
                sort=False
            ).reset_index()
            pivot_df = pivot_df.merge(
                df_filtered[['original_index', 'Solution Label', 'unique_id']],
                on=['Solution Label', 'unique_id'],
                how='left'
            ).sort_values('original_index').drop(columns=['original_index', 'unique_id']).drop_duplicates()
        else:
            df_filtered['group_id'] = 0
            for solution_label in df_filtered['Solution Label'].unique():
                df_subset = df_filtered[df_filtered['Solution Label'] == solution_label].copy()
                expected_size = most_common_sizes.get(solution_label, 1)
                df_subset['group_id'] = df_subset.groupby('Solution Label').cumcount() // expected_size
                df_filtered.loc[df_filtered['Solution Label'] == solution_label, 'group_id'] = df_subset['group_id']

            element_counts = df_filtered.groupby(['Solution Label', 'group_id', 'Element']).size().reset_index(name='count')
            df_filtered = df_filtered.merge(
                element_counts[['Solution Label', 'group_id', 'Element', 'count']],
                on=['Solution Label', 'group_id', 'Element'],
                how='left'
            )
            df_filtered['count'] = df_filtered['count'].fillna(1).astype(int)
            df_filtered['element_count'] = df_filtered.groupby(['Solution Label', 'group_id', 'Element']).cumcount() + 1

            df_filtered['Element_with_id'] = df_filtered.apply(
                lambda x: f"{x['Element']}_{x['element_count']}" if x['count'] > 1 else x['Element'],
                axis=1
            )

            expected_columns_dict = {}
            for solution_label in df_filtered['Solution Label'].unique():
                expected_size = most_common_sizes.get(solution_label, 1)
                set_sizes_subset = df_filtered[df_filtered['Solution Label'] == solution_label].groupby('group_id').size().reset_index(name='set_size')
                valid_groups = set_sizes_subset[set_sizes_subset['set_size'] == expected_size]['group_id']
                if not valid_groups.empty:
                    first_group_id = valid_groups.min()
                    first_set_elements = df_filtered[
                        (df_filtered['Solution Label'] == solution_label) & 
                        (df_filtered['group_id'] == first_group_id)
                    ]['Element_with_id'].unique().tolist()
                    expected_columns_dict[solution_label] = first_set_elements
                else:
                    expected_columns_dict[solution_label] = []

            solution_label_order = df_filtered[['Solution Label', 'group_id']].drop_duplicates().sort_values('group_id')['Solution Label'].apply(clean_label).tolist()
            element_order = list(set().union(*expected_columns_dict.values()))

            value_column = 'Int' if use_intensity else 'Corr Con'
            if value_column not in df_filtered.columns:
                logger.error(f"Column '{value_column}' not found in data!")
                return None, None

            pivot_dfs = []
            min_index_per_group = {}
            for solution_label, expected_columns in expected_columns_dict.items():
                if not expected_columns:
                    continue
                df_subset = df_filtered[df_filtered['Solution Label'] == solution_label].copy()
                min_index_per_group[solution_label] = df_subset.groupby('group_id')['original_index'].min().to_dict()
                pivot_subset = df_subset.pivot_table(
                    index=['Solution Label', 'group_id'],
                    columns='Element_with_id',
                    values=value_column,
                    aggfunc='first',
                    sort=False
                )
                pivot_subset = pivot_subset.reset_index()
                pivot_subset = pivot_subset.reindex(columns=['Solution Label', 'group_id'] + expected_columns)
                pivot_subset['min_original_index'] = pivot_subset['group_id'].map(min_index_per_group[solution_label])
                pivot_dfs.append(pivot_subset)

            if not pivot_dfs:
                logger.error("No valid pivot tables created!")
                return None, None
            pivot_df = pd.concat(pivot_dfs, ignore_index=False)
            if 'min_original_index' in pivot_df.columns:
                pivot_df = pivot_df.sort_values(by='min_original_index').reset_index(drop=True)
            columns_to_drop = [col for col in ['group_id', 'min_original_index'] if col in pivot_df.columns]
            if columns_to_drop:
                pivot_df = pivot_df.drop(columns=columns_to_drop)

        if use_oxide:
            rename_dict = {}
            for col in pivot_df.columns:
                if col != 'Solution Label':
                    element = col.split()[0]
                    if element in oxide_factors:
                        oxide_formula, factor = oxide_factors[element]
                        suffix = col.split('_')[-1] if '_' in col and has_repeats else ''
                        new_col = f"{oxide_formula}_{suffix}" if suffix else oxide_formula
                        rename_dict[col] = new_col
                        pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce') * factor
            pivot_df.rename(columns=rename_dict, inplace=True)
            element_order = [rename_dict.get(col, col) for col in element_order]

        return pivot_df, element_order
    except Exception as e:
        logger.error(f"Failed to create pivot table: {str(e)}")
        return None, None

# Function to add element columns to a database table
def add_element_columns(conn, table_name, element_columns):
    """Add new element columns to the specified table if they don't exist."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
    existing_columns = {col[1] for col in cursor.fetchall()}
    
    for element in element_columns:
        if element not in existing_columns:
            # Sanitize column name to avoid SQL injection
            element_safe = element.replace('"', '""')
            try:
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{element_safe}" REAL')
                logger.debug(f"Added column {element} to {table_name} table")
            except Exception as e:
                logger.error(f"Error adding column {element} to {table_name} table: {str(e)}")
    
    conn.commit()

# Function to export pivoted tables to CSV
def export_pivot_tables(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get all table names (assuming they are device-specific)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']  # Exclude system tables

        for table_name in tables:
            # Export each device table to a separate CSV
            cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
            columns = [col[1] for col in cursor.fetchall()]
            columns_str = ', '.join([f'"{col}"' for col in columns])
            cursor.execute(f"SELECT {columns_str} FROM \"{table_name}\"")
            rows = cursor.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=columns)
                output_path = f"{table_name}.csv"
                df.to_csv(output_path, index=False, encoding='utf-8')
                logger.info(f"Exported table {table_name} to {output_path}")
            else:
                logger.warning(f"No data found in table {table_name}")

        conn.close()
    except Exception as e:
        logger.error(f"Error exporting pivot tables: {str(e)}")
        if 'conn' in locals():
            conn.close()

# Main function to process folders and extract CRM and blank data
def process_folders(folder_paths, db_path, crm_ids, use_intensity=False, use_oxide=False):
    try:
        conn = init_db(db_path)
        cursor = conn.cursor()
        crm_pattern = re.compile(r'^(?:\s*CRM\s*)?(\d{3}(?:\s*[a-zA-Z])?)$', re.IGNORECASE)
        logger.info(f"Starting to process folders: {folder_paths}")

        for folder in folder_paths:
            folder_path = Path(folder).resolve()
            if not folder_path.exists():
                logger.warning(f"Folder not found: {folder_path}")
                continue

            file_extensions = ['*.rep', '*.csv', '*.xlsx', '*.xls']
            files_to_process = []
            for ext in file_extensions:
                files_to_process.extend(folder_path.rglob(ext))
            
            if not files_to_process:
                logger.warning(f"No files found in {folder_path}")
                continue

            logger.info(f"Found {len(files_to_process)} files in {folder_path}")
            for file_path in tqdm(files_to_process, desc=f"Converting .rep files in {folder_path}"):
                if file_path.suffix.lower() == '.rep':
                    try:
                        new_file_path = file_path.with_suffix('.csv')
                        if not new_file_path.exists():
                            shutil.copy(file_path, new_file_path)
                            logger.debug(f"Converted {file_path} to {new_file_path}")
                    except Exception as e:
                        logger.error(f"Error converting {file_path}: {str(e)}")
                        continue

            for file_path in tqdm([f for f in files_to_process if f.suffix.lower() in ['.csv', '.xlsx', '.xls']], desc=f"Processing files in {folder_path}"):
                try:
                    if not re.match(r'^\d{4}-\d{2}-\d{1,2}', file_path.name):
                        logger.warning(f"Skipping file with invalid name format: {file_path.name}")
                        continue

                    folder_name = extract_device_name(file_path.parent)
                    date = extract_date(file_path.name)
                    df = load_excel(file_path)
                    if df is None or df.empty:
                        logger.warning(f"No data loaded from {file_path}")
                        continue

                    # Create pivot table for CRM data
                    crm_df_filtered = df[df['Type'].isin(['Samp', 'Sample'])].copy()
                    crm_pivot_df, crm_element_order = create_pivot(crm_df_filtered, use_intensity=use_intensity, use_oxide=use_oxide)

                    # Create pivot table for blank data
                    blank_df_filtered = df[
                        df['Solution Label'].str.contains(
                            r'(?:CRM\s*)?(?:BLANK|BLNK)(?:S|s)?(?:\s+.*)?',
                            case=False, na=False, regex=True
                        )
                    ].copy()
                    blank_pivot_df, blank_element_order = create_pivot(blank_df_filtered, use_intensity=use_intensity, use_oxide=use_oxide)

                    if crm_pivot_df is None and blank_pivot_df is None:
                        logger.warning(f"No pivot data created for {file_path}")
                        continue

                    # Sanitize table name based on folder_name (device name)
                    table_name = sanitize_table_name(folder_name)
                    create_device_table(conn, table_name)

                    # Add element columns to the device table (union of CRM and blank elements)
                    all_element_order = list(set(crm_element_order or []) | set(blank_element_order or []))
                    add_element_columns(conn, table_name, all_element_order)

                    # Add metadata to pivot DataFrames
                    if crm_pivot_df is not None:
                        crm_pivot_df['data_type'] = 'CRM'
                        crm_pivot_df['file_name'] = file_path.name
                        crm_pivot_df['folder_name'] = folder_name
                        crm_pivot_df['date'] = date
                        crm_pivot_df['analysis_method'] = 'Various analytical methods'

                        # Add CRM ID column
                        crm_pivot_df['crm_id'] = crm_pivot_df['Solution Label'].apply(
                            lambda x: crm_pattern.search(str(x)).group(1) if crm_pattern.search(str(x)) else None
                        )
                        crm_pivot_df = crm_pivot_df[crm_pivot_df['crm_id'].notna()]

                        # Store CRM data
                        for _, row in crm_pivot_df.iterrows():
                            solution_label = row['Solution Label']
                            crm_id = row['crm_id']
                            is_valid, valid_crm_id = is_valid_crm_id(crm_id)
                            if is_valid:
                                columns = ['data_type', 'crm_id', 'solution_label', 'file_name', 'folder_name', 'date', 'analysis_method'] + all_element_order
                                values = ['CRM', valid_crm_id, solution_label, row['file_name'], row['folder_name'], row['date'], row['analysis_method']] + \
                                         [float(row.get(element, None)) if pd.notna(row.get(element, None)) else None for element in all_element_order]
                                
                                columns_str = ', '.join([f'"{col}"' for col in columns])
                                placeholders = ', '.join(['?' for _ in columns])
                                
                                cursor.execute(f'''
                                    INSERT INTO "{table_name}" ({columns_str})
                                    VALUES ({placeholders})
                                ''', values)
                                logger.debug(f"Inserted CRM data: Solution Label={solution_label}, File={file_path.name}, Table={table_name}")

                    if blank_pivot_df is not None:
                        blank_pivot_df['data_type'] = 'Blank'
                        blank_pivot_df['crm_id'] = None  # No CRM ID for blanks
                        blank_pivot_df['file_name'] = file_path.name
                        blank_pivot_df['folder_name'] = folder_name
                        blank_pivot_df['date'] = date
                        blank_pivot_df['analysis_method'] = None  # No analysis method for blanks

                        # Store blank data
                        for _, row in blank_pivot_df.iterrows():
                            solution_label = row['Solution Label']
                            columns = ['data_type', 'crm_id', 'solution_label', 'file_name', 'folder_name', 'date', 'analysis_method'] + all_element_order
                            values = ['Blank', None, solution_label, row['file_name'], row['folder_name'], row['date'], None] + \
                                     [float(row.get(element, None)) if pd.notna(row.get(element, None)) else None for element in all_element_order]
                            
                            columns_str = ', '.join([f'"{col}"' for col in columns])
                            placeholders = ', '.join(['?' for _ in columns])
                            
                            cursor.execute(f'''
                                INSERT INTO "{table_name}" ({columns_str})
                                VALUES ({placeholders})
                            ''', values)
                            logger.debug(f"Inserted Blank data: Solution Label={solution_label}, File={file_path.name}, Table={table_name}")
                    
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
                    continue

        # Export all device tables to CSV
        export_pivot_tables(db_path)
        logger.info("Finished processing folders")
        conn.close()
    except Exception as e:
        logger.error(f"Error in process_folders: {str(e)}")
        if 'conn' in locals():
            conn.close()

# Example usage
if __name__ == "__main__":
    folder_paths = ["New folder//1404 mass", "New folder//oes 4ac", "New folder//oes fire"]
    db_path = "crm_mass1.db"
    crm_ids = ['258', '252', '906', '506', '233', '255', '263', '260']
    process_folders(folder_paths, db_path, crm_ids, use_intensity=False, use_oxide=False)