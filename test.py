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
            # Normalize to YYYY-MM-DD
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

# Function to load Excel/CSV files
def load_excel(file_path):
    """Load and parse Excel/CSV file and return DataFrame."""
    file_path = Path(file_path)  # Ensure file_path is a Path object
    logger.info(f"Processing file: {file_path}")
    try:
        is_new_format = False
        if file_path.suffix.lower() == '.csv':
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    preview_lines = [f.readline().strip() for _ in range(10)]
                logger.debug(f"CSV preview (first 10 lines) for {file_path.name}:\n{preview_lines}")
                is_new_format = any("Sample ID:" in line for line in preview_lines) or \
                                any("Net Intensity" in line for line in preview_lines)
                logger.info(f"File {file_path.name} detected as {'new' if is_new_format else 'old'} format")
            except Exception as e:
                logger.warning(f"Could not read CSV preview for {file_path.name}: {str(e)}. Assuming new format.")
                is_new_format = True
        else:
            try:
                engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                preview = pd.read_excel(file_path, header=None, nrows=10, engine=engine)
                logger.debug(f"Excel preview (first 10 rows) for {file_path.name}:\n{preview.to_string()}")
                is_new_format = any(preview[0].str.contains("Sample ID:", na=False)) or \
                                any(preview[0].str.contains("Net Intensity", na=False))
                logger.info(f"File {file_path.name} detected as {'new' if is_new_format else 'old'} format")
            except Exception as e:
                logger.error(f"Error reading Excel preview for {file_path.name}: {str(e)}")
                raise
        
        data_rows = []
        current_sample = None

        if is_new_format:
            if file_path.suffix.lower() == '.csv':
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = list(csv.reader(f, delimiter=',', quotechar='"'))
                        total_rows = len(reader)
                        logger.debug(f"Total rows in CSV {file_path.name}: {total_rows}")
                        for idx, row in enumerate(reader):
                            # logger.debug(f"Processing row {idx}: {row}")
                            if idx == total_rows - 1:
                                logger.debug(f"Skipping last row {idx} in {file_path.name}")
                                continue
                            if not row or all(cell.strip() == "" for cell in row):
                                # logger.debug(f"Skipping empty row {idx} in {file_path.name}")
                                continue
                            
                            if len(row) > 0 and row[0].startswith("Sample ID:"):
                                current_sample = row[1].strip() if len(row) > 1 else "Unknown_Sample"
                                # logger.debug(f"Set current_sample to {current_sample} at row {idx}")
                                continue
                            
                            if len(row) > 0 and (row[0].startswith("Method File:") or row[0].startswith("Calibration File:")):
                                # logger.debug(f"Skipping metadata row {idx}: {row[0]}")
                                continue
                            
                            if current_sample is None:
                                current_sample = "Unknown_Sample"
                                logger.warning(f"No Sample ID found before row {idx}, using default: {current_sample}")
                            
                            element = split_element_name(row[0].strip())
                            try:
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
                                    # logger.debug(f"Added row: Solution Label={current_sample}, Element={element}, Int={intensity}, Corr Con={concentration}, Type={type_value}")
                                else:
                                    logger.warning(f"Skipping row {idx} in {file_path.name}: Non-numeric values - Int={row[1] if len(row) > 1 else 'N/A'}, Corr Con={row[5] if len(row) > 5 else 'N/A'}")
                            except Exception as e:
                                logger.error(f"Error processing row {idx} in {file_path.name}: {str(e)}")
                                continue
                except Exception as e:
                    logger.error(f"Failed to process CSV {file_path.name}: {str(e)}")
                    raise
            else:
                try:
                    engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                    raw_data = pd.read_excel(file_path, header=None, engine=engine)
                    total_rows = raw_data.shape[0]
                    logger.debug(f"Total rows in Excel {file_path.name}: {total_rows}")
                    for index, row in raw_data.iterrows():
                        # logger.debug(f"Processing row {index}: {row.tolist()}")
                        if index == total_rows - 1:
                            logger.debug(f"Skipping last row {index} in {file_path.name}")
                            continue
                        row_list = row.tolist()
                        
                        if any("No valid data found in the file" in str(cell) for cell in row_list):
                            logger.debug(f"Skipping row {index} with 'No valid data' in {file_path.name}")
                            continue
                        
                        if isinstance(row[0], str) and row[0].startswith("Sample ID:"):
                            current_sample = row[0].split("Sample ID:")[1].strip()
                            logger.debug(f"Set current_sample to {current_sample} at row {index}")
                            continue
                        
                        if isinstance(row[0], str) and (row[0].startswith("Method File:") or row[0].startswith("Calibration File:")):
                            logger.debug(f"Skipping metadata row {index}: {row[0]}")
                            continue
                        
                        if current_sample and pd.notna(row[0]):
                            element = split_element_name(str(row[0]).strip())
                            try:
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
                                    logger.debug(f"Added row: Solution Label={current_sample}, Element={element}, Int={intensity}, Corr Con={concentration}, Type={type_value}")
                                else:
                                    logger.warning(f"Skipping row {index} in {file_path.name}: Non-numeric values - Int={row[1] if pd.notna(row[1]) else 'N/A'}, Corr Con={row[5] if pd.notna(row[5]) else 'N/A'}")
                            except Exception as e:
                                # logger.error(f"Error processing row {index} in {file_path.name}: {str(e)}")
                                continue
                except Exception as e:
                    logger.error(f"Failed to process Excel {file_path.name}: {str(e)}")
                    raise
        else:
            if file_path.suffix.lower() == '.csv':
                try:
                    temp_df = pd.read_csv(file_path, header=None, nrows=1, on_bad_lines='skip')
                    logger.debug(f"CSV header preview for {file_path.name}: {temp_df.to_string()}")
                    if temp_df.iloc[0].notna().sum() == 1:
                        df = pd.read_csv(file_path, header=1, on_bad_lines='skip')
                    else:
                        df = pd.read_csv(file_path, header=0, on_bad_lines='skip')
                    logger.debug(f"Loaded CSV {file_path.name} with {len(df)} rows")
                except Exception as e:
                    logger.error(f"Could not parse CSV {file_path.name} as tabular format: {str(e)}")
                    raise ValueError(f"Could not parse CSV as tabular format: {str(e)}")
            else:
                try:
                    engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
                    temp_df = pd.read_excel(file_path, header=None, nrows=1, engine=engine)
                    logger.debug(f"Excel header preview for {file_path.name}: {temp_df.to_string()}")
                    if temp_df.iloc[0].notna().sum() == 1:
                        df = pd.read_excel(file_path, header=1, engine=engine)
                    else:
                        df = pd.read_excel(file_path, header=0, engine=engine)
                    logger.debug(f"Loaded Excel {file_path.name} with {len(df)} rows")
                except Exception as e:
                    logger.error(f"Could not parse Excel {file_path.name} as tabular format: {str(e)}")
                    raise ValueError(f"Could not parse Excel as tabular format: {str(e)}")
            
            df = df.iloc[:-1]
            logger.debug(f"Removed last row, remaining rows: {len(df)}")
            
            expected_columns = ["Solution Label", "Element", "Int", "Corr Con"]
            column_mapping = {"Sample ID": "Solution Label"}
            df.rename(columns=column_mapping, inplace=True)
            
            if not all(col in df.columns for col in expected_columns):
                missing_cols = set(expected_columns) - set(df.columns)
                logger.error(f"Required columns missing in {file_path.name}: {', '.join(missing_cols)}")
                raise ValueError(f"Required columns missing: {', '.join(missing_cols)}")
            
            df['Element'] = df['Element'].apply(split_element_name)
            logger.debug(f"Applied split_element_name to Element column in {file_path.name}")
            
            if 'Type' not in df.columns:
                df['Type'] = df['Solution Label'].apply(lambda x: "Blk" if "BLANK" in str(x).upper() else "Sample")
                logger.debug(f"Added Type column to {file_path.name}")
            
            # Filter out rows with non-numeric Corr Con
            df = df[df['Corr Con'].apply(is_numeric)].copy()
            df['Corr Con'] = df['Corr Con'].astype(float)
            logger.debug(f"Filtered non-numeric Corr Con, remaining rows: {len(df)}")
        
        if not data_rows and is_new_format:
            logger.error(f"No valid data found in {file_path.name}")
            raise ValueError("No valid data found in the file")
        elif is_new_format:
            df = pd.DataFrame(data_rows, columns=["Solution Label", "Element", "Int", "Corr Con", "Type"])
            df['Element'] = df['Element'].apply(split_element_name)
            df = df[df['Corr Con'].apply(is_numeric)].copy()
            df['Corr Con'] = df['Corr Con'].astype(float)
            logger.debug(f"Created DataFrame for new format with {len(df)} rows")
        
        logger.info(f"Successfully processed {file_path.name} with {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        return None

# Function to initialize SQLite database
def init_db(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crm_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crm_id TEXT,
                solution_label TEXT,
                element TEXT,
                value REAL,
                file_name TEXT,
                folder_name TEXT,
                date TEXT
            )
        ''')
        conn.commit()
        logger.info(f"Initialized database at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

# Function to validate CRM ID
def is_valid_crm_id(crm_id):
    """Validate CRM ID: 3 digits or 3 digits + optional letter (e.g., 258, 258b, 258 b)."""
    pattern = r'^(?:\s*CRM\s*)?(\d{3}(?:\s*[a-zA-Z])?)$'
    match = re.match(pattern, str(crm_id).strip(), re.IGNORECASE)
    is_valid = bool(match)
    # logger.debug(f"Checking CRM ID: {crm_id}, Valid: {is_valid}")
    return is_valid, match.group(1) if match else None

# Main function to process folders and extract CRM data
def process_folders(folder_paths, db_path, crm_ids):
    try:
        conn = init_db(db_path)
        cursor = conn.cursor()
        # Regex for CRM IDs
        crm_pattern = re.compile(r'^(?:\s*CRM\s*)?(\d{3}(?:\s*[a-zA-Z])?)$', re.IGNORECASE)
        # Regex for BLANK entries
        blank_pattern = re.compile(r'(?:CRM\s*)?(?:BLANK|BLNK)(?:S|s)?(?:\s+.*)?', re.IGNORECASE)
        logger.info(f"Starting to process folders: {folder_paths}")

        for folder in folder_paths:
            folder_path = Path(folder).resolve()  # Resolve to absolute path
            if not folder_path.exists():
                logger.warning(f"Folder not found: {folder_path}")
                continue

            # Find all .rep, .csv, .xlsx, .xls files recursively
            file_extensions = ['*.rep', '*.csv', '*.xlsx', '*.xls']
            files_to_process = []
            for ext in file_extensions:
                files_to_process.extend(folder_path.rglob(ext))
            
            if not files_to_process:
                logger.warning(f"No files found in {folder_path}")
                continue

            logger.info(f"Found {len(files_to_process)} files in {folder_path}")
            # Process .rep files (convert to .csv)
            for file_path in tqdm(files_to_process, desc=f"Converting .rep files in {folder_path}"):
                if file_path.suffix.lower() == '.rep':
                    try:
                        new_file_path = file_path.with_suffix('.csv')
                        if not new_file_path.exists():  # Avoid overwriting existing .csv
                            shutil.copy(file_path, new_file_path)
                            logger.debug(f"Converted {file_path} to {new_file_path}")
                        else:
                            logger.debug(f"Skipped conversion of {file_path}, CSV already exists")
                    except Exception as e:
                        logger.error(f"Error converting {file_path}: {str(e)}")
                        continue

            # Process .csv, .xlsx, .xls files
            for file_path in tqdm([f for f in files_to_process if f.suffix.lower() in ['.csv', '.xlsx', '.xls']], desc=f"Processing files in {folder_path}"):
                try:
                    # Check if file name starts with valid date format (YYYY-MM-DD or YYYY-MM-D)
                    if not re.match(r'^\d{4}-\d{2}-\d{1,2}', file_path.name):
                        logger.warning(f"Skipping file with invalid name format: {file_path.name}")
                        continue

                    # Get the middle part of the folder path
                    folder_parts = file_path.parent.parts
                    if len(folder_parts) >= 3:
                        folder_name = folder_parts[-2]  # Take the second-to-last part (e.g., 'mass' from 'New folder/mass/1 -2')
                    else:
                        folder_name = str(file_path.parent.relative_to(Path.cwd()))  # Fallback to relative path
                        logger.warning(f"Folder path {file_path.parent} has fewer than 3 parts, using relative path: {folder_name}")

                    # Check if file already exists in the database
                    cursor.execute('''
                        SELECT COUNT(*) FROM crm_data
                        WHERE file_name = ? AND folder_name = ?
                    ''', (file_path.name, folder_name))
                    file_count = cursor.fetchone()[0]
                    
                    if file_count > 0:
                        logger.info(f"Skipping file {file_path.name} in folder {folder_name} as it already exists in the database")
                        continue

                    date = extract_date(file_path.name)
                    logger.debug(f"Processing {file_path.name}, folder: {folder_name}, date: {date}")

                    df = load_excel(file_path)
                    if df is None or df.empty:
                        logger.warning(f"No data loaded from {file_path}")
                        continue

                    # Filter for Sample and Blk types (include both CRM and BLANK)
                    df_filtered = df[df['Type'].isin(['Samp', 'Sample', 'Blk'])].copy()
                    if df_filtered.empty:
                        logger.warning(f"No Sample or Blk data in {file_path}")
                        continue
                    logger.debug(f"Filtered {len(df_filtered)} Sample/Blk rows from {file_path}")

                    for _, row in df_filtered.iterrows():
                        solution_label = row['Solution Label']
                        # Check for BLANK first
                        blank_match = blank_pattern.search(str(solution_label))
                        if blank_match:
                            crm_id = "BLANK"  # Use "BLANK" as the identifier for BLANK samples
                            element = row['Element']
                            value = row['Corr Con']
                            if pd.notna(value):
                                cursor.execute('''
                                    INSERT INTO crm_data (crm_id, solution_label, element, value, file_name, folder_name, date)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (crm_id, solution_label, element, float(value), file_path.name, folder_name, date))
                                conn.commit()
                                logger.debug(f"Inserted BLANK: CRM ID={crm_id}, Solution Label={solution_label}, Element={element}, Value={value}, File={file_path.name}, Folder={folder_name}")
                            else:
                                logger.debug(f"Skipping BLANK with invalid value in {file_path.name}: Solution Label={solution_label}")
                            continue  # Skip to next row after processing BLANK
                        
                        # Check for CRM ID
                        crm_match = crm_pattern.search(str(solution_label))
                        if crm_match:
                            crm_id = crm_match.group(1)
                            is_valid, valid_crm_id = is_valid_crm_id(crm_id)
                            if is_valid:
                                element = row['Element']
                                value = row['Corr Con']
                                if pd.notna(value):
                                    cursor.execute('''
                                        INSERT INTO crm_data (crm_id, solution_label, element, value, file_name, folder_name, date)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                    ''', (valid_crm_id, solution_label, element, float(value), file_path.name, folder_name, date))
                                    conn.commit()
                                    logger.debug(f"Inserted CRM: CRM ID={valid_crm_id}, Solution Label={solution_label}, Element={element}, Value={value}, File={file_path.name}, Folder={folder_name}")
                                else:
                                    logger.debug(f"Skipping CRM with invalid value in {file_path.name}: Solution Label={solution_label}")
                            else:
                                logger.debug(f"Skipping invalid CRM ID: {crm_id} in {file_path.name}")
                        else:
                            logger.debug(f"No valid CRM or BLANK ID found in Solution Label: {solution_label} in {file_path.name}")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
                    continue

        conn.close()
        logger.info("Finished processing folders")

    except Exception as e:
        logger.error(f"Error in process_folders: {str(e)}")
        if 'conn' in locals():
            conn.close()
# Example usage
if __name__ == "__main__":
    # Replace with your actual folder paths
    folder_paths = [
        "New folder//mass",
        "New folder//oes 4ac",
        "New folder//oes fire"
    ]
    db_path = "crm_blank.db"
    crm_ids = ['258', '252', '906', '506', '233', '255', '263', '260']
    process_folders(folder_paths, db_path, crm_ids)