import pandas as pd
import csv
import os
import re
import sqlite3
from pathlib import Path
import shutil
from tqdm import tqdm
from persiantools.jdatetime import JalaliDate

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
    """Extract date from file_name like '1404-01-01'."""
    try:
        match = re.match(r'(\d{4}-\d{2}-\d{2})', file_name)
        if match:
            date_str = match.group(1)
            year, month, day = map(int, date_str.split('-'))
            return JalaliDate(year, month, day).strftime("%Y/%m/%d")
        return None
    except Exception as e:
        print(f"Error extracting date from {file_name}: {str(e)}")
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
    try:
        is_new_format = False
        file_path = str(file_path)  # Ensure file_path is string
        if file_path.lower().endswith('.csv'):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    preview_lines = [f.readline().strip() for _ in range(10)]
                is_new_format = any("Sample ID:" in line for line in preview_lines) or \
                                any("Net Intensity" in line for line in preview_lines)
            except Exception:
                is_new_format = True
        else:
            try:
                engine = 'openpyxl' if file_path.lower().endswith('.xlsx') else 'xlrd'
                preview = pd.read_excel(file_path, header=None, nrows=10, engine=engine)
                is_new_format = any(preview[0].str.contains("Sample ID:", na=False)) or \
                                any(preview[0].str.contains("Net Intensity", na=False))
            except Exception as e:
                raise
        
        data_rows = []
        current_sample = None

        if is_new_format:
            if file_path.lower().endswith('.csv'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = list(csv.reader(f, delimiter=',', quotechar='"'))
                        total_rows = len(reader)
                        for idx, row in enumerate(reader):
                            if idx == total_rows - 1:
                                continue
                            if not row or all(cell.strip() == "" for cell in row):
                                continue
                            
                            if len(row) > 0 and row[0].startswith("Sample ID:"):
                                current_sample = row[1].strip()
                                continue
                            
                            if len(row) > 0 and (row[0].startswith("Method File:") or row[0].startswith("Calibration File:")):
                                continue
                            
                            if current_sample is None:
                                current_sample = "Unknown_Sample"
                            
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
                                else:
                                    print(f"Skipping row in {file_path.name}: Non-numeric values - Int={row[1] if len(row) > 1 else 'N/A'}, Corr Con={row[5] if len(row) > 5 else 'N/A'}")
                            except Exception as e:
                                print(f"Error processing row in {file_path.name}: {str(e)}")
                                continue
                except Exception as e:
                    raise
            else:
                try:
                    engine = 'openpyxl' if file_path.lower().endswith('.xlsx') else 'xlrd'
                    raw_data = pd.read_excel(file_path, header=None, engine=engine)
                    total_rows = raw_data.shape[0]
                    for index, row in raw_data.iterrows():
                        if index == total_rows - 1:
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
                                else:
                                    print(f"Skipping row in {file_path.name}: Non-numeric values - Int={row[1] if pd.notna(row[1]) else 'N/A'}, Corr Con={row[5] if pd.notna(row[5]) else 'N/A'}")
                            except Exception as e:
                                print(f"Error processing row in {file_path.name}: {str(e)}")
                                continue
                except Exception as e:
                    raise
        else:
            if file_path.lower().endswith('.csv'):
                try:
                    temp_df = pd.read_csv(file_path, header=None, nrows=1, on_bad_lines='skip')
                    if temp_df.iloc[0].notna().sum() == 1:
                        df = pd.read_csv(file_path, header=1, on_bad_lines='skip')
                    else:
                        df = pd.read_csv(file_path, header=0, on_bad_lines='skip')
                except Exception as e:
                    raise ValueError(f"Could not parse CSV as tabular format: {str(e)}")
            else:
                try:
                    engine = 'openpyxl' if file_path.lower().endswith('.xlsx') else 'xlrd'
                    temp_df = pd.read_excel(file_path, header=None, nrows=1, engine=engine)
                    if temp_df.iloc[0].notna().sum() == 1:
                        df = pd.read_excel(file_path, header=1, engine=engine)
                    else:
                        df = pd.read_excel(file_path, header=0, engine=engine)
                except Exception as e:
                    raise ValueError(f"Could not parse Excel as tabular format: {str(e)}")
            
            df = df.iloc[:-1]
            
            expected_columns = ["Solution Label", "Element", "Int", "Corr Con"]
            column_mapping = {"Sample ID": "Solution Label"}
            df.rename(columns=column_mapping, inplace=True)
            
            if not all(col in df.columns for col in expected_columns):
                raise ValueError(f"Required columns missing: {', '.join(set(expected_columns) - set(df.columns))}")
            
            df['Element'] = df['Element'].apply(split_element_name)
            
            if 'Type' not in df.columns:
                df['Type'] = df['Solution Label'].apply(lambda x: "Blk" if "BLANK" in str(x).upper() else "Sample")
            
            # Filter out rows with non-numeric Corr Con
            df = df[df['Corr Con'].apply(is_numeric)].copy()
            df['Corr Con'] = df['Corr Con'].astype(float)
        
        if not data_rows and is_new_format:
            raise ValueError("No valid data found in the file")
        elif is_new_format:
            df = pd.DataFrame(data_rows, columns=["Solution Label", "Element", "Int", "Corr Con", "Type"])
            df['Element'] = df['Element'].apply(split_element_name)
            df = df[df['Corr Con'].apply(is_numeric)].copy()
            df['Corr Con'] = df['Corr Con'].astype(float)
        
        return df
        
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")
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
        return conn
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        raise

# Function to validate CRM ID
def is_valid_crm_id(crm_id):
    """Validate CRM ID: 3 digits or 3 digits + optional letter (e.g., 258, 258b, 258 b)."""
    pattern = r'^(?:\s*CRM\s*)?(\d{3}(?:\s*[a-zA-Z])?)$'
    match = re.match(pattern, str(crm_id).strip(), re.IGNORECASE)
    is_valid = bool(match)
    # print(f"Checking CRM ID: {crm_id}, Valid: {is_valid}")
    return is_valid, match.group(1) if match else None

# Main function to process folders and extract CRM data
def process_folders(folder_paths, db_path, crm_ids):
    try:
        conn = init_db(db_path)
        # Regex to match valid CRM IDs
        crm_pattern = re.compile(r'^(?:\s*CRM\s*)?(\d{3}(?:\s*[a-zA-Z])?)$', re.IGNORECASE)

        for folder in folder_paths:
            folder_path = Path(folder).resolve()  # Resolve to absolute path
            if not folder_path.exists():
                print(f"Folder not found: {folder_path}")
                continue

            # Find all .rep, .csv, .xlsx, .xls files recursively
            file_extensions = ['*.rep', '*.csv', '*.xlsx', '*.xls']
            files_to_process = []
            for ext in file_extensions:
                files_to_process.extend(folder_path.rglob(ext))
            
            if not files_to_process:
                print(f"No files found in {folder_path}")
                continue

            # Process .rep files (convert to .csv)
            for file_path in tqdm(files_to_process, desc=f"Converting .rep files in {folder_path}"):
                if file_path.suffix.lower() == '.rep':
                    try:
                        new_file_path = file_path.with_suffix('.csv')
                        if not new_file_path.exists():  # Avoid overwriting existing .csv
                            shutil.copy(file_path, new_file_path)
                    except Exception as e:
                        print(f"Error converting {file_path}: {str(e)}")
                        continue

            # Process .csv, .xlsx, .xls files
            for file_path in tqdm([f for f in files_to_process if f.suffix.lower() in ['.csv', '.xlsx', '.xls']], desc=f"Processing files in {folder_path}"):
                try:
                    # Check if file name starts with valid date format
                    if not re.match(r'^\d{4}-\d{2}-\d{2}', file_path.name):
                        print(f"Skipping file with invalid name format: {file_path.name}")
                        continue

                    # Get the relative path of the folder containing the file
                    folder_name = str(file_path.parent.relative_to(Path.cwd()))
                    date = extract_date(file_path.name)

                    df = load_excel(file_path)
                    if df is None or df.empty:
                        print(f"No data loaded from {file_path}")
                        continue

                    # Filter for Sample type
                    df_filtered = df[df['Type'].isin(['Samp', 'Sample'])].copy()
                    if df_filtered.empty:
                        print(f"No Sample data in {file_path}")
                        continue

                    for _, row in df_filtered.iterrows():
                        solution_label = row['Solution Label']
                        match = crm_pattern.search(str(solution_label))
                        if match:
                            crm_id = match.group(1)
                            is_valid, valid_crm_id = is_valid_crm_id(crm_id)
                            if is_valid:
                                element = row['Element']
                                value = row['Corr Con']
                                if pd.notna(value):
                                    cursor = conn.cursor()
                                    cursor.execute('''
                                        INSERT INTO crm_data (crm_id, solution_label, element, value, file_name, folder_name, date)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                    ''', (valid_crm_id, solution_label, element, float(value), file_path.name, folder_name, date))
                                    conn.commit()
                                    #print(f"Inserted: CRM ID={valid_crm_id}, Element={element}, Value={value}, File={file_path.name}")
                            else:
                                pass
                                #print(f"Skipping invalid CRM ID: {crm_id}")
                        else:
                            pass
                            # print(f"No valid CRM ID found in Solution Label: {solution_label}")
                except Exception as e:
                    # print(f"Error processing {file_path}: {str(e)}")
                    continue

        conn.close()

    except Exception as e:
        print(f"Error in process_folders: {str(e)}")
        if 'conn' in locals():
            conn.close()

# Example usage
if __name__ == "__main__":
    # Replace with your actual folder paths
    folder_paths = [
        "New folder/1404 mass",
        "New folder/1404 oes 4ac",
        "New folder/1404 oes fire"
    ]
    db_path = "crm_database.db"
    crm_ids = ['258', '252', '906', '506', '233', '255', '263', '260']
    process_folders(folder_paths, db_path, crm_ids)