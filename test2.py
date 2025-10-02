import sqlite3
import pandas as pd
from pathlib import Path

# Function to check if a record already exists in the target database
def record_exists(cursor, crm_id, element, file_name, date):
    """Check if a record with given crm_id, element, file_name, and date exists."""
    query = '''
        SELECT COUNT(*) FROM crm_data 
        WHERE crm_id = ? AND element = ? AND file_name = ? AND date = ?
    '''
    cursor.execute(query, (crm_id, element, file_name, date))
    count = cursor.fetchone()[0]
    return count > 0

# Function to merge reference database into main database
def merge_databases(main_db_path, reference_db_path):
    try:
        # Connect to main database
        main_conn = sqlite3.connect(main_db_path)
        main_cursor = main_conn.cursor()

        # Ensure the crm_data table exists in the main database
        main_cursor.execute('''
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
        main_conn.commit()

        # Check if reference database exists
        if not Path(reference_db_path).exists():
            print(f"Reference database not found: {reference_db_path}")
            main_conn.close()
            return

        # Connect to reference database
        ref_conn = sqlite3.connect(reference_db_path)
        ref_cursor = ref_conn.cursor()

        # Check if crm_data table exists in reference database
        ref_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crm_data'")
        if not ref_cursor.fetchone():
            print(f"Table 'crm_data' not found in {reference_db_path}")
            ref_conn.close()
            main_conn.close()
            return

        # Read data from reference database
        ref_df = pd.read_sql_query("SELECT * FROM crm_data", ref_conn)
        print(f"Loaded {len(ref_df)} records from {reference_db_path}")

        # Process records
        inserted_count = 0
        skipped_count = 0
        for _, row in ref_df.iterrows():
            crm_id = row['crm_id']
            element = row['element']
            file_name = row['file_name']
            date = row['date']
            solution_label = row['solution_label']
            value = row['value']
            folder_name = row['folder_name']

            # Check for duplicate record
            if not record_exists(main_cursor, crm_id, element, file_name, date):
                main_cursor.execute('''
                    INSERT INTO crm_data (crm_id, solution_label, element, value, file_name, folder_name, date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    crm_id,
                    solution_label,
                    element,
                    value,
                    file_name,
                    folder_name,
                    date
                ))
                main_conn.commit()
                inserted_count += 1
                print(f"Inserted: CRM ID={crm_id}, Element={element}, Value={value}, File={file_name}")
            else:
                skipped_count += 1
                print(f"Skipped duplicate: CRM ID={crm_id}, Element={element}, File={file_name}, Date={date}")

        print(f"Completed merging: {inserted_count} records inserted, {skipped_count} records skipped")
        
        # Close connections
        ref_conn.close()
        main_conn.close()

    except Exception as e:
        print(f"Error merging databases: {str(e)}")
        if 'main_conn' in locals():
            main_conn.close()
        if 'ref_conn' in locals():
            ref_conn.close()

# Example usage
if __name__ == "__main__":
    main_db_path = "crm_database.db"
    reference_db_path = "crm_mass.db"
    merge_databases(main_db_path, reference_db_path)