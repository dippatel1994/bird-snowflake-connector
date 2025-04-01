#!/usr/bin/env python3
"""
Spider CSV to Snowflake Uploader

This script creates tables and uploads data from output_sql and output_csv folders 
to an existing Snowflake database using external browser authentication.
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging
import sys
from tqdm import tqdm
import importlib
import time
import re
import numpy as np # Import numpy for np.nan

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("snowflake_upload.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# List of SQL reserved keywords that need special handling (copied from export_to_csv.py)
SQL_RESERVED_KEYWORDS = [
    'ORDER', 'GROUP', 'TABLE', 'INDEX', 'SELECT', 'FROM', 'WHERE', 'JOIN',
    'HAVING', 'WITH', 'OR', 'AND', 'NOT', 'NULL', 'TRUE', 'FALSE', 'DEFAULT',
    'CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE', 'CASE', 'WHEN',
    'THEN', 'ELSE', 'END', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'NATURAL'
]

# Function to check if an identifier needs quoting in Snowflake (copied from export_to_csv.py)
def needs_quoting(identifier):
    """Check if an identifier contains spaces, special chars, is a keyword, or isn't uppercase standard."""
    # Already quoted?
    if identifier.startswith('"') and identifier.endswith('"'):
        return False 
    # Check for spaces or hyphens
    if ' ' in identifier or '-' in identifier:
        return True
    # Check if it's a reserved keyword (case-insensitive check, but keyword list is upper)
    if identifier.upper() in SQL_RESERVED_KEYWORDS:
        return True
    # Check if it's not entirely standard (uppercase letters, numbers, underscores)
    # Allows mixed case standard identifiers without forcing quotes
    if not re.match(r'^[A-Z_][A-Z0-9_]*$', identifier):
       # Check if it's just lowercase/mixed case standard identifier
       if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
           return False # Needs uppercasing, but not quoting
       else:
           # Contains other special chars, needs quoting
           return True 
    return False

def check_dependencies():
    """Check that all required dependencies are available."""
    try:
        # Verify pandas is installed and importable
        pd_version = pd.__version__
        logger.info(f"Using pandas version {pd_version}")
        
        # Verify specific pandas dependencies that might be needed for write_pandas
        required_packages = ['pyarrow', 'numpy']
        for package in required_packages:
            try:
                module = importlib.import_module(package)
                logger.info(f"Using {package} version {module.__version__}")
            except (ImportError, AttributeError):
                logger.warning(f"Optional dependency {package} not found. Installing it may improve performance.")
        
        # Check snowflake connector
        sf_version = snowflake.connector.__version__
        logger.info(f"Using snowflake-connector-python version {sf_version}")
        
        return True
    except Exception as e:
        logger.error(f"Dependency check failed: {str(e)}")
        return False

def table_exists_and_has_data(conn, table_name):
    """Check if a table exists and already has data in it."""
    cursor = conn.cursor()
    try:
        # First, check if the table exists by querying it
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        
        # If we got a result, the table exists and has count rows
        return True, count
    except Exception as e:
        # If the table doesn't exist or there's any other error
        cursor.close()
        if "does not exist" in str(e).lower() or "invalid object" in str(e).lower():
            return False, 0
        else:
            # For any other error, assume the table exists but there's a problem with the query
            logger.warning(f"Error checking if table {table_name} has data: {e}")
            return True, 0

def fix_reserved_keyword_table_name(table_name):
    """Add double quotes to table names that contain reserved keywords."""
    # List of common reserved words in Snowflake
    reserved_keywords = ["ORDER", "TABLE", "GROUP", "SELECT", "FROM", "WHERE", 
                         "GRANT", "REFERENCES", "TRANSACTION", "PRIMARY", 
                         "FOREIGN", "NATURAL", "SESSION", "USING"]
    
    # Check if any part of the table name is a reserved keyword
    parts = table_name.split('_')
    for part in parts:
        if part.upper() in reserved_keywords:
            # Quote the entire table name
            return f'"{table_name}"'
    
    # Return as is if no reserved keywords found
    return table_name

def create_table_with_retry(cursor, sql_content, table_full_name, max_retries=3):
    """Attempt to create a table with retry logic, handling common errors."""
    retries = 0
    while retries < max_retries:
        try:
            cursor.execute(sql_content)
            logger.info(f"Created table: {table_full_name}")
            return True
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle specific errors
            if "already exists" in error_str:
                logger.info(f"Table {table_full_name} already exists")
                return True
            elif "syntax error" in error_str:
                # Try to fix common SQL syntax issues
                if retries == 0:
                    # Try adding quotes around the table name
                    # Check if the table name already has quotes within it
                    if '"' in table_full_name:
                        # For already quoted tables (from reserved keywords), handle differently
                        parts = table_full_name.split('_TABLE_')
                        if len(parts) == 2:
                            db_part = parts[0]
                            table_part = parts[1]
                            if table_part.startswith('"') and table_part.endswith('"'):
                                # Already has quotes, so wrap the entire name
                                sql_content = sql_content.replace(f"TABLE {table_full_name}", f'TABLE "{db_part}_TABLE_{table_part[1:-1]}"')
                            else:
                                sql_content = sql_content.replace(f"TABLE {table_full_name}", f'TABLE "{table_full_name}"')
                    else:
                        # Normal case - just quote the whole name
                        sql_content = sql_content.replace(f"TABLE {table_full_name}", f'TABLE "{table_full_name}"')
                    logger.info(f"Retrying with quoted table name: {table_full_name}")
                elif retries == 1:
                    # Try replacing problematic data types
                    sql_content = sql_content.replace("VARCHAR()", "VARCHAR(16777216)")
                    logger.info(f"Retrying with fixed VARCHAR data types")
                    
                    # Also check for unquoted column names and quote them
                    column_pattern = re.compile(r'\s+([a-zA-Z0-9_\s]+)\s+(VARCHAR|INTEGER|FLOAT|BINARY|BOOLEAN|NUMBER|TIMESTAMP|DATE|CHAR)')
                    
                    def quote_column_name(match):
                        col_name = match.group(1).strip()
                        col_type = match.group(2)
                        # If the column name isn't already quoted and contains a space or is a keyword
                        if not (col_name.startswith('"') and col_name.endswith('"')):
                            return f'    "{col_name}" {col_type}'
                        return match.group(0)
                    
                    # Replace unquoted column names with quoted ones
                    sql_content = column_pattern.sub(quote_column_name, sql_content)
                    logger.info("Retrying with quoted column names")
                else:
                    logger.error(f"Failed to create table {table_full_name} after retries: {e}")
                    return False
            else:
                logger.error(f"Failed to create table {table_full_name}: {e}")
                if retries < max_retries - 1:
                    logger.info(f"Retrying... ({retries + 1}/{max_retries})")
                else:
                    return False
            
            retries += 1
            time.sleep(1)  # Brief pause before retry
    
    return False

def parse_sql_columns(sql_file_path):
    """Parse a CREATE TABLE SQL file to get column names and Snowflake types."""
    columns = {}
    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        # Regex to find column definitions (handles quoted and unquoted names)
        # Looks for lines like: `    COLUMN_NAME TYPE,` or `    "COLUMN NAME" TYPE,`
        pattern = re.compile(r'^\s*("?([\w\s\-\/]+)"?)\s+([A-Z_]+(?:\(\d+\))?)[,\s]*$', re.MULTILINE | re.IGNORECASE)
        matches = pattern.findall(sql_content)
        
        for match in matches:
            full_name, name_part, sf_type = match
            # Store the identifier exactly as it appears in SQL (quoted or not)
            # and the general Snowflake type category
            col_name_in_sql = full_name.strip()
            type_category = 'OTHER'
            if 'NUMBER' in sf_type.upper() or 'INT' in sf_type.upper():
                type_category = 'NUMBER'
            elif 'FLOAT' in sf_type.upper() or 'REAL' in sf_type.upper() or 'DOUBLE' in sf_type.upper():
                type_category = 'FLOAT'
            elif 'TIMESTAMP' in sf_type.upper() or 'DATE' in sf_type.upper() or 'TIME' in sf_type.upper():
                type_category = 'TIMESTAMP'
                
            columns[col_name_in_sql] = type_category
            
    except Exception as e:
        logger.error(f"Error parsing SQL file {sql_file_path}: {e}")
    return columns

def main():
    """Create tables and upload data from CSV files to Snowflake."""
    try:
        # Check dependencies first
        if not check_dependencies():
            logger.error("Required dependencies are missing. Please install them and try again.")
            logger.info("Try: pip install pandas pyarrow snowflake-connector-python")
            sys.exit(1)
            
        # Load environment variables
        load_dotenv()
        
        # Get Snowflake connection parameters from environment
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE')
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        
        logger.info(f"Connecting to Snowflake using external browser authentication")
        logger.info(f"Database: {database}, Schema: {schema}")
        
        # Connect to Snowflake with external browser authentication
        conn = snowflake.connector.connect(
            user=user,
            account=account,
            authenticator='externalbrowser',
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        logger.info("Connected to Snowflake successfully")
        
        # Check if directories exist
        csv_dir = "output_csv"
        sql_dir = "output_sql"
        
        if not os.path.exists(csv_dir) or not os.path.exists(sql_dir):
            logger.error(f"Required directories not found. Please ensure both exist: {csv_dir}, {sql_dir}")
            sys.exit(1)
        
        # Get list of database directories from the CSV folder
        databases = [d for d in os.listdir(csv_dir) 
                    if os.path.isdir(os.path.join(csv_dir, d)) 
                    and not d.startswith('.')]
        
        if not databases:
            logger.error(f"No database directories found in {csv_dir}")
            sys.exit(1)
        
        logger.info(f"Found {len(databases)} databases to process")
        
        # Index SQL files by database name 
        sql_files = [f for f in os.listdir(sql_dir) if f.endswith('.sql')]
        logger.info(f"Found {len(sql_files)} SQL files in {sql_dir}")
        
        sql_files_by_db = {}
        sql_column_types = {} # Store column types per table: { (db_name, table_name): {col: type} }
        for sql_file in sql_files:
            matched = False
            for db_name in databases:
                # Match filename format: db_name_table_name.sql
                if sql_file.startswith(f"{db_name}_"):
                    if db_name not in sql_files_by_db:
                        sql_files_by_db[db_name] = []
                    sql_files_by_db[db_name].append(sql_file)
                    
                    # Parse columns and types from SQL file
                    table_name = sql_file[len(db_name)+1:].split('.')[0]
                    sql_path = os.path.join(sql_dir, sql_file)
                    sql_column_types[(db_name, table_name)] = parse_sql_columns(sql_path)
                    
                    matched = True
                    break
            if not matched:
                logger.warning(f"Could not match SQL file {sql_file} to any database")

        # Get existing tables from the schema to avoid duplicates
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        existing_tables = [row[1] for row in cursor.fetchall()]
        logger.info(f"Found {len(existing_tables)} existing tables in schema")
        cursor.close()
        
        # Track overall statistics
        total_tables_created = 0
        total_tables_loaded = 0
        total_tables_skipped = 0
        failed_tables = []
        
        # Track tables that need to be retried
        tables_to_retry = []
        
        # Process each database directory
        for db_name in tqdm(databases, desc="Processing databases"):
            logger.info(f"Processing database: {db_name}")
            
            db_sql_files = sql_files_by_db.get(db_name, [])
            if not db_sql_files:
                logger.warning(f"No SQL files found for {db_name}, skipping table creation")
            else:
                # Create tables for this database
                cursor = conn.cursor()
                tables_created = 0
                for sql_file in db_sql_files:
                    table_name = sql_file[len(db_name)+1:].split('.')[0]
                    sql_path = os.path.join(sql_dir, sql_file)

                    # Determine the full table name based on SQL file content
                    # Read the first line to get the table name potentially with quotes
                    target_table_full_name = None
                    try:
                        with open(sql_path, 'r') as f:
                            first_line = f.readline()
                            match = re.search(r'CREATE OR REPLACE TABLE\s+([^\(]+)\(?', first_line, re.IGNORECASE)
                            if match:
                                target_table_full_name = match.group(1).strip()
                    except Exception as e_read:
                        logger.error(f"Could not read table name from {sql_path}: {e_read}")

                    if not target_table_full_name:
                        # Fallback if reading fails (should not happen often)
                        logger.warning(f"Could not determine table name from {sql_path}, constructing default name.")
                        table_name_upper = table_name.upper()
                        snowflake_table_name = f'"{table_name_upper}"' if needs_quoting(table_name_upper) else table_name_upper
                        target_table_full_name = f"{db_name.upper()}_TABLE_{snowflake_table_name}"
                    
                    # Skip if table already exists
                    # Need to check against potentially quoted and unquoted names in existing_tables
                    normalized_target_name = target_table_full_name.strip('"')
                    if target_table_full_name in existing_tables or normalized_target_name in existing_tables:
                        logger.info(f"Table {target_table_full_name} already exists, skipping creation")
                        continue
                    
                    try:
                        # Read SQL content
                        with open(sql_path, 'r') as f:
                            sql_content = f.read()
                        
                        # Special handling for FINANCIAL_TABLE_ORDER - create directly
                        if target_table_full_name == 'FINANCIAL_TABLE_"ORDER"':
                            logger.info(f"Attempting to create special table {target_table_full_name} directly.")
                            # Execute exactly the SQL read from the file
                            cursor.execute(sql_content)
                            tables_created += 1
                            existing_tables.append(target_table_full_name) # Add the quoted name
                            logger.info(f"Created table: {target_table_full_name}")
                        # Use retry logic for other tables
                        elif create_table_with_retry(cursor, sql_content, target_table_full_name):
                            tables_created += 1
                            existing_tables.append(target_table_full_name)
                        else:
                            failed_tables.append(f"{target_table_full_name} (creation)")
                    except Exception as e:
                        logger.error(f"Failed to create table {target_table_full_name}: {str(e)}")
                        failed_tables.append(f"{target_table_full_name} (creation)")
                cursor.close()
                logger.info(f"Database {db_name}: Created {tables_created} tables")

            # Upload data for this database
            db_csv_dir = os.path.join(csv_dir, db_name)
            csv_files = [f for f in os.listdir(db_csv_dir) if f.endswith('.csv')] if os.path.exists(db_csv_dir) else []
            
            tables_loaded = 0
            tables_skipped = 0
            if not csv_files:
                logger.warning(f"No CSV files found for {db_name}, skipping data upload")
            else:
                for csv_file in csv_files:
                    table_name = os.path.splitext(csv_file)[0] # Original table name from CSV filename
                    
                    # --- Determine target table name (handle reserved keywords like ORDER) ---
                    table_name_upper = table_name.upper()
                    snowflake_table_name_part = f'"{table_name_upper}"' if needs_quoting(table_name_upper) else table_name_upper
                    # Special case for ORDER table
                    if table_name.lower() == 'order' and db_name.lower() == 'financial':
                        snowflake_table_name_part = '"ORDER"' 
                    table_full_name = f"{db_name.upper()}_TABLE_{snowflake_table_name_part}"
                    # --- End target table name determination ---

                    csv_path = os.path.join(db_csv_dir, csv_file)
                    
                    try:
                        # Check if table exists and has data
                        table_exists, row_count = table_exists_and_has_data(conn, table_full_name)
                        
                        if table_exists and row_count > 0:
                            logger.info(f"Table {table_full_name} already has {row_count} rows, skipping data upload")
                            tables_skipped += 1
                            continue
                        elif not table_exists:
                            logger.warning(f"Table {table_full_name} does not exist, skipping data upload. Check SQL creation.")
                            failed_tables.append(f"{table_full_name} (upload - table missing)")
                            continue

                        # Read CSV as string
                        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
                        # ... (error handling for read_csv remains the same) ...
                        
                        # --- Data Preprocessing --- 
                        df.columns = [col.upper() for col in df.columns] # Uppercase column names
                        
                        # Get column types from parsed SQL 
                        col_types_for_table = sql_column_types.get((db_name, table_name), {})
                        if not col_types_for_table:
                            logger.warning(f"Could not find SQL column types for {db_name}.{table_name}, skipping preprocessing.")
                        else:
                            for col_name_sql, type_category in col_types_for_table.items():
                                # Column name in DataFrame is always uppercase
                                col_name_df = col_name_sql.strip('"').upper()
                                
                                if col_name_df in df.columns:
                                    if type_category in ['NUMBER', 'FLOAT', 'TIMESTAMP']:
                                        # Replace empty strings with None (becomes NULL in Snowflake)
                                        # Use np.nan for intermediate representation, then replace with None
                                        df[col_name_df] = df[col_name_df].replace('', np.nan).where(pd.notna(df[col_name_df]), None)
                        # --- End Data Preprocessing --- 

                        if len(df) == 0:
                            logger.warning(f"CSV file {csv_file} is empty after preprocessing, skipping")
                            continue
                        
                        logger.info(f"Preparing to upload {len(df)} rows to {table_full_name}")
                        
                        # --- Handle special case for FINANCIAL_TABLE_ORDER column names ---
                        final_df_columns = df.columns.tolist()
                        if table_full_name == 'FINANCIAL_TABLE_"ORDER"':
                            # Ensure columns match the manual definition (lowercase for write_pandas check)
                            expected_cols_order = ['order_id', 'account_id', 'bank_to', 'account_to', 'amount', 'k_symbol']
                            df.columns = [col.lower() for col in df.columns] # Convert DF columns to lower for this table
                            final_df_columns = df.columns.tolist()
                            logger.info(f"Adjusted columns for {table_full_name}: {final_df_columns}")
                        # --- End special case --- 

                        # Upload data
                        # ... (write_pandas call and error handling remain similar) ...
                        # Make sure to pass the potentially modified df
                        success, nchunks, nrows, _ = write_pandas(
                            conn=conn,
                            df=df, # Use the preprocessed df
                            table_name=table_full_name,
                            auto_create_table=False,  
                            overwrite=True, 
                            quote_identifiers=False # Let Snowflake handle default casing
                        )
                        # ... (rest of try/except for write_pandas) ...

                        if success:
                            logger.info(f"Loaded {nrows} rows into {table_full_name}")
                            tables_loaded += 1
                        else:
                            logger.error(f"Failed to load data into {table_full_name}")
                            failed_tables.append(f"{table_full_name} (data upload)")
                    except Exception as e:
                        logger.error(f"Error loading data into {table_full_name}: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                        failed_tables.append(f"{table_full_name} (data upload)")
            
            logger.info(f"Database {db_name}: Created {tables_created} tables, Loaded data into {tables_loaded} tables, Skipped {tables_skipped} tables with existing data")
            total_tables_created += tables_created
            total_tables_loaded += tables_loaded
            total_tables_skipped += tables_skipped
            
            # Commit changes for this database
            conn.commit()
        
        logger.info(f"Complete! Created {total_tables_created} tables, Loaded data into {total_tables_loaded} tables, Skipped {total_tables_skipped} tables with existing data")
        
        if failed_tables:
            logger.warning(f"Failed operations ({len(failed_tables)}): {', '.join(failed_tables)}")
        
        # Close connection
        conn.close()
        logger.info("Snowflake connection closed")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 