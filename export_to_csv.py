#!/usr/bin/env python3
"""
SQLite to CSV Exporter

This script exports all SQLite databases from the dev_databases folder to CSV files.
These CSV files can then be uploaded to Snowflake using the upload_to_snowflake.py script.
"""

import os
import glob
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
from tqdm import tqdm
import logging
import sys
import time
import csv
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("export.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# List of SQL reserved keywords that need special handling
SQL_RESERVED_KEYWORDS = [
    'ORDER', 'GROUP', 'TABLE', 'INDEX', 'SELECT', 'FROM', 'WHERE', 'JOIN',
    'HAVING', 'WITH', 'OR', 'AND', 'NOT', 'NULL', 'TRUE', 'FALSE', 'DEFAULT',
    'CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE', 'CASE', 'WHEN',
    'THEN', 'ELSE', 'END', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'NATURAL'
]

# Function to check if an identifier needs quoting in Snowflake
def needs_quoting(identifier):
    """Check if an identifier contains non-standard chars or is a reserved keyword."""
    # Already quoted?
    if identifier.startswith('"') and identifier.endswith('"'):
        return False 
    # Check for anything other than uppercase letters, numbers, underscores
    if not re.match(r'^[A-Z_][A-Z0-9_]*$', identifier):
        return True # Contains lowercase, spaces, hyphens, or other symbols
    # Check if it's a reserved keyword
    if identifier in SQL_RESERVED_KEYWORDS: # Keywords list is uppercase
        return True
    return False

def get_sqlite_files(base_dir="dev_databases"):
    """Find all SQLite files in the dev_databases directory."""
    db_files = []
    
    # Check if the directory exists
    if not os.path.exists(base_dir):
        logger.error(f"Directory {base_dir} not found")
        return db_files
    
    # Walk through all subdirectories and find .sqlite files
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".sqlite"):
                db_files.append(os.path.join(root, file))
    
    return db_files

def get_sqlite_tables(sqlite_file):
    """Get all tables from a SQLite database."""
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    tables = [table[0] for table in tables]
    
    return tables, conn

def create_output_dirs():
    """Create the output directory structure."""
    os.makedirs("output_csv", exist_ok=True)
    os.makedirs("output_sql", exist_ok=True)
    
    return "output_csv", "output_sql"

def export_table_to_csv(conn, table_name, db_name, csv_dir):
    """Export a single table to a CSV file."""
    try:
        # Create a directory for the database
        db_dir = os.path.join(csv_dir, db_name)
        os.makedirs(db_dir, exist_ok=True)
        
        # Create filename for the CSV file
        csv_file = os.path.join(db_dir, f"{table_name}.csv")
        
        # Check if table name is a reserved keyword
        if table_name.upper() in SQL_RESERVED_KEYWORDS:
            # Quote the table name for the query
            query = f'SELECT * FROM "{table_name}"'
            logger.info(f"Using quoted name for reserved keyword table: {table_name}")
        else:
            query = f"SELECT * FROM {table_name}"
        
        # Extract the table data from SQLite
        df = pd.read_sql_query(query, conn)
        
        # Export to CSV
        df.to_csv(csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        logger.info(f"Exported {table_name} to {csv_file} with {len(df)} rows")
        return True
    except Exception as e:
        logger.error(f"Error exporting table {table_name}: {e}")
        return False

def generate_create_table_sql(conn, table_name, db_name, output_path):
    """Generate CREATE TABLE SQL for Snowflake from a SQLite table schema."""
    try:
        cursor = conn.cursor()
        
        snowflake_db_name = db_name.upper()
        original_table_name = table_name # Keep original for lookups

        # Determine Snowflake table name (uppercase, quoted only if needed)
        table_name_upper = original_table_name.upper()
        if needs_quoting(table_name_upper): # Check the uppercased version
            snowflake_table_name = f'"{table_name_upper}"'
        else:
            snowflake_table_name = table_name_upper
        
        is_financial_order = original_table_name.lower() == 'order' and db_name.lower() == 'financial'

        # Special case handling for the 'order' table in the financial database
        if is_financial_order:
            # Manually define the schema - use standard uppercase names here
            columns_info = [
                ('ORDER_ID', 'NUMBER'), 
                ('ACCOUNT_ID', 'NUMBER'), 
                ('BANK_TO', 'VARCHAR'), 
                ('ACCOUNT_TO', 'VARCHAR'), 
                ('AMOUNT', 'FLOAT'), 
                ('K_SYMBOL', 'VARCHAR')
            ]
            logger.info(f"Using manual schema definition for: {original_table_name} in db {db_name}")
            snowflake_table_name = '"ORDER"' # Explicitly quote the reserved keyword
        else:
            # Get the table schema using the original table name (quoting for PRAGMA if needed)
            pragma_table_name = f'"{original_table_name}"' if needs_quoting(original_table_name) else original_table_name
            try:
                cursor.execute(f'PRAGMA table_info({pragma_table_name})')
                columns_pragma = cursor.fetchall()
            except sqlite3.OperationalError as e:
                 logger.error(f"PRAGMA failed for {db_name}.{original_table_name}: {e}")
                 return False
                 
            if not columns_pragma:
                logger.warning(f"No columns found via PRAGMA for {db_name}.{original_table_name}")
                return False
            
            # Convert PRAGMA result to standard format
            columns_info = []
            for col in columns_pragma:
                _cid, col_name, col_type, _notnull, _dflt_value, _pk = col
                # Force uppercase for Snowflake standard identifiers
                col_name_upper = col_name.upper()
                # Quote if it contains non-standard chars or is a reserved keyword
                snowflake_col_name = f'"{col_name_upper}"' if needs_quoting(col_name_upper) else col_name_upper

                # Map SQLite types to Snowflake types
                col_type_upper = col_type.upper() if col_type else ''
                if 'INT' in col_type_upper:
                    sf_type = 'NUMBER'
                elif col_type_upper in ['REAL', 'DOUBLE', 'FLOAT', 'NUMERIC', 'DECIMAL']:
                    sf_type = 'FLOAT'
                elif col_type_upper in ['CHAR', 'VARCHAR', 'TEXT', 'NVARCHAR', 'CLOB']:
                    sf_type = 'VARCHAR'
                elif 'DATE' in col_type_upper or 'TIME' in col_type_upper:
                    sf_type = 'TIMESTAMP_NTZ'
                elif 'BOOL' in col_type_upper:
                    sf_type = 'BOOLEAN'
                elif 'BLOB' in col_type_upper or 'BINARY' in col_type_upper:
                    sf_type = 'BINARY'
                else:
                    sf_type = 'VARCHAR'  # Default
                    logger.warning(f"Unknown type '{col_type}' for {col_name} in {original_table_name}. Defaulting to VARCHAR.")
                
                columns_info.append((snowflake_col_name, sf_type))

        # Construct the full table name for Snowflake
        snowflake_full_table_name = f"{snowflake_db_name}_TABLE_{snowflake_table_name}"
        
        # Start creating the SQL
        create_table = f"CREATE OR REPLACE TABLE {snowflake_full_table_name} (\n"
        column_defs = [f"    {col_name} {col_type}" for col_name, col_type in columns_info]
        create_table += ",\n".join(column_defs)
        create_table += "\n);"
        
        # Write the SQL to a file 
        output_file = os.path.join(output_path, f"{db_name}_{original_table_name}.sql")
        with open(output_file, 'w') as f:
            f.write(create_table)
            
        logger.info(f"Generated SQL file for {db_name}.{original_table_name} -> {snowflake_full_table_name}")
        return True
    except Exception as e:
        import traceback
        logger.error(f"Error generating SQL for {db_name}.{original_table_name}: {e}\n{traceback.format_exc()}")
        return False

def process_sqlite_file(sqlite_file, csv_dir, sql_dir):
    """Process a single SQLite file and export all its tables."""
    try:
        # Extract database name from file path - use the directory name
        db_name = os.path.basename(os.path.dirname(sqlite_file))
        
        # If file is directly in the dev_databases folder, use the filename without extension
        if db_name == "dev_databases":
            db_name = os.path.splitext(os.path.basename(sqlite_file))[0]
        
        logger.info(f"Processing database: {db_name} from file: {sqlite_file}")
        
        # Get all tables from the SQLite database
        tables, conn = get_sqlite_tables(sqlite_file)
        
        # Export each table
        csv_success_count = 0
        sql_success_count = 0
        failed_tables = []
        
        for table_name in tables:
            if export_table_to_csv(conn, table_name, db_name, csv_dir):
                csv_success_count += 1
            else:
                failed_tables.append(f"{table_name} (csv)")
            
            if generate_create_table_sql(conn, table_name, db_name, sql_dir):
                sql_success_count += 1
            else:
                failed_tables.append(f"{table_name} (sql)")
                logger.error(f"Failed to generate SQL for table {table_name}")
        
        conn.close()
        
        logger.info(f"Completed {db_name}: {csv_success_count}/{len(tables)} tables exported to CSV")
        logger.info(f"Completed {db_name}: {sql_success_count}/{len(tables)} CREATE TABLE statements generated")
        
        if failed_tables:
            logger.warning(f"Failed tables for {db_name}: {', '.join(failed_tables)}")
        
        return True
    except Exception as e:
        logger.error(f"Error processing SQLite file {sqlite_file}: {e}")
        return False

def main():
    """Main function to export all SQLite databases to CSV files."""
    # Load environment variables
    load_dotenv()
    
    try:
        # Get all SQLite files from dev_databases folder
        sqlite_files = get_sqlite_files()
        logger.info(f"Found {len(sqlite_files)} SQLite database files")
        
        if not sqlite_files:
            logger.error("No SQLite files found in dev_databases folder")
            sys.exit(1)
        
        # Create output directories
        csv_dir, sql_dir = create_output_dirs()
        
        # Process each SQLite file
        success_count = 0
        total_tables = 0
        exported_tables = 0
        
        for i, sqlite_file in enumerate(tqdm(sqlite_files, desc="Processing SQLite files")):
            logger.info(f"Processing file {i+1}/{len(sqlite_files)}: {sqlite_file}")
            
            # Get table count before processing
            tables, conn = get_sqlite_tables(sqlite_file)
            total_tables += len(tables)
            conn.close()
            
            if process_sqlite_file(sqlite_file, csv_dir, sql_dir):
                success_count += 1
                
                # Count successfully exported tables
                db_name = os.path.basename(os.path.dirname(sqlite_file))
                if db_name == "dev_databases":
                    db_name = os.path.splitext(os.path.basename(sqlite_file))[0]
                    
                db_dir = os.path.join(csv_dir, db_name)
                if os.path.exists(db_dir):
                    exported_tables += len([f for f in os.listdir(db_dir) if f.endswith('.csv')])
        
        logger.info(f"Completed exporting {success_count}/{len(sqlite_files)} databases to CSV")
        logger.info(f"Exported {exported_tables}/{total_tables} tables in total")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 