#!/usr/bin/env python3
"""
Verify Snowflake Tables

This script connects to Snowflake and verifies the uploaded tables.
It shows the number of tables uploaded and allows you to query the tables.
"""

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create a connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            authenticator=os.getenv('SNOWFLAKE_AUTH_TYPE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        logger.info("Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

def get_table_count(conn):
    """Get the number of tables in the schema."""
    cursor = conn.cursor()
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    
    query = f"""
    SELECT COUNT(*) 
    FROM {database}.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '{schema}'
    """
    
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    
    return count

def get_table_list(conn):
    """Get a list of all tables in the schema."""
    cursor = conn.cursor()
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    
    query = f"""
    SELECT TABLE_NAME, ROW_COUNT
    FROM {database}.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '{schema}'
    ORDER BY TABLE_NAME
    """
    
    cursor.execute(query)
    tables = cursor.fetchall()
    cursor.close()
    
    return tables

def get_table_sample(conn, table_name, limit=5):
    """Get a sample of rows from a table."""
    cursor = conn.cursor()
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    
    query = f"""
    SELECT * 
    FROM {database}.{schema}.{table_name}
    LIMIT {limit}
    """
    
    try:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        return columns, rows
    except Exception as e:
        logger.error(f"Error querying table {table_name}: {e}")
        cursor.close()
        return None, None

def main():
    """Main function to verify Snowflake tables."""
    # Load environment variables
    load_dotenv()
    
    try:
        # Connect to Snowflake
        conn = get_snowflake_connection()
        
        # Get table count
        table_count = get_table_count(conn)
        logger.info(f"Total tables in schema: {table_count}")
        
        # Get table list
        tables = get_table_list(conn)
        logger.info("Table list:")
        for i, (table_name, row_count) in enumerate(tables):
            logger.info(f"{i+1}. {table_name}: {row_count} rows")
        
        # Interactive mode to explore tables
        if tables:
            while True:
                try:
                    print("\nEnter a table number to view sample data (or 'q' to quit): ", end="")
                    choice = input().strip()
                    
                    if choice.lower() == 'q':
                        break
                    
                    if choice.isdigit() and 1 <= int(choice) <= len(tables):
                        table_idx = int(choice) - 1
                        table_name = tables[table_idx][0]
                        
                        logger.info(f"Fetching sample data from {table_name}...")
                        columns, rows = get_table_sample(conn, table_name)
                        
                        if columns and rows:
                            # Create a pandas DataFrame for pretty printing
                            df = pd.DataFrame(rows, columns=columns)
                            print("\nSample data:")
                            print(df.to_string(index=False))
                        else:
                            logger.warning(f"No data available for {table_name}")
                    else:
                        logger.warning("Invalid choice. Please enter a valid table number.")
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Error: {e}")
        
        # Close Snowflake connection
        conn.close()
        logger.info("Snowflake connection closed.")
        
    except Exception as e:
        logger.error(f"Error in verification process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 