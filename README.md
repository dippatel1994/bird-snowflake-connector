# BIRD SQLite to Snowflake Uploader

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Snowflake](https://img.shields.io/badge/Snowflake-0.48.0+-00A1E0?logo=snowflake)](https://www.snowflake.com/)
[![BIRD Benchmark](https://img.shields.io/badge/BIRD-Benchmark-green)](https://bird-bench.github.io/)

A tool designed to prepare and upload [BIRD benchmark](https://bird-bench.github.io/) SQLite databases to Snowflake, enabling large-scale database grounded text-to-SQL evaluation.

##  Solution Overview

Due to compatibility issues with the Snowflake Python connector on newer Python versions, this solution takes a two-step approach:

1. ** Export SQLite databases to CSV files**: The script extracts all tables from the SQLite databases and exports them to CSV files, organized by database name.
2. ** Generate CREATE TABLE statements**: For each table, the script generates a Snowflake-compatible CREATE TABLE SQL statement.
3. ** Upload to Snowflake**: You can choose between two methods for uploading to Snowflake:
   - Manual upload through the Snowflake web interface
   - Programmatic upload using the provided script

##  Prerequisites

- Python 3.8+
- Snowflake account

##  Setup

1. Create and activate a virtual environment:

```bash
python -m venv .venv
# On macOS/Linux
source .venv/bin/activate
# On Windows
.venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your Snowflake credentials (required for programmatic upload):

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

##  Usage

### Step 1: Export SQLite to CSV

First unzip `dev_databases` folder (which contains .sqlite files) into this repo and than run the export script to convert all SQLite databases to CSV files:

```bash
./export_to_csv.py
```

This will:
1. Scan all SQLite database files in the `dev_databases` directory
2. Export each table to a CSV file
3. Generate CREATE TABLE SQL statements for Snowflake
4. Save everything in the `output_csv` and `output_sql` directories

### Step 2: Upload to Snowflake

#### Option A: Programmatic Upload (Recommended)

Run the upload script to automatically upload all CSV files to Snowflake:

```bash
./run_upload.sh
```

This will:
1. Connect to your Snowflake instance using credentials from `.env` (or prompt you for them)
2. Create the specified database and schema if they don't exist
3. Execute SQL statements to create tables
4. Upload data from CSV files to the corresponding tables
5. Show progress as the data is being uploaded
6. Skip tables that already have data to prevent duplicate uploads

#### Option B: Manual Upload via Web Interface

Alternatively, you can upload the files manually:

1. Log in to your Snowflake instance
2. Create a database and schema (or use existing ones)
3. In the Snowflake UI, navigate to Data → Databases → [Your Database] → [Your Schema]
4. For each database directory:
   a. Create tables using the SQL files in the `output_sql` directory
   b. Click 'Load Data' and select the corresponding CSV file
   c. Follow the wizard to complete the upload

##  Table Naming Convention

Tables are named as `database_name_table_tablename` to avoid conflicts.
For example, the 'students' table in the 'school' database would be named 'SCHOOL_TABLE_STUDENTS' in Snowflake.

##  Features and Fixes

### Handling Reserved Keywords

The tool automatically handles SQL reserved keywords in both table and column names:
- Tables with reserved keywords (e.g., "order") are properly quoted in SQL statements
- Special handling is provided for known problematic tables (e.g., the "order" table in the financial database)

### Column Names with Spaces

All column names are automatically quoted in the generated SQL, handling:
- Column names with spaces
- Column names with special characters
- Column names that are SQL reserved keywords

### Duplicate Upload Prevention

The upload script checks if tables already have data before uploading to prevent duplicates.

## 🔍 Troubleshooting

- For large databases, the export or upload may take some time; check the console for progress updates
- If you encounter encoding issues in the CSV files, you may need to adjust the export settings in the code
- For export issues, check the `export.log` file for details
- For upload issues, check the `snowflake_upload.log` file for details
- If you encounter Python version compatibility issues with the Snowflake connector, try using Python 3.8-3.11

##  Output Directories

- `output_csv/`: Contains all exported CSV files, organized by database name
- `output_sql/`: Contains all CREATE TABLE SQL statements

##  About BIRD Benchmark

This tool is specifically designed for working with the [BIRD benchmark](https://bird-bench.github.io/) - a Big Bench for Large-Scale Database Grounded Text-to-SQLs evaluation. BIRD contains over 12,751 unique question-SQL pairs, 95 big databases with a total size of 33.4 GB, covering more than 37 professional domains including blockchain, hockey, healthcare, education, and more.

### Citation

If you use this tool or BIRD benchmark in your research, please cite:

```bibtex
@article{li2024can,
  title={Can llm already serve as a database interface? a big bench for large-scale database grounded text-to-sqls},
  author={Li, Jinyang and Hui, Binyuan and Qu, Ge and Yang, Jiaxi and Li, Binhua and Li, Bowen and Wang, Bailin and Qin, Bowen and Geng, Ruiying and Huo, Nan and others},
  journal={Advances in Neural Information Processing Systems},
  volume={36},
  year={2024}
}
```

##  License

This project is licensed under the MIT License.