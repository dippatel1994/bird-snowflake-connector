#!/bin/bash
# Script to upload exported CSV files to Snowflake

echo "Starting Snowflake upload process..."

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Check if the upload_to_snowflake.py script exists and is executable
if [ ! -x "upload_to_snowflake.py" ]; then
    echo "Making upload_to_snowflake.py executable..."
    chmod +x upload_to_snowflake.py
fi

# Run the upload script
echo "Running upload script..."
./upload_to_snowflake.py

# Check the exit code
if [ $? -eq 0 ]; then
    echo "Upload completed successfully!"
else
    echo "Upload failed. Please check the logs for more information."
fi

echo "Done!" 