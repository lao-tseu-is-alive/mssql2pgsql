#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Environment Setup ---
# Check if the .env file exists
if [ -f .env ]; then
  # Export the variables from .env file for the script's environment
  # This command reads the .env file, ignores lines starting with #,
  # and exports the rest as environment variables.
  export "$(grep -v '^#' .env | xargs)"
  echo "✅ Loaded environment variables from .env file."
else
  echo "⚠️ Warning: .env file not found. Assuming environment variables are already set."
fi

# --- Application Execution ---
# Execute the main Python script, passing all arguments from this script
# The "$@" allows you to pass table names or other parameters dynamically.
# e.g., ./run_local.sh Employe
echo "🚀 Starting the mssql2pgsql script..."
uv run copy_Mssql_Table_to_Postgresql.py "$@"