#!/bin/bash

# Get the directory of this script (i.e., the data folder)
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define download URL and file names using current NYC TLC data source
url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
tmpdir=$(mktemp -d)
full_file="$tmpdir/yellow_tripdata_2024-01.parquet"
sample_file="$script_dir/nyc_taxi_sample.csv"

echo "Downloading NYC taxi data from current source..."

# Download the full dataset with error checking
if ! curl -L -o "$full_file" "$url"; then
    echo "Error: Failed to download data from $url"
    echo "Please check your internet connection and try again."
    rm -rf "$tmpdir"
    exit 1
fi

# Check if file was actually downloaded
if [ ! -f "$full_file" ] || [ ! -s "$full_file" ]; then
    echo "Error: Downloaded file is empty or doesn't exist"
    rm -rf "$tmpdir"
    exit 1
fi

echo "Download successful. Converting parquet to CSV and sampling..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 is required to process parquet files"
    echo "Please install Python3 and the required packages: pip install pandas pyarrow"
    rm -rf "$tmpdir"
    exit 1
fi

# Create a temporary Python script to process the parquet file
cat > "$tmpdir/process_parquet.py" << 'EOF'
import pandas as pd
import sys
import os

def process_data(input_file, output_file):
    try:
        # Read the parquet file
        df = pd.read_parquet(input_file)
        
        # Take first 10,000 rows (equivalent to your original sampling)
        sample_df = df.head(10000)
        
        # Save to CSV
        sample_df.to_csv(output_file, index=False)
        
        print(f"Successfully created sample with {len(sample_df)} rows")
        print(f"Columns: {', '.join(sample_df.columns)}")
        
    except ImportError:
        print("Error: Required Python packages not installed")
        print("Please run: pip install pandas pyarrow")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python process_parquet.py <input_parquet> <output_csv>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_data(input_file, output_file)
EOF

# Process the parquet file
if ! python3 "$tmpdir/process_parquet.py" "$full_file" "$sample_file"; then
    echo "Error: Failed to process parquet file"
    echo "Make sure you have pandas and pyarrow installed: pip install pandas pyarrow"
    rm -rf "$tmpdir"
    exit 1
fi

# Clean up temp files
rm -rf "$tmpdir"

echo "Sample saved to $sample_file"

# Show some info about the created file
if [ -f "$sample_file" ]; then
    echo "File size: $(du -h "$sample_file" | cut -f1)"
    echo "Number of lines: $(wc -l < "$sample_file")"
fi

exit 0