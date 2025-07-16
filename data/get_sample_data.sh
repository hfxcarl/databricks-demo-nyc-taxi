#!/bin/bash

# Get the directory of this script (i.e., the data folder)
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define download URL and file names
url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv"
tmpdir=$(mktemp -d)
full_file="$tmpdir/yellow_tripdata_2019-01.csv"
sample_file="$script_dir/nyc_taxi_sample.csv"

# Download the full dataset
echo "Downloading full dataset..."
curl -L -o "$full_file" "$url"

# Extract first 10,000 rows + header
echo "Extracting sample..."
(head -n 1 "$full_file" && tail -n +2 "$full_file" | head -n 10000) > "$sample_file"

# Clean up temp files
rm -rf "$tmpdir"

echo "Sample saved to $sample_file"
