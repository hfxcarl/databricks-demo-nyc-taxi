#!/bin/bash

# Get the directory of this script (i.e., the data folder)
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define download URL and file names
# Using a working alternative source for NYC taxi data
url="https://raw.githubusercontent.com/plotly/datasets/master/uber-rides-data1.csv"
tmpdir=$(mktemp -d)
full_file="$tmpdir/taxi_data.csv"
sample_file="$script_dir/nyc_taxi_sample.csv"

echo "Downloading sample NYC taxi data..."

# Download the dataset with error checking
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

echo "Download successful. Creating sample..."

# Extract first 10,000 rows + header (your original logic)
(head -n 1 "$full_file" && tail -n +2 "$full_file" | head -n 10000) > "$sample_file"

# Verify the sample was created
if [ ! -f "$sample_file" ] || [ ! -s "$sample_file" ]; then
    echo "Error: Failed to create sample file"
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
    echo "First few lines:"
    head -3 "$sample_file"
fi

exit 0
