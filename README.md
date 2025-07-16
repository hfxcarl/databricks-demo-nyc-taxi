# NYC Taxi ETL Demo on Databricks

This project demonstrates a simple ETL pipeline using Azure Databricks and PySpark. It reads NYC taxi data, performs basic cleaning and transformations, writes data to Delta Lake format, and queries for insights.

## Steps

1. Load CSV data
2. Clean and transform using PySpark
3. Write and read Delta Lake
4. Visualize average trip distances

## Tools Used

- Databricks (Community Edition or Azure)
- PySpark
- Delta Lake

## Project Structure
databricks-demo-nyc-taxi/
│
├── notebooks/
│   └── nyc_taxi_etl_demo.py      # Main Databricks notebook in Python
│
├── data/
│   └── nyc_taxi_sample.csv       # Small sample dataset (or link to it)
│
├── README.md                     # Description and instructions
├── requirements.txt              # Optional, for local Spark emulation
└── LICENSE                       # MIT or other open-source license


## How to Run

Open the notebook in Databricks, attach to a cluster, and run all cells.

