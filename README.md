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
├── notebooks/
│ └── nyc_taxi_etl_demo.py
├── data/
│ └── nyc_taxi_sample.csv
├── README.md
├── requirements.txt
└── LICENSE


## How to Run

1. Open the notebook in Databricks
2. Attach it to a running cluster
3. Upload `nyc_taxi_sample.csv` to DBFS
4. Run all cells

## Dataset Reference

NYC TLC Trip Record Data:  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## License

MIT License
