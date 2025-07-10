import os
import argparse
import logging
from datetime import datetime

from ETL.Extract import fetch_earthquake_all_day
from ETL.Transfrom import clean_earthquake_data, enrich_earthquake_data
from ETL.Load import upload_to_bigquery

from prefect_gcp import GcpCredentials
from prefect import flow
from dotenv import load_dotenv
load_dotenv()


@flow(name="ETL Pipeline", log_prints=True)
def run_pipeline(mode:str="replace") -> None:
    """
    Run the ETL pipeline with configurable mode.
    
    Args:
        mode (str,optional): "replace, "fall", or "append". Default to "replace".
        
    """

    logging.info(f"Starting ETL pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # === 1. Extract ===
    logging.info("Extracting data ...")
    raw = fetch_earthquake_all_day()
    if raw.empty:
        logging.warning("No data fetched. Exiting pipeline.")
        return 
    
    # === 2. Transform ===
    logging.info("Transforming data ...")
    logging.info(f"--Data Cleanup--")
    clean = clean_earthquake_data(raw)
    logging.info(f"--Data Enrichment--")
    enriched = enrich_earthquake_data(clean)
    
    # === 3. Load ===
    project_id = "earthquake-jcds-0612-465304"
    table_id = "earthquake-jcds-0612-465304.earthquakes.events" #dataset.id
    

    logging.info("Loading data ...")
    upload_to_bigquery(enriched, table_id, project_id, mode)

    
    logging.info(f"ETL pipeline completed - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 


if __name__ == "__main__":
    run_pipeline()