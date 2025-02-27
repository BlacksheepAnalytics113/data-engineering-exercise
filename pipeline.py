#!/usr/bin/env python3
import argparse
import logging
import yaml
import json
import sys
import time  # For measuring runtime durations
from extract import get_cached_api_data  # Fetch API data with caching and retry logic
from transform import transform_data     # Process raw JSON into structured DataFrames
from load import load_data               # Output DataFrames to CSV files

class JsonFormatter(logging.Formatter):
    """Custom logging formatter to output logs in JSON format."""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage()
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging():
    """
    Set up structured logging with JSON formatting using a custom formatter.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])

def load_config(config_file='config.yaml'):
    """
    Load the pipeline configuration from a YAML file.
    """
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error("Error loading configuration from %s: %s", config_file, e)
        sys.exit(1)

def main():
    setup_logging()
    
    # Load configuration
    config = load_config()
    
    # Parse command-line arguments for subject override
    parser = argparse.ArgumentParser(description="Run the Open Library Data Pipeline")
    parser.add_argument("--subject", type=str, help="Subject to extract data for", default=None)
    args = parser.parse_args()

    # Use command-line subject if provided, otherwise use default from config
    subject = args.subject if args.subject else config['api']['subject']
    config['api']['subject'] = subject  # Update config with the selected subject
    
    # Construct the full API endpoint URL using the endpoint format template
    endpoint = config['api']['endpoint_format'].format(base_url=config['api']['base_url'], subject=subject)
    
    # Log the start of the pipeline with key details in JSON format
    logging.info(json.dumps({"event": "Pipeline started", "subject": subject, "endpoint": endpoint}))
    
    # --- Extraction Phase ---
    extraction_start = time.time()
    try:
        raw_data = get_cached_api_data(endpoint)
    except Exception as e:
        logging.error("Extraction failed: %s", e)
        sys.exit(1)
    extraction_duration = time.time() - extraction_start
    logging.info(json.dumps({"event": "Extraction completed", "duration_seconds": extraction_duration}))
    
    # --- Transformation Phase ---
    transformation_start = time.time()
    try:
        authors_df, books_df = transform_data(raw_data)
    except Exception as e:
        logging.error("Transformation failed: %s", e)
        sys.exit(1)
    transformation_duration = time.time() - transformation_start
    logging.info(json.dumps({"event": "Transformation completed", "duration_seconds": transformation_duration}))
    
    # --- Loading Phase ---
    loading_start = time.time()
    try:
        load_data(authors_df, books_df)
    except Exception as e:
        logging.error("Loading failed: %s", e)
        sys.exit(1)
    loading_duration = time.time() - loading_start
    logging.info(json.dumps({"event": "Loading completed", "duration_seconds": loading_duration}))
    
    logging.info(json.dumps({"event": "Pipeline executed successfully."}))

if __name__ == "__main__":
    main()