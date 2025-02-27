import pytest
import os
import pandas as pd
import subprocess
import sys

def test_full_pipeline(tmp_path, monkeypatch):
    """
    Test the full pipeline execution:
      - Create a temporary working directory.
      - Write a temporary config.yaml.
      - Monkeypatch sys.argv to simulate command-line subject override.
      - Run pipeline.py using subprocess.
      - Verify that output CSV files are created and contain data.
    """
    # Set up a temporary working directory.
    cwd = tmp_path / "pipeline_test"
    cwd.mkdir()
    
    # Create a temporary config.yaml file.
    config_file = cwd / "config.yaml"
    config_content = """
api:
  subject: "science_fiction"
  base_url: "https://openlibrary.org/subjects"
  endpoint_format: "{base_url}/{subject}.json"
aws:
  s3_bucket: "drivewealth-openlibrary-data"
  region: "us-east-1"
schema_version: 1.0
"""
    config_file.write_text(config_content)
    
    # Simulate command-line override for the subject using monkeypatch.
    monkeypatch.setattr(sys, 'argv', ["pipeline.py", "--subject", "mystery"])
    
    # Run the pipeline.py script in the temporary directory.
    result = subprocess.run([sys.executable, "pipeline.py"],
                            cwd=str(cwd), capture_output=True, text=True)
    
    # Check that the process exited successfully.
    assert result.returncode == 0, f"Pipeline failed with error: {result.stderr}"
    
    # Verify that output CSV files exist.
    authors_file = cwd / "authors.csv"
    books_file = cwd / "books.csv"
    assert os.path.exists(authors_file), "authors.csv was not created."
    assert os.path.exists(books_file), "books.csv was not created."
    
    # Read the CSV files and ensure they are not empty.
    authors_df = pd.read_csv(authors_file)
    books_df = pd.read_csv(books_file)
    assert not authors_df.empty, "authors.csv is empty."
    assert not books_df.empty, "books.csv is empty."