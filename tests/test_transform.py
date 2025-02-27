import pytest
import pandas as pd
import os
from transform import transform_data, SCHEMA_VERSION

# Sample valid JSON input with one work having one author.
VALID_JSON = {
    "works": [
        {
            "key": "/works/OL123W",
            "title": "Sample Book",
            "first_publish_year": 2001,
            "authors": [{"key": "/authors/OL1A", "name": "Author One"}]
        }
    ]
}

# Sample JSON with missing critical fields.
INVALID_JSON = {
    "works": [
        {"key": None, "title": None, "first_publish_year": None, "authors": []}
    ]
}

def test_valid_transformation():
    """Test that valid JSON input produces correct authors and books DataFrames."""
    authors_df, books_df = transform_data(VALID_JSON)
    # Verify that authors_df contains the expected columns and one record.
    assert "author_id" in authors_df.columns
    assert "author_name" in authors_df.columns
    assert len(authors_df) == 1

    # Verify that books_df contains the expected columns and one record.
    assert "book_id" in books_df.columns
    assert "title" in books_df.columns
    assert "publication_year" in books_df.columns
    assert "author_id" in books_df.columns
    assert len(books_df) == 1
    assert pd.api.types.is_integer_dtype(books_df["publication_year"])

def test_missing_fields_transformation():
    """
    Test that transformation handles input with missing critical fields.
    Expected behavior: records with missing book_id/title are skipped, and missing publication_year is defaulted.
    """
    authors_df, books_df = transform_data(INVALID_JSON)
    # Expect no valid records due to missing key fields.
    assert authors_df.empty and books_df.empty

def test_error_record_persistence(tmp_path):
    """
    Test that error records are persisted to 'error_records.csv' when invalid records are encountered.
    """
    # Run transform_data on invalid input.
    try:
        transform_data(INVALID_JSON)
    except Exception:
        pass  # Exception is expected due to missing data.
    error_file = "error_records.csv"
    # Check if the file exists.
    assert os.path.exists(error_file)
    # Optionally, verify the contents.
    error_df = pd.read_csv(error_file)
    assert not error_df.empty
    # Clean up after test.
    os.remove(error_file)