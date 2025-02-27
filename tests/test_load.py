import pytest
import pandas as pd
import os
from load import load_data

@pytest.fixture
def sample_dataframes():
    """Fixture to provide sample authors and books DataFrames."""
    authors_df = pd.DataFrame({
        "author_id": ["/authors/OL1A", "/authors/OL2B"],
        "author_name": ["Author One", "Author Two"]
    })
    books_df = pd.DataFrame({
        "book_id": ["/works/OL123W", "/works/OL456W"],
        "title": ["Sample Book One", "Sample Book Two"],
        "publication_year": [2001, 2005],
        "author_id": ["/authors/OL1A", "/authors/OL2B"]
    })
    return authors_df, books_df

def test_csv_output(sample_dataframes, tmp_path):
    """Test that load_data correctly writes DataFrames to CSV files."""
    authors_df, books_df = sample_dataframes
    # Use temporary file paths.
    authors_file = tmp_path / "authors_test.csv"
    books_file = tmp_path / "books_test.csv"
    
    load_data(authors_df, books_df, str(authors_file), str(books_file))
    
    # Check that the files exist.
    assert os.path.exists(str(authors_file))
    assert os.path.exists(str(books_file))
    
    # Read the files and compare with the original DataFrames.
    loaded_authors = pd.read_csv(str(authors_file))
    loaded_books = pd.read_csv(str(books_file))
    pd.testing.assert_frame_equal(authors_df.reset_index(drop=True), loaded_authors)
    pd.testing.assert_frame_equal(books_df.reset_index(drop=True), loaded_books)

def test_load_failure(monkeypatch, sample_dataframes):
    """Test that load_data raises an exception when file writing fails."""
    authors_df, books_df = sample_dataframes
    # Simulate failure by monkeypatching DataFrame.to_csv to always raise an IOError.
    def fail_to_csv(*args, **kwargs):
        raise IOError("Simulated file write error")
    
    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_to_csv)
    with pytest.raises(IOError):
        load_data(authors_df, books_df)