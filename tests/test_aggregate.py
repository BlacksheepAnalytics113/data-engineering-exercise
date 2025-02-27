import pytest
import pandas as pd
import os
import time
from aggregate import aggregate_books

@pytest.fixture
def sample_books_csv(tmp_path):
    """Fixture to create a sample CSV file with test book data for aggregation."""
    data = {
        "book_id": ["/works/OL1W", "/works/OL2W", "/works/OL3W", "/works/OL4W"],
        "title": ["Book One", "Book Two", "Book Three", "Book Four"],
        "publication_year": [2001, 2001, 2002, 2002],
        "author_id": ["/authors/A1", "/authors/A1", "/authors/A2", "/authors/A2"]
    }
    df = pd.DataFrame(data)
    csv_file = tmp_path / "test_books.csv"
    df.to_csv(csv_file, index=False)
    return str(csv_file)

def test_valid_aggregation(sample_books_csv):
    """Test that valid aggregation computes correct counts and averages."""
    start_time = time.time()
    aggregation, avg_aggregation = aggregate_books(sample_books_csv)
    duration = time.time() - start_time
    logging_message = f"Aggregation completed in {duration:.2f} seconds."
    print(logging_message)  # Optionally log performance.
    
    # Check expected columns.
    assert set(aggregation.columns) == {"author_id", "publication_year", "num_books"}
    assert set(avg_aggregation.columns) == {"author_id", "avg_books_per_year"}
    
    # Verify total count and average values.
    assert aggregation["num_books"].sum() == 4
    for avg in avg_aggregation["avg_books_per_year"]:
        assert avg == 2

def test_empty_aggregation(tmp_path):
    """Test that aggregation handles an empty CSV file gracefully."""
    empty_file = tmp_path / "empty_books.csv"
    pd.DataFrame(columns=["book_id", "title", "publication_year", "author_id"]).to_csv(empty_file, index=False)
    with pytest.raises(Exception):
        aggregate_books(str(empty_file))