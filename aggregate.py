from typing import Tuple
import pandas as pd
import logging
import time

def aggregate_books(books_csv: str = "books.csv") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aggregates book data from a CSV file and returns two DataFrames:
      1. Aggregation DataFrame: Contains the count of books grouped by author_id and publication_year.
      2. Average Aggregation DataFrame: Contains the average number of books per author per year.
    
    Parameters:
        books_csv (str): Path to the CSV file containing books data. Defaults to "books.csv".
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]:
            - aggregation: DataFrame with columns [author_id, publication_year, num_books].
            - avg_aggregation: DataFrame with columns [author_id, avg_books_per_year].
    
    Raises:
        FileNotFoundError: If the specified CSV file cannot be found.
        Exception: For any other errors encountered during the aggregation process.
    """
    start_time = time.time()  # Start performance timer

    try:
        books_df = pd.read_csv(books_csv)
        logging.info("Successfully read books data from '%s'.", books_csv)
    except FileNotFoundError as fnf_err:
        logging.error("The file '%s' was not found: %s", books_csv, fnf_err)
        raise
    except Exception as e:
        logging.error("Error reading '%s': %s", books_csv, e)
        raise

    # Check if the DataFrame is empty and log a warning
    if books_df.empty:
        logging.warning("The books DataFrame is empty. No data available for aggregation.")

    # Assert that the expected columns exist
    expected_columns = {"book_id", "author_id", "publication_year"}
    missing_columns = expected_columns - set(books_df.columns)
    if missing_columns:
        error_message = f"Missing expected columns in books_df: {missing_columns}"
        logging.error(error_message)
        raise ValueError(error_message)

    try:
        # Group books by author_id and publication_year to count the number of books.
        aggregation = books_df.groupby(["author_id", "publication_year"]).agg(
            num_books=("book_id", "count")
        ).reset_index()

        # Group by author_id to compute the average number of books per year.
        avg_aggregation = aggregation.groupby("author_id").agg(
            avg_books_per_year=("num_books", "mean")
        ).reset_index()

        duration = time.time() - start_time  # End performance timer
        logging.info("Aggregation completed in %.2f seconds with %d rows.", duration, len(aggregation))
        
        # Note: For very large datasets, consider using distributed processing (e.g., Dask) for scalability.
        return aggregation, avg_aggregation
    except Exception as e:
        logging.error("Error during aggregation: %s", e)
        raise

if __name__ == "__main__":
    # Standalone execution: run aggregation and print results.
    try:
        agg, avg_agg = aggregate_books()
        print("Aggregation (Books per Year by Author):")
        print(agg)
        print("\nAverage Books per Year by Author:")
        print(avg_agg)
    except Exception as e:
        logging.error("Aggregation failed when running as standalone: %s", e)