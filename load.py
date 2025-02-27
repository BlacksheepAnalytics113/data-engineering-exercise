import pandas as pd
import logging

def load_data(authors_df: pd.DataFrame, books_df: pd.DataFrame, 
              authors_file: str = "authors.csv", books_file: str = "books.csv") -> None:
    """
    Writes the transformed authors and books DataFrames to CSV files.
    
    This function outputs two CSV files:
      - authors_file: for the authors data.
      - books_file: for the books data.
    
    In a production environment, CSV outputs would likely be replaced by a 
    persistent database solution (e.g., PostgreSQL on AWS RDS) with appropriate
    partitioning and indexing.
    
    Parameters:
        authors_df (pd.DataFrame): DataFrame containing author information.
        books_df (pd.DataFrame): DataFrame containing book information.
        authors_file (str): Output file name for authors data. Defaults to "authors.csv".
        books_file (str): Output file name for books data. Defaults to "books.csv".
    
    Returns:
        None
        
    Raises:
        Exception: Any errors encountered during file writing are logged and re-raised.
    """
    try:
        authors_df.to_csv(authors_file, index=False)
        logging.info("Successfully wrote authors data to '%s'.", authors_file)
    except Exception as e:
        logging.error("Failed to write authors data to '%s': %s", authors_file, e)
        raise

    try:
        books_df.to_csv(books_file, index=False)
        logging.info("Successfully wrote books data to '%s'.", books_file)
    except Exception as e:
        logging.error("Failed to write books data to '%s': %s", books_file, e)
        raise

    logging.info("Data successfully loaded into output files: '%s' and '%s'.", authors_file, books_file)