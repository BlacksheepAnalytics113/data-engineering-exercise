from typing import Tuple, Dict, Any, List
import pandas as pd
import logging
import time

# Schema version constant to manage future updates in data structure.
SCHEMA_VERSION: float = 1.0

def transform_data(raw_data: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes raw JSON data from the Open Library API and returns two DataFrames:
    one for authors and one for books.
    
    Parameters:
        raw_data (dict): JSON data fetched from the API.
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]:
            - authors_df: DataFrame containing unique author details.
            - books_df: DataFrame containing book details with an associated author_id.
            
    Raises:
        ValueError: If the input data is missing the required 'works' key.
    """
    start_time = time.time()  # Start measuring transformation duration

    works = raw_data.get("works")
    if works is None:
        logging.error("Input raw_data is missing the 'works' key.")
        raise ValueError("Invalid input: 'works' key not found in raw_data.")
    
    authors_list: List[Dict[str, Any]] = []
    books_list: List[Dict[str, Any]] = []
    error_records: List[Dict[str, Any]] = []  # Collect records that fail validation

    # Iterate over works; note: for large datasets, vectorized processing may be considered.
    for idx, work in enumerate(works):
        book_id = work.get("key")
        title = work.get("title")
        pub_year = work.get("first_publish_year")
        
        if not book_id or not title:
            logging.warning("Skipping work at index %d due to missing key fields: %s", idx, work)
            error_records.append({"index": idx, "reason": "missing book_id or title", "record": work})
            continue
        
        if pub_year is None:
            logging.warning("Missing publication_year for work %s at index %d. Setting default value 0.", book_id, idx)
            pub_year = 0

        work_authors = work.get("authors", [])
        if not work_authors:
            logging.warning("No authors found for work %s at index %d.", book_id, idx)
            error_records.append({"index": idx, "reason": "no authors", "record": work})
        
        for auth_idx, author in enumerate(work_authors):
            author_id = author.get("key")
            author_name = author.get("name")
            if not author_id or not author_name:
                logging.warning("Skipping author at index %d for work %s due to missing fields: %s", auth_idx, book_id, author)
                error_records.append({
                    "work_index": idx,
                    "author_index": auth_idx,
                    "reason": "missing author_id or author_name",
                    "record": author
                })
                continue

            authors_list.append({
                "author_id": author_id,
                "author_name": author_name
            })
            books_list.append({
                "book_id": book_id,
                "title": title,
                "publication_year": pub_year,
                "author_id": author_id
            })
    
    # Create DataFrames from the collected records.
    authors_df: pd.DataFrame = pd.DataFrame(authors_list).drop_duplicates(subset=["author_id"])
    books_df: pd.DataFrame = pd.DataFrame(books_list)
    
    # Ensure publication_year is numeric; non-numeric values are coerced to 0.
    books_df["publication_year"] = pd.to_numeric(books_df["publication_year"], errors="coerce").fillna(0).astype(int)
    
    # Verify expected columns exist.
    assert "author_id" in authors_df.columns, "authors_df is missing 'author_id' column."
    assert "author_name" in authors_df.columns, "authors_df is missing 'author_name' column."
    assert "book_id" in books_df.columns, "books_df is missing 'book_id' column."
    assert "title" in books_df.columns, "books_df is missing 'title' column."
    assert "publication_year" in books_df.columns, "books_df is missing 'publication_year' column."
    assert "author_id" in books_df.columns, "books_df is missing 'author_id' column."
    
    transformation_duration = time.time() - start_time
    logging.info("Transformation completed in %.2f seconds: Processed %d authors and %d books.", 
                 transformation_duration, len(authors_df), len(books_df))
    
    # Persist error records for further analysis if any exist.
    if error_records:
        error_df = pd.DataFrame(error_records)
        error_df.to_csv("error_records.csv", index=False)
        logging.warning("Persisted %d error records to 'error_records.csv'.", len(error_records))
    
    return authors_df, books_df