"""
Module for converting CRSP monthly CSV files to Parquet format.

This module provides functionality to clean and convert CRSP (Center for Research
in Security Prices) monthly data from CSV to Parquet format, applying standard
data cleaning procedures.
"""

from pathlib import Path
from typing import Optional

import pandas as pd


def normalize_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase.
    
    Args:
        dataframe: Input DataFrame with potentially mixed-case column names.
        
    Returns:
        DataFrame with lowercase column names.
    """
    dataframe = dataframe.copy()
    dataframe.columns = dataframe.columns.str.lower()
    return dataframe


def sort_by_identifier_and_date(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Sort DataFrame by permanent number (permno) and date.
    
    Args:
        dataframe: Input DataFrame with 'permno' and 'date' columns.
        
    Returns:
        Sorted DataFrame.
    """
    return dataframe.sort_values(by=['permno', 'date'])


def convert_data_types(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Convert specific columns to appropriate data types.
    
    Converts 'siccd' to string and 'date' to datetime.
    
    Args:
        dataframe: Input DataFrame with 'siccd' and 'date' columns.
        
    Returns:
        DataFrame with converted data types.
    """
    dataframe = dataframe.copy()
    dataframe['siccd'] = dataframe['siccd'].astype(str)
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    return dataframe


def clean_price_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the price (prc) column.
    
    Makes all prices positive (negative values represent average of bid and ask)
    and removes rows where price is zero.
    
    Args:
        dataframe: Input DataFrame with 'prc' column.
        
    Returns:
        DataFrame with cleaned price column and zero-price rows removed.
    """
    dataframe = dataframe.copy()
    # Make all prices positive (negative values are average of bid and ask)
    dataframe['prc'] = dataframe['prc'].abs()
    # Remove rows where price is zero
    dataframe = dataframe[dataframe['prc'] != 0]
    return dataframe


def clean_return_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the return (ret) column.
    
    Converts return values to numeric, coercing errors to NaN, and removes
    rows with NaN returns.
    
    Args:
        dataframe: Input DataFrame with 'ret' column.
        
    Returns:
        DataFrame with cleaned return column and NaN return rows removed.
    """
    dataframe = dataframe.copy()
    # Convert to numeric, coercing errors to NaN
    dataframe['ret'] = pd.to_numeric(dataframe['ret'], errors='coerce')
    # Remove rows where return is NaN
    dataframe = dataframe[dataframe['ret'].notna()]
    return dataframe


def load_and_clean_crsp_data(csv_path: str) -> pd.DataFrame:
    """
    Load CRSP monthly data from CSV and apply initial cleaning steps.
    
    Args:
        csv_path: Path to the input CSV file.
        
    Returns:
        Partially cleaned DataFrame.
        
    Raises:
        FileNotFoundError: If the CSV file does not exist.
        pd.errors.EmptyDataError: If the CSV file is empty.
    """
    # Load the CSV file
    dataframe = pd.read_csv(csv_path)
    
    # Apply initial cleaning steps
    dataframe = normalize_column_names(dataframe)
    dataframe = sort_by_identifier_and_date(dataframe)
    dataframe = convert_data_types(dataframe)
    
    return dataframe


def apply_data_cleaning(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data cleaning procedures to remove invalid rows.
    
    Args:
        dataframe: Input DataFrame with 'prc' and 'ret' columns.
        
    Returns:
        Cleaned DataFrame with invalid rows removed.
    """
    dataframe = clean_price_column(dataframe)
    dataframe = clean_return_column(dataframe)
    return dataframe


def csv_to_parquet(
    csv_path: str,
    output_path: Optional[str] = None,
    index: bool = False
) -> None:
    """
    Convert CRSP monthly CSV file to Parquet format with data cleaning.
    
    Args:
        csv_path: Path to the input CSV file.
        output_path: Path for the output Parquet file. If None, uses the same
                     name as the CSV file with .parquet extension.
        index: Whether to include the DataFrame index in the Parquet file.
               Default is False.
        
    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If the CSV file cannot be read or processed.
        
    Example:
        >>> csv_to_parquet('data/crsp_monthly.csv', 'data/crsp_monthly.parquet')
    """
    # Validate input file exists
    csv_file_path = Path(csv_path)
    if not csv_file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Determine output path if not provided
    if output_path is None:
        output_path = csv_file_path.with_suffix('.parquet')
    
    # Ensure output directory exists
    output_file_path = Path(output_path)
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load and apply initial cleaning
    dataframe = load_and_clean_crsp_data(csv_path)
    
    # Apply data cleaning procedures
    dataframe = apply_data_cleaning(dataframe)
    
    # Save to Parquet format
    dataframe.to_parquet(output_path, index=index)


def main():
    csv_to_parquet('data/crsp_monthly.csv', 'data/crsp_monthly.parquet')

if __name__ == "__main__":
    main()