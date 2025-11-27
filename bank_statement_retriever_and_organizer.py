# Converted from general_deprec_code.ipynb

import sys
import io

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

## RRANDOM FUCNTIONS
def clean_column_names(df):
    df.columns = (
        df.columns.str.strip()         # Remove leading/trailing spaces
        .str.lower()                   # Convert to lowercase
        .str.replace(r'\W+', '_', regex=True)  # Replace non-word characters with '_'
        .str.replace(r'_+', '_', regex=True)   # Remove multiple consecutive '_'
        .str.rstrip('_')                # Remove trailing '_'
    )
    return df



# Standard library imports
import os
import re
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Third-party imports
import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

# Telegram (optional - only if sending messages)
from telegram import Bot
from telegram.constants import ParseMode

from pathlib import Path
import pickle

import os
import pandas as pd
import numpy as np
from typing import Dict, Optional, List

def _drop_fully_blank_and_unnamed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns that are either all blank/NaN or whose names are Unnamed: ... (case-insensitive).
    """
    def is_unnamed(col):
        col_str = str(col).strip().lower()
        return col_str.startswith("unnamed:") or col_str == ''
    # First drop full blank columns
    df = df.dropna(axis=1, how='all')
    # Then drop columns that are unnamed
    cols_to_drop = [col for col in df.columns if is_unnamed(col)]
    df = df.drop(columns=cols_to_drop)
    return df

def _drop_fully_blank_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows that are entirely blank/NaN.
    """
    return df.dropna(axis=0, how='all')

def _remove_beginning_balance_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that contain 'Beginning balance' in any column (case-insensitive).
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with 'Beginning balance' rows removed
    """
    df_cleaned = df.copy()
    
    # Check each row to see if any column contains 'Beginning balance'
    mask = df_cleaned.astype(str).apply(
        lambda row: any('beginning balance' in str(val).lower() for val in row), 
        axis=1
    )
    
    # Remove rows where mask is True
    rows_removed = mask.sum()
    if rows_removed > 0:
        df_cleaned = df_cleaned[~mask]
        print(f"  Removed {rows_removed} row(s) containing 'Beginning balance'")
    
    return df_cleaned

def _clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean numeric columns by removing $, %, and commas, then convert to numeric.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned numeric columns
    """
    df_cleaned = df.copy()
    
    for col in df_cleaned.columns:
        # Check if column is string/object type (might contain numeric data with formatting)
        if df_cleaned[col].dtype == 'object' or df_cleaned[col].dtype == 'string':
            # Try to detect if it looks numeric (contains $, %, or numbers with commas)
            sample_values = df_cleaned[col].dropna().astype(str).head(10)
            looks_numeric = any(
                '$' in str(val) or '%' in str(val) or ',' in str(val) 
                for val in sample_values
            )
            
            if looks_numeric:
                # Remove $, %, and commas, then convert to numeric
                df_cleaned[col] = (
                    df_cleaned[col]
                    .astype(str)
                    .str.replace('$', '', regex=False)
                    .str.replace('%', '', regex=False)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                )
                # Convert to numeric, coercing errors to NaN
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
    
    return df_cleaned

def _clean_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns with 'Date' in the name to datetime format.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with date columns converted to datetime
    """
    df_cleaned = df.copy()
    
    for col in df_cleaned.columns:
        # Check if column name contains 'date' (case-insensitive)
        if 'date' in str(col).lower():
            try:
                # Convert to datetime, coercing errors to NaT
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
            except Exception as e:
                # If conversion fails, leave as is and print warning
                print(f"Warning: Could not convert column '{col}' to datetime: {e}")
                continue
    
    return df_cleaned

def bank_statements_retriever(directory: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Scan a directory for Excel (.xlsx, .xls) and CSV (.csv) files.
    Excludes files containing 'GWID' or 'Rubric' in the filename (case-insensitive).
    Returns a dict mapping filename to DataFrame.
    
    During import, automatically cleans:
    - Numeric columns: removes $, %, and commas, then converts to numeric
    - Date columns: converts columns with 'Date' in name to datetime format
    - Removes rows containing 'Beginning balance' in any column

    Args:
        directory: Path to the folder to scan. Defaults to current working directory.

    Returns:
        Dict[str, pd.DataFrame]: Keys are filenames -> DataFrame values
    """
    directory = directory or os.getcwd()
    all_files: List[str] = os.listdir(directory)

    # Identify candidate files (exclude GWID and Rubric files)
    candidates: List[str] = []
    for file_name in all_files:
        lower_name = file_name.lower()
        # Check if file is Excel or CSV
        is_excel_or_csv = (
            lower_name.endswith(".csv")
            or lower_name.endswith(".xlsx")
            or lower_name.endswith(".xls")
        )
        # Exclude files with 'gwid' or 'rubric' in the name
        has_gwid = "gwid" in lower_name
        has_rubric = "rubric" in lower_name
        
        if is_excel_or_csv and not has_gwid and not has_rubric:
            candidates.append(file_name)

    # Load dataframes with filename as key
    dataframes: Dict[str, pd.DataFrame] = {}
    for file_name in candidates:
        file_path = os.path.join(directory, file_name)
        ext = os.path.splitext(file_name)[1].lower()

        try:
            if ext == ".csv":
                df = pd.read_csv(file_path)
            elif ext in (".xlsx", ".xls"):
                df = pd.read_excel(file_path)
            else:
                continue  # Should not happen due to filter, but guard anyway

            # Clean numeric columns (remove $, %, commas and convert to numeric)
            df = _clean_numeric_columns(df)
            
            # Clean date columns (convert columns with 'Date' in name to datetime)
            df = _clean_date_columns(df)
            
            # Remove rows containing 'Beginning balance'
            df = _remove_beginning_balance_rows(df)
            
            # Clean fully blank columns and "Unnamed" columns
            df = _drop_fully_blank_and_unnamed(df)
            # Drop fully blank rows
            df = _drop_fully_blank_rows(df)
            # Remove 'balance' column if it exists (Chase files sometimes have this)
            cols_lower = [col.lower().strip() for col in df.columns]
            if 'balance' in cols_lower:
                balance_idx = cols_lower.index('balance')
                df = df.drop(columns=[df.columns[balance_idx]])
                print(f"  Removed 'balance' column from {file_name}")

            # If after dropping, the DataFrame is empty (no rows), skip adding
            if df.shape[0] == 0:
                continue

            # Use filename as key (not modified)
            dataframes[file_name] = df
            
        except Exception as exc:
            print(f"Warning: Failed to read file '{file_name}': {exc}")
            continue

    return dataframes

try:
    x = bank_statements_retriever()
    for name, df in x.items():
        print(df.columns)
except Exception as e:
    print("There was an error with importing bank statement, pls verify the statements are correct (BofA/Chase), if not contact the technical team")
    # Optionally print exception for debugging:
    #print(e)

# Define standard Chase columns as a constant (can be used throughout the codebase)
STANDARD_CHASE_COLUMNS = ['details', 'posting date', 'description', 'amount', 'type']

def get_standard_chase_columns() -> List[str]:
    """
    Returns the standard Chase column format that all data should match.
    
    Returns:
        List[str]: Standard Chase columns in lowercase: ['details', 'posting date', 'description', 'amount', 'type']
    """
    return STANDARD_CHASE_COLUMNS.copy()

def bank_selector(dataframes_dict: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    """
    Bank Selector Function
    
    This function analyzes a dictionary of DataFrames (where keys are filenames and values are the DataFrames)
    to identify which bank each file belongs to based on column structure.
    
    For each DataFrame:
    - Cleans column names (lowercase, strip whitespace)
    - Checks if columns match Chase bank statement format: ['details', 'posting date', 'description', 'amount', 'type']
    - Checks if columns match BofA bank statement format: ['date', 'description', 'amount']
    - Returns a dictionary mapping filename to bank name
    - For "Unknown" files, prints what columns are expected vs what was found
    
    Args:
        dataframes_dict: Dictionary where keys are filenames and values are pandas DataFrames
    
    Returns:
        Dict[str, str]: Dictionary mapping filename -> bank name (e.g., "Chase", "BofA", or "Unknown")
    """
    bank_mapping: Dict[str, str] = {}
    
    # Use the standard Chase columns constant
    chase_columns = STANDARD_CHASE_COLUMNS
    
    # Define the expected BofA columns (cleaned and lowercased from: 'Date', 'Description', 'Amount')
    bofa_columns = ['date', 'description', 'amount']
    
    print("\n=== Bank Detection Analysis ===\n")
    print(f"Standard Chase columns expected: {chase_columns}\n")
    print(f"Standard BofA columns expected: {bofa_columns}\n")
    
    for filename, df in dataframes_dict.items():
        # Clean column names: lowercase and strip whitespace
        cleaned_columns = [col.strip().lower() for col in df.columns]
        
        # Check if columns match Chase format
        if sorted(cleaned_columns) == sorted(chase_columns):
            bank_mapping[filename] = "Chase"
            print(f"✓ {filename}: Matches Chase format")
        # Check if columns match BofA format
        elif sorted(cleaned_columns) == sorted(bofa_columns):
            bank_mapping[filename] = "BofA"
            print(f"✓ {filename}: Matches BofA format")
        else:
            bank_mapping[filename] = "Unknown"
            print(f"⚠ {filename}: Unknown format")
            print(f"  Found columns: {cleaned_columns}")
            print(f"  Expected Chase columns: {chase_columns}")
            print(f"  Expected BofA columns: {bofa_columns}")
            print(f"  → To standardize, this file needs columns: {chase_columns}\n")
    
    print("\n=== Bank Detection Complete ===\n")
    
    return bank_mapping

try:
    dataframe_files = bank_selector(x)
except Exception as e:
    print("There was an error structing  the bank statements, pls verify BofA/Chase statements were uplaoded, if not contact the techinla team")

def filter_and_split_banks(bank_mapping: Dict[str, str], dataframes_dict: Dict[str, pd.DataFrame]) -> Dict[str, any]:
    """
    Filter and Split Banks Function
    
    This function takes a dictionary mapping filenames to bank names and the original DataFrames dictionary, and:
    - Filters to only keep "Chase" and "Bank of America" (or "BofA") entries
    - Splits the results into two separate dictionaries: one for Chase, one for BofA
    - Concatenates all Chase DataFrames into a single DataFrame (chase_central)
    - Concatenates all BofA DataFrames into a single DataFrame (bofa_central)
    - Validates with robust checks:
        * Row counts match
        * Column names match
        * Date columns: min and max values match
        * Amount columns: totals and standard deviations match
    
    Args:
        bank_mapping: Dictionary where keys are filenames and values are bank names
        dataframes_dict: Dictionary where keys are filenames and values are pandas DataFrames
    
    Returns:
        Dict[str, any]: Dictionary with four keys:
            - "Chase": Dictionary of Chase filenames -> bank name
            - "BofA": Dictionary of Bank of America filenames -> bank name
            - "chase_central": Concatenated DataFrame of all Chase accounts
            - "bofa_central": Concatenated DataFrame of all BofA accounts
    
    Raises:
        ValueError: If any validation checks fail, specifying which checks failed
    """
    chase_files = {}
    bofa_files = {}
    chase_dataframes = []
    bofa_dataframes = []
    
    # Track row counts for validation
    chase_individual_rows = 0
    bofa_individual_rows = 0
    
    for filename, bank_name in bank_mapping.items():
        # Normalize bank name for comparison (case-insensitive)
        bank_lower = bank_name.lower().strip()
        
        if bank_lower == "chase":
            chase_files[filename] = bank_name
            # Add the DataFrame to the list for concatenation
            if filename in dataframes_dict:
                df = dataframes_dict[filename]
                chase_dataframes.append(df)
                chase_individual_rows += len(df)
        elif bank_lower in ["bofa", "bank of america", "boa"]:
            bofa_files[filename] = bank_name
            # Add the DataFrame to the list for concatenation
            if filename in dataframes_dict:
                df = dataframes_dict[filename]
                bofa_dataframes.append(df)
                bofa_individual_rows += len(df)
    
    # Concatenate all Chase DataFrames
    chase_central = pd.concat(chase_dataframes, ignore_index=True) if chase_dataframes else pd.DataFrame()
    
    # Concatenate all BofA DataFrames
    bofa_central = pd.concat(bofa_dataframes, ignore_index=True) if bofa_dataframes else pd.DataFrame()
    
    # Robust validation function
    def validate_concatenation(individual_dfs: List[pd.DataFrame], central_df: pd.DataFrame, bank_name: str) -> List[str]:
        """
        Validate concatenation with multiple checks.
        Returns list of failed check messages.
        """
        failed_checks = []
        passed_checks = []
        
        if not individual_dfs or central_df.empty:
            print(f"⚠ {bank_name}: No data to validate")
            return failed_checks  # Skip validation if empty
        
        print(f"\n=== Validating {bank_name} ===")
        
        # Check 1: Row counts
        individual_rows = sum(len(df) for df in individual_dfs)
        central_rows = len(central_df)
        if individual_rows != central_rows:
            failed_checks.append(f"Row count mismatch: Individual total = {individual_rows}, Central = {central_rows}")
        else:
            passed_checks.append(f"✓ Row count: {individual_rows} rows match")
            print(f"✓ Row count: {individual_rows} rows match")
        
        # Check 2: Column names
        # Get all unique column names from individual DataFrames
        individual_columns = set()
        for df in individual_dfs:
            individual_columns.update(df.columns)
        central_columns = set(central_df.columns)
        
        if individual_columns != central_columns:
            missing_in_central = individual_columns - central_columns
            extra_in_central = central_columns - individual_columns
            if missing_in_central:
                failed_checks.append(f"Column names mismatch: Missing in central = {missing_in_central}")
            if extra_in_central:
                failed_checks.append(f"Column names mismatch: Extra in central = {extra_in_central}")
        else:
            passed_checks.append(f"✓ Column names: {len(individual_columns)} columns match")
            print(f"✓ Column names: {len(individual_columns)} columns match")
        
        # Check 3: Date columns - min and max
        date_columns = [col for col in central_df.columns if 'date' in str(col).lower()]
        if date_columns:
            for date_col in date_columns:
                # Get min and max from individual DataFrames
                individual_mins = []
                individual_maxs = []
                for df in individual_dfs:
                    if date_col in df.columns:
                        col_data = pd.to_datetime(df[date_col], errors='coerce').dropna()
                        if not col_data.empty:
                            individual_mins.append(col_data.min())
                            individual_maxs.append(col_data.max())
                
                if individual_mins and individual_maxs:
                    individual_min = min(individual_mins)
                    individual_max = max(individual_maxs)
                    central_min = pd.to_datetime(central_df[date_col], errors='coerce').dropna().min()
                    central_max = pd.to_datetime(central_df[date_col], errors='coerce').dropna().max()
                    
                    if pd.isna(central_min) or pd.isna(central_max):
                        failed_checks.append(f"Date column '{date_col}': Central has no valid dates")
                    else:
                        min_match = individual_min == central_min
                        max_match = individual_max == central_max
                        
                        if min_match and max_match:
                            passed_checks.append(f"✓ Date column '{date_col}': Min ({individual_min}) and Max ({individual_max}) match")
                            print(f"✓ Date column '{date_col}': Min ({individual_min}) and Max ({individual_max}) match")
                        else:
                            if not min_match:
                                failed_checks.append(f"Date column '{date_col}': Min mismatch - Individual = {individual_min}, Central = {central_min}")
                            if not max_match:
                                failed_checks.append(f"Date column '{date_col}': Max mismatch - Individual = {individual_max}, Central = {central_max}")
        else:
            print(f"ℹ No date columns found to validate")
        
        # Check 4: Amount columns - totals and standard deviations
        amount_columns = [col for col in central_df.columns if 'amount' in str(col).lower()]
        if amount_columns:
            for amount_col in amount_columns:
                # Get totals and std dev from individual DataFrames
                individual_totals = []
                individual_stds = []
                individual_values = []
                
                for df in individual_dfs:
                    if amount_col in df.columns:
                        col_data = pd.to_numeric(df[amount_col], errors='coerce').dropna()
                        if not col_data.empty:
                            individual_totals.append(col_data.sum())
                            individual_stds.append(col_data.std())
                            individual_values.extend(col_data.tolist())
                
                if individual_totals:
                    individual_total = sum(individual_totals)
                    # Calculate overall std from all individual values combined
                    individual_std = pd.Series(individual_values).std() if individual_values else 0
                    
                    central_values = pd.to_numeric(central_df[amount_col], errors='coerce').dropna()
                    if central_values.empty:
                        failed_checks.append(f"Amount column '{amount_col}': Central has no valid numeric values")
                    else:
                        central_total = central_values.sum()
                        central_std = central_values.std()
                        
                        # Use small tolerance for floating point comparison
                        tolerance = 0.01
                        total_match = abs(individual_total - central_total) <= tolerance
                        std_match = abs(individual_std - central_std) <= tolerance
                        
                        if total_match and std_match:
                            passed_checks.append(f"✓ Amount column '{amount_col}': Total ({individual_total:.2f}) and Std Dev ({individual_std:.2f}) match")
                            print(f"✓ Amount column '{amount_col}': Total ({individual_total:.2f}) and Std Dev ({individual_std:.2f}) match")
                        else:
                            if not total_match:
                                failed_checks.append(f"Amount column '{amount_col}': Total mismatch - Individual = {individual_total:.2f}, Central = {central_total:.2f}")
                            if not std_match:
                                failed_checks.append(f"Amount column '{amount_col}': Std deviation mismatch - Individual = {individual_std:.2f}, Central = {central_std:.2f}")
        else:
            print(f"ℹ No amount columns found to validate")
        
        return failed_checks
    
    # Validate Chase
    chase_failed = validate_concatenation(chase_dataframes, chase_central, "Chase")
    if chase_failed:
        error_msg = "Mismatch between Chase individual files and chase_central:\n" + "\n".join(f"  - {check}" for check in chase_failed)
        raise ValueError(error_msg)
    else:
        print(f"✓ All Chase validation checks passed!\n")
    
    # Validate BofA
    bofa_failed = validate_concatenation(bofa_dataframes, bofa_central, "BofA")
    if bofa_failed:
        error_msg = "Mismatch between BofA individual files and bofa_central:\n" + "\n".join(f"  - {check}" for check in bofa_failed)
        raise ValueError(error_msg)
    else:
        print(f"✓ All BofA validation checks passed!\n")
    
    return {
        "Chase": chase_files,
        "BofA": bofa_files,
        "chase_central": chase_central,
        "bofa_central": bofa_central
    }

try:
    cc = filter_and_split_banks(dataframe_files, x)
except Exception as e:
    print(f"Error aggregating bank statements, pls verify the statements are correct (BofA/Chase), if not contact the technical team")
    print(f"Detailed error: {e}")
    import sys
    sys.exit(1)

def bofa_standardizer(bofa_central: Optional[pd.DataFrame] = None, chase_central: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    BofA Standardizer Function
    
    This function transforms the bofa_central DataFrame to match the format and structure of chase_central.
    Column mappings:
    - Details: Based on Amount column (negative = DEBIT, positive = CREDIT)
    - Posting Date: From Date column in bofa
    - Description: Same as Description column
    - Amount: Same as Amount column
    - Type: From Details column with "ACH_" prefix (e.g., ACH_DEBIT, ACH_CREDIT)
    
    The output DataFrame will have the exact same column names and order as chase_central.
    Includes robust validation checks to verify all transformations.
    
    Args:
        bofa_central: BofA central DataFrame (defaults to global bofa_central if not provided)
        chase_central: Chase central DataFrame (defaults to global chase_central if not provided)
    
    Returns:
        pd.DataFrame: Standardized BofA DataFrame matching Chase format with same column names
    
    Raises:
        ValueError: If validation checks fail, listing which checks failed
    """
    # Access from global scope if not provided
    if bofa_central is None:
        try:
            bofa_central = globals().get('cc', {}).get('bofa_central')
            if bofa_central is None:
                raise ValueError("bofa_central not found. Please provide it as parameter or ensure 'cc' dictionary exists.")
        except:
            raise ValueError("bofa_central not found. Please provide it as parameter.")
    
    if chase_central is None:
        try:
            chase_central = globals().get('cc', {}).get('chase_central')
            if chase_central is None:
                raise ValueError("chase_central not found. Please provide it as parameter or ensure 'cc' dictionary exists.")
        except:
            raise ValueError("chase_central not found. Please provide it as parameter.")
    
    # If bofa_central is empty, return an empty standardized DataFrame with correct columns and structure
    if bofa_central is not None and bofa_central.empty:
        print("ℹ Warning: bofa_central is empty, continuing. Returning an empty standardized DataFrame.")
        # Use chase_central's columns and dtypes for structure
        standardized_df = pd.DataFrame(columns=chase_central.columns)
        # It's important to set dtypes to those of chase_central:
        for col in chase_central.columns:
            standardized_df[col] = standardized_df[col].astype(chase_central[col].dtype)
        return standardized_df
    
    if chase_central.empty:
        raise ValueError("chase_central is empty, cannot use as template")
    
    # Get column names from chase_central (use exact names and order)
    chase_columns = list(chase_central.columns)
    
    # Find column mappings in chase_central (case-insensitive matching)
    details_col = None
    posting_date_col = None
    description_col = None
    amount_col_chase = None
    type_col = None
    
    for col in chase_columns:
        col_lower = col.lower().strip()
        if col_lower == 'details':
            details_col = col
        elif 'posting' in col_lower and 'date' in col_lower:
            posting_date_col = col
        elif col_lower == 'description':
            description_col = col
        elif col_lower == 'amount':
            amount_col_chase = col
        elif col_lower == 'type':
            type_col = col
    
    # Validate we found all required Chase columns
    missing_chase_cols = []
    if not details_col:
        missing_chase_cols.append("Details")
    if not posting_date_col:
        missing_chase_cols.append("Posting Date")
    if not description_col:
        missing_chase_cols.append("Description")
    if not amount_col_chase:
        missing_chase_cols.append("Amount")
    if not type_col:
        missing_chase_cols.append("Type")
    
    if missing_chase_cols:
        raise ValueError(f"Missing required columns in chase_central: {missing_chase_cols}")
    
    # Get bofa column names (case-insensitive)
    bofa_columns_lower = [col.lower().strip() for col in bofa_central.columns]
    bofa_date_col = None
    bofa_description_col = None
    bofa_amount_col = None
    
    for col in bofa_central.columns:
        col_lower = col.lower().strip()
        if col_lower == 'date':
            bofa_date_col = col
        elif col_lower == 'description':
            bofa_description_col = col
        elif col_lower == 'amount':
            bofa_amount_col = col
    
    # Validate required columns exist in bofa
    missing_bofa_cols = []
    if not bofa_date_col:
        missing_bofa_cols.append("Date")
    if not bofa_description_col:
        missing_bofa_cols.append("Description")
    if not bofa_amount_col:
        missing_bofa_cols.append("Amount")
    
    if missing_bofa_cols:
        raise ValueError(f"Missing required columns in bofa_central: {missing_bofa_cols}")
    
    # Create new DataFrame with exact same structure as chase_central
    standardized_df = pd.DataFrame(index=bofa_central.index)
    
    # Map columns in the same order as chase_central
    for col in chase_columns:
        if col == details_col:
            # Details column: Based on Amount (negative = DEBIT, positive = CREDIT)
            standardized_df[col] = bofa_central[bofa_amount_col].apply(
                lambda x: "DEBIT" if pd.notna(x) and float(x) < 0 else "CREDIT" if pd.notna(x) else None
            )
        elif col == posting_date_col:
            # Posting Date column: From Date column
            standardized_df[col] = bofa_central[bofa_date_col]
        elif col == description_col:
            # Description column: Same as Description
            standardized_df[col] = bofa_central[bofa_description_col]
        elif col == amount_col_chase:
            # Amount column: Same as Amount
            standardized_df[col] = bofa_central[bofa_amount_col]
        elif col == type_col:
            # Type column: Based on Details with "ACH_" prefix
            details_values = bofa_central[bofa_amount_col].apply(
                lambda x: "DEBIT" if pd.notna(x) and float(x) < 0 else "CREDIT" if pd.notna(x) else None
            )
            standardized_df[col] = details_values.apply(
                lambda x: f"ACH_{x}" if pd.notna(x) else None
            )
        else:
            # For any other columns in chase_central, fill with NaN
            standardized_df[col] = None
    
    # Ensure column order matches chase_central exactly
    standardized_df = standardized_df[chase_columns]
    
    # Reset index to match chase format
    standardized_df = standardized_df.reset_index(drop=True)
    
    # Robust validation checks
    print("\n=== Validating BofA Standardization ===")
    failed_checks = []
    
    # Check 1: Row count matches
    if len(standardized_df) != len(bofa_central):
        failed_checks.append(f"Row count mismatch: Original = {len(bofa_central)}, Standardized = {len(standardized_df)}")
    else:
        print(f"✓ Row count: {len(standardized_df)} rows match")
    
    # Check 2: Column names and order match chase_central exactly
    if list(standardized_df.columns) != chase_columns:
        failed_checks.append(f"Column names/order mismatch: Expected {chase_columns}, Got {list(standardized_df.columns)}")
    else:
        print(f"✓ Column structure: Matches chase_central exactly ({len(chase_columns)} columns)")
    
    # Check 3: Details column values are DEBIT or CREDIT
    details_values = standardized_df[details_col].dropna().unique()
    valid_details = set(['DEBIT', 'CREDIT'])
    if not set(details_values).issubset(valid_details):
        invalid = set(details_values) - valid_details
        failed_checks.append(f"Details column has invalid values: {invalid}")
    else:
        print(f"✓ Details column: All values are DEBIT or CREDIT")
    
    # Check 4: Details matches Amount sign (negative = DEBIT, positive = CREDIT)
    amount_details_match = True
    for idx in standardized_df.index:
        amount_val = standardized_df.loc[idx, amount_col_chase]
        details_val = standardized_df.loc[idx, details_col]
        if pd.notna(amount_val) and pd.notna(details_val):
            expected_detail = "DEBIT" if amount_val < 0 else "CREDIT"
            if details_val != expected_detail:
                amount_details_match = False
                break
    
    if not amount_details_match:
        failed_checks.append("Details column does not match Amount sign (negative should be DEBIT, positive should be CREDIT)")
    else:
        print(f"✓ Details-Amount consistency: Details correctly match Amount sign")
    
    # Check 5: Posting Date matches original Date
    date_match = standardized_df[posting_date_col].equals(bofa_central[bofa_date_col].reset_index(drop=True))
    if not date_match:
        # Check if they're equal after conversion
        date_match = (
            pd.to_datetime(standardized_df[posting_date_col], errors='coerce')
            .reset_index(drop=True)
            .equals(pd.to_datetime(bofa_central[bofa_date_col], errors='coerce').reset_index(drop=True))
        )
    if not date_match:
        failed_checks.append("Posting Date does not match original Date column")
    else:
        print(f"✓ Posting Date: Matches original Date column")
    
    # Check 6: Description matches original Description
    desc_match = standardized_df[description_col].equals(bofa_central[bofa_description_col].reset_index(drop=True))
    if not desc_match:
        failed_checks.append("Description does not match original Description column")
    else:
        print(f"✓ Description: Matches original Description column")
    
    # Check 7: Amount matches original Amount
    amount_match = standardized_df[amount_col_chase].equals(bofa_central[bofa_amount_col].reset_index(drop=True))
    if not amount_match:
        # Check with tolerance for floating point
        amount_diff = abs(standardized_df[amount_col_chase].reset_index(drop=True) - bofa_central[bofa_amount_col].reset_index(drop=True)).max()
        if amount_diff > 0.01:
            failed_checks.append(f"Amount does not match original Amount column (max diff: {amount_diff})")
        else:
            print(f"✓ Amount: Matches original Amount column (within tolerance)")
    else:
        print(f"✓ Amount: Matches original Amount column")
    
    # Check 8: Type column has ACH_ prefix
    type_values = standardized_df[type_col].dropna().unique()
    all_have_prefix = all(str(val).startswith('ACH_') for val in type_values if pd.notna(val))
    if not all_have_prefix:
        invalid_types = [val for val in type_values if not str(val).startswith('ACH_')]
        failed_checks.append(f"Type column has values without ACH_ prefix: {invalid_types}")
    else:
        print(f"✓ Type column: All values have ACH_ prefix")
    
    # Check 9: Type matches Details with ACH_ prefix
    type_details_match = True
    for idx in standardized_df.index:
        details_val = standardized_df.loc[idx, details_col]
        type_val = standardized_df.loc[idx, type_col]
        if pd.notna(details_val) and pd.notna(type_val):
            expected_type = f"ACH_{details_val}"
            if type_val != expected_type:
                type_details_match = False
                break
    
    if not type_details_match:
        failed_checks.append("Type column does not match Details with ACH_ prefix")
    else:
        print(f"✓ Type-Details consistency: Type correctly matches Details with ACH_ prefix")
    
    # Raise error if any checks failed
    if failed_checks:
        error_msg = "BofA standardization validation failed:\n" + "\n".join(f"  - {check}" for check in failed_checks)
        raise ValueError(error_msg)
    else:
        print(f"✓ All BofA standardization checks passed!\n")
    
    return standardized_df

try:
    bofa_central_standardized = bofa_standardizer(cc["bofa_central"], cc["chase_central"])
except Exception as e:
    print(f"Error in standardizing BofA statements, pls verify the statements are correct (BofA/Chase), if not contact the technical team")
    bofa_central_standardized = None

bofa_central_standardized

def create_central_df(
    chase_central: Optional[pd.DataFrame] = None,
    bofa_central_standardized: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Create Central DataFrame Function

    This function concatenates chase_central and bofa_central_standardized DataFrames
    into a single unified DataFrame called central_df.
    Adds a "Bank Name" column to identify the source: "CHASE" for Chase rows, "BofA" for BofA rows.

    Args:
        chase_central: Chase central DataFrame (defaults to global chase_central if not provided)
        bofa_central_standardized: Standardized BofA DataFrame (defaults to global bofa_central_standardized if not provided)

    Returns:
        pd.DataFrame: Concatenated DataFrame containing both Chase and BofA data with Bank Name column
    """
    # Access from global scope if not provided
    if chase_central is None:
        try:
            chase_central = globals().get('cc', {}).get('chase_central')
            if chase_central is None:
                raise ValueError("chase_central not found. Please provide it as parameter or ensure 'cc' dictionary exists.")
        except:
            raise ValueError("chase_central not found. Please provide it as parameter.")

    if bofa_central_standardized is None:
        try:
            bofa_central_standardized = globals().get('bofa_central_standardized')
            if bofa_central_standardized is None:
                raise ValueError("bofa_central_standardized not found. Please provide it as parameter or ensure it exists.")
        except:
            raise ValueError("bofa_central_standardized not found. Please provide it as parameter.")

    # Accept if either DataFrame is empty; produce a concatenated DataFrame of the non-empties
    # Collect all input DataFrames
    input_dfs = []
    df_sources = []
    if chase_central is not None and not chase_central.empty:
        chase_df = chase_central.copy()
        chase_df['Bank Name'] = 'CHASE'
        input_dfs.append(chase_df)
        df_sources.append('CHASE')
    else:
        print("✓ chase_central is empty or None -- skipping.")

    if bofa_central_standardized is not None and not bofa_central_standardized.empty:
        bofa_df = bofa_central_standardized.copy()
        bofa_df['Bank Name'] = 'BofA'
        input_dfs.append(bofa_df)
        df_sources.append('BofA')
    else:
        print("✓ bofa_central_standardized is empty or None -- skipping.")

    if not input_dfs:
        raise ValueError("Both chase_central and bofa_central_standardized are empty or None. Nothing to concatenate.")

    # Validate columns among all non-empty DataFrames (must all match)
    # Ignore "Bank Name" column (will add after comparison)
    def _columns_no_bankname(df):
        return set(col for col in df.columns if col != "Bank Name")

    reference_columns = _columns_no_bankname(input_dfs[0])
    for i, df in enumerate(input_dfs[1:], 1):
        if _columns_no_bankname(df) != reference_columns:
            first_source = df_sources[0]
            this_source = df_sources[i]
            missing_in_this = reference_columns - _columns_no_bankname(df)
            missing_in_first = _columns_no_bankname(df) - reference_columns
            error_msg = f"Column mismatch between {first_source} and {this_source}:\n"
            if missing_in_this:
                error_msg += f"  Missing in {this_source}: {missing_in_this}\n"
            if missing_in_first:
                error_msg += f"  Missing in {first_source}: {missing_in_first}"
            raise ValueError(error_msg)

    # Concatenate the DataFrames
    central_df = pd.concat(input_dfs, ignore_index=True)

    # Print summary
    print(f"\n=== Central DataFrame Created ===")
    for source, df in zip(df_sources, input_dfs):
        print(f"✓ {source} rows: {len(df)}")
    print(f"✓ Total rows in central_df: {len(central_df)}")
    print(f"✓ Columns: {list(central_df.columns)}")
    print(f"✓ Total columns: {len(central_df.columns)}")

    # Validate row count
    expected_rows = sum(len(df) for df in input_dfs)
    if len(central_df) != expected_rows:
        raise ValueError(
            f"Row count mismatch: Expected {expected_rows} rows, got {len(central_df)} rows"
        )
    else:
        print(f"✓ Row count validation passed: {expected_rows} rows")

    # Validate Bank Name column
    cols = central_df['Bank Name'].value_counts().to_dict()
    for source in df_sources:
        expected = len([df for src, df in zip(df_sources, input_dfs) if src == source][0])
        actual = cols.get(source, 0)
        if actual == expected:
            print(f"✓ Bank Name column validation passed: {actual} {source} rows")
        else:
            raise ValueError(
                f"Bank Name column mismatch: Expected {expected} {source} rows, got {actual} {source}"
            )

    print(f"\n✓ Central DataFrame creation complete!\n")
    return central_df

try:
    central_df = create_central_df(cc.get("chase_central", None), bofa_central_standardized)
    central_df = clean_column_names(central_df)
    central_df
except Exception as e:
    print(f"An error occurred while creating the central dataframe, pls verify the statements are correct (BofA/Chase), if not contact the technical team")
    central_df = None


import os
import glob
from pathlib import Path

def load_gwid_file(directory: str = None) -> pd.DataFrame:
    """
    Automatically find and load the GWID file from directory.
    
    Args:
        directory: Directory to search (defaults to script's directory)
    
    Returns:
        pd.DataFrame: Loaded and cleaned GWID DataFrame
    
    Raises:
        FileNotFoundError: If no GWID file found
        ValueError: If multiple GWID files found
    """
    # If no directory provided, use the directory where THIS script is located
    if directory is None:
        directory = os.path.dirname(os.path.abspath(__file__))
    
    # Search for GWID files (only in current directory, not subdirectories)
    gwid_patterns = [
        os.path.join(directory, "GWID_*")
    ]
    
    gwid_files = []
    for pattern in gwid_patterns:
        gwid_files.extend(glob.glob(pattern, recursive=False))
    
    # Filter to only Excel/CSV files
    gwid_files = [
        f for f in gwid_files 
        if f.lower().endswith(('.xls', '.xlsx', '.csv')) and os.path.isfile(f)
    ]
    
    # Remove duplicates (in case patterns overlap)
    gwid_files = list(set(gwid_files))
    
    if len(gwid_files) == 0:
        raise FileNotFoundError(
            f"No GWID file found in directory '{directory}'. "
            f"Expected a file with 'GWID_' in the name (e.g., 'GWID_SI.xls', 'GWID_SI.xlsx')"
        )
    elif len(gwid_files) > 1:
        raise ValueError(
            f"Multiple GWID files found ({len(gwid_files)} files). "
            f"Please ensure only one GWID file exists.\n"
            f"Found files:\n" + "\n".join(f"  - {f}" for f in gwid_files)
        )
    
    gwid_file = gwid_files[0]
    print(f"✓ Loading GWID file: {gwid_file}")
    
    # Load the file
    if gwid_file.lower().endswith('.csv'):
        gwids_df = pd.read_csv(gwid_file)
    else:
        gwids_df = pd.read_excel(gwid_file, engine='xlrd')
    
    # Clean column names
    gwids_df = clean_column_names(gwids_df)
    gwids_df = gwids_df.rename(columns={'crm_1_id': 'gwid'})
    
    return gwids_df

# Use the function with try-except
try:
    gwids_df = load_gwid_file()
    print(gwids_df["processor"].unique())
except Exception as e:
    print(f"Error loading the gateway id file, pls verify the file is correct, if not contact the technical team.")
    print(f"Actual error: {str(e)}")  # Print the actual error message
    import sys
    sys.exit(1)  # Stop the script here



import re
from rapidfuzz import process, fuzz

# one-time prep (do this once)
_CAND_MIDS = gwids_df["mid"].astype(str).str.strip()
_CAND_MIDS_D = _CAND_MIDS.str.replace(r"\D", "", regex=True).tolist()  # digits only

def match_mid_score(mid: str, score_cutoff: int = 85):
    """
    Input: a MID string.
    Output: (fuzz_score:int, best_match_mid:str from gwids_df) or None if below cutoff.
    """
    q = re.sub(r"\D", "", str(mid))  # normalize like NEF
    m = process.extractOne(q, _CAND_MIDS_D, scorer=fuzz.partial_ratio, score_cutoff=score_cutoff)
    if not m:
        return None
    _, score, idx = m
    return score, gwids_df["mid"].iloc[idx]

import pandas as pd

def single_pass_extract_mid_credit(
    df: pd.DataFrame,
    details_col: str = "details",
    description_col: str = "description",
    amount_col: str = "amount",
    restrict_to_credit: bool = True,
) -> pd.DataFrame:
    """
    One-pass, vectorized extraction with MID matcher:
      - Clean description (lower, remove spaces/specials)
      - MID ID: between 'indid' and ('indname'|'origid') → digits-only
      - Credit type: between 'coentrydescr' and 'sec' (after removing digits)
      - Optionally restrict rows to CREDIT
      - Returns df with clean_description, midid, credit_charge, matched_mid, gwid, processor, corp
    """
    # Clean description once
    desc = df[description_col].astype(str).str.lower()
    clean_desc = (
        desc.str.replace(r"\s+", "", regex=True)
            .str.replace(r"[^a-z0-9]", "", regex=True)
    )
    # Remove digits for credit charge extraction (matches NEF logic intent)
    no_digits = clean_desc.str.replace(r"\d+", "", regex=True)

    # Extract MID (digits) and credit charge (letters)
    midid = clean_desc.str.extract(r"indid(\d+?)(?=indname|origid)", expand=False)
    credit_charge = no_digits.str.extract(r"coentrydescr([a-z]+?)(?=sec)", expand=False)

    out = df.copy()
    out["clean_description"] = clean_desc
    out["midid"] = midid
    out["credit_charge"] = credit_charge

    if restrict_to_credit:
        out = out[out[details_col].astype(str).str.upper() == "CREDIT"]

    # Build dictionaries to map matched_mid to gwid, processor, and corp
    # Ensure all are strings and stripped
    _tmp_gwids = (
        gwids_df.dropna(subset=["mid", "gwid"])
        .assign(
            mid=lambda d: d["mid"].astype(str).str.strip(),
            gwid=lambda d: d["gwid"].astype(str).str.strip(),
            processor=lambda d: d["processor"].astype(str).str.strip(),
            corp=lambda d: d["corp"].astype(str).str.strip()
        )
    )
    mid_to_gwid = _tmp_gwids.set_index("mid")["gwid"].to_dict()
    mid_to_processor = _tmp_gwids.set_index("mid")["processor"].to_dict()
    mid_to_corp = _tmp_gwids.set_index("mid")["corp"].to_dict()

    matched_mids = []
    gwid_matches = []
    processor_matches = []
    corp_matches = []
    for idx, row in out.iterrows():
        m_raw = row["midid"]
        if pd.isnull(m_raw) or str(m_raw).strip() == "":
            matched_mids.append("")
            gwid_matches.append("")
            processor_matches.append("")
            corp_matches.append("")
            continue
        result = match_mid_score(str(m_raw))
        if result is not None:
            score, real_mid = result
            matched_mids.append(real_mid)
            mid_key = str(real_mid).strip()
            gwid_val = mid_to_gwid.get(mid_key, "")
            processor_val = mid_to_processor.get(mid_key, "")
            corp_val = mid_to_corp.get(mid_key, "")
            gwid_matches.append(gwid_val)
            processor_matches.append(processor_val)
            corp_matches.append(corp_val)
            if score < 85:
                print(f"Transaction {idx} has low match score: {score} for midid '{m_raw}' (matched MID: '{real_mid}')")
        else:
            matched_mids.append("")
            gwid_matches.append("")
            processor_matches.append("")
            corp_matches.append("")
            print(f"Transaction {idx} has NO possible MID match for midid '{m_raw}'")

    out["matched_mid"] = matched_mids
    out["gwid"] = gwid_matches
    out["processor"] = processor_matches
    out["corp"] = corp_matches

    return out

result_df = single_pass_extract_mid_credit(central_df)

# Get the min max date of the result_df
# We need to get the min max date of the result_df
min_date = result_df["posting_date"].min()
max_date = result_df["posting_date"].max()
print(f"Min date: {min_date}, Max date: {max_date}")

# Create date metadata dictionary
date_metadata = {
    'min_date': min_date,
    'max_date': max_date,
    'min_date_str': min_date.strftime('%Y/%m/%d'),  # e.g., "2025/07/30"
    'max_date_str': max_date.strftime('%Y/%m/%d'),  # e.g., "2025/10/03"
    'min_date_display': min_date.strftime('%b %d, %Y'),  # For display in reports
    'max_date_display': max_date.strftime('%b %d, %Y')
}
data_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
data_dir = Path.cwd()  # 

# Save date metadata
with open(data_dir / 'deprec_date_metadata.pkl', 'wb') as f:
    pickle.dump(date_metadata, f)
print("✓ Saved: date_metadata (min/max dates from result_df)")
print(f"  Date range: {date_metadata['min_date_display']} → {date_metadata['max_date_display']}")

print("\n✓ All data saved successfully! CRM report integrator can now load this data.")
print(f"✓ Data saved in: {data_dir}")
print("="*80 + "\n")

# Debug: identify and categorize rows missing MID IDs
missing_mid = result_df[result_df["midid"].isna() | (result_df["midid"].astype(str).str.strip() == "")]
total_rows = len(result_df)
num_missing = len(missing_mid)
print(f"Total rows after extraction: {total_rows}")
print(f"Rows missing MID: {num_missing} ({(num_missing/total_rows*100 if total_rows else 0):.2f}%)")

# Heuristics on why it’s missing, based on cleaned description
cd = result_df["clean_description"].fillna("").astype(str)
has_indid = cd.str.contains("indid")
has_end = cd.str.contains("indname") | cd.str.contains("origid")

no_indid = missing_mid[~has_indid.loc[missing_mid.index]]
indid_no_end = missing_mid[has_indid.loc[missing_mid.index] & ~has_end.loc[missing_mid.index]]
bad_window = missing_mid.index.difference(no_indid.index).difference(indid_no_end.index)

print(f"- Missing 'indid' token: {len(no_indid)}")
print(f"- Has 'indid' but missing an end token ('indname'/'origid'): {len(indid_no_end)}")
print(f"- Has tokens but window invalid/regex didn’t match: {len(bad_window)}")

# Show samples for inspection
print("\nSample: missing 'indid'")
print(no_indid[["details","description"]].head(5))

print("\nSample: has 'indid' but no end token")
print(indid_no_end[["details","description"]].head(5))

print("\nSample: invalid window / didn’t match")
print(result_df.loc[bad_window, ["details","description"]].head(5))

import pandas as pd

def extract_mid_chase(
    df: pd.DataFrame,
    description_col: str = "description",
) -> pd.DataFrame:
    """
    Extract MID from CHASE bank format.
    Pattern: indid<digits>indname or indid<digits>origid
    """
    desc = df[description_col].astype(str).str.lower()
    clean_desc = (
        desc.str.replace(r"\s+", "", regex=True)
            .str.replace(r"[^a-z0-9]", "", regex=True)
    )
    no_digits = clean_desc.str.replace(r"\d+", "", regex=True)
    
    # Chase pattern
    midid = clean_desc.str.extract(r"indid(\d+?)(?=indname|origid)", expand=False)
    credit_charge = no_digits.str.extract(r"coentrydescr([a-z]+?)(?=sec)", expand=False)
    
    out = df.copy()
    out["clean_description"] = clean_desc
    out["midid"] = midid
    out["credit_charge"] = credit_charge
    
    return out


def extract_mid_bofa(
    df: pd.DataFrame,
    description_col: str = "description",
) -> pd.DataFrame:
    """
    Extract MID from Bank of America format.
    Pattern: id:<digits>indn:
    """
    desc = df[description_col].astype(str).str.lower()
    clean_desc = (
        desc.str.replace(r"\s+", "", regex=True)
            .str.replace(r"[^a-z0-9:]", "", regex=True)  # Keep colons for BofA
    )
    no_digits = clean_desc.str.replace(r"\d+", "", regex=True)
    
    # BofA pattern: id:<digits>indn:
    midid = clean_desc.str.extract(r"id:(\d+?)(?=indn:)", expand=False)
    
    # Credit charge extraction (if exists in BofA format)
    credit_charge = no_digits.str.extract(r"des:([a-z]+?)(?=id:|$)", expand=False)
    
    out = df.copy()
    out["clean_description"] = clean_desc
    out["midid"] = midid
    out["credit_charge"] = credit_charge
    
    return out


def process_multi_bank(
    df: pd.DataFrame,
    bank_name_col: str = "bank_name",
    details_col: str = "details",
    description_col: str = "description",
    restrict_to_credit: bool = True,
) -> pd.DataFrame:
    """
    Process multiple banks - calls appropriate extraction based on bank_name.
    Supports: CHASE, BofA
    
    Returns DataFrame with: clean_description, midid, credit_charge, matched_mid, gwid, processor, corp
    """
    result_dfs = []
    
    print("\n=== Starting Multi-Bank MID Extraction ===\n")
    
    # Group by bank name
    for bank_name, group_df in df.groupby(bank_name_col):
        bank_lower = str(bank_name).lower().strip()
        
        print(f"Processing {len(group_df)} rows for bank: {bank_name}")
        
        # Route to appropriate extraction function
        if "chase" in bank_lower:
            extracted = extract_mid_chase(group_df, description_col=description_col)
        elif "bofa" in bank_lower or "bank of america" in bank_lower or "boa" in bank_lower:
            extracted = extract_mid_bofa(group_df, description_col=description_col)
        else:
            print(f"⚠️  Unknown bank format: {bank_name}, defaulting to CHASE pattern")
            extracted = extract_mid_chase(group_df, description_col=description_col)
        
        # Restrict to CREDIT if needed
        if restrict_to_credit:
            before_filter = len(extracted)
            extracted = extracted[extracted[details_col].astype(str).str.upper() == "CREDIT"]
            after_filter = len(extracted)
            print(f"  → Filtered to CREDIT only: {after_filter}/{before_filter} rows")
        
        # Count extracted MIDs for this bank
        extracted_count = extracted["midid"].notna().sum()
        print(f"  → Extracted MIDs: {extracted_count}/{len(extracted)} rows ({extracted_count/len(extracted)*100:.1f}%)")
        
        result_dfs.append(extracted)
    
    # Combine all banks
    result_df = pd.concat(result_dfs, ignore_index=True)
    
    print(f"\n=== Combined Results ===")
    print(f"Total rows: {len(result_df)}")
    print(f"Total MIDs extracted: {result_df['midid'].notna().sum()} ({result_df['midid'].notna().sum()/len(result_df)*100:.1f}%)")
    
    # Now do the MID matching (same for all banks)
    print(f"\n=== Starting Fuzzy MID Matching ===\n")
    
    _tmp_gwids = (
        gwids_df.dropna(subset=["mid", "gwid"])
        .assign(
            mid=lambda d: d["mid"].astype(str).str.strip(),
            gwid=lambda d: d["gwid"].astype(str).str.strip(),
            processor=lambda d: d["processor"].astype(str).str.strip(),
            corp=lambda d: d["corp"].astype(str).str.strip()
        )
    )
    mid_to_gwid = _tmp_gwids.set_index("mid")["gwid"].to_dict()
    mid_to_processor = _tmp_gwids.set_index("mid")["processor"].to_dict()
    mid_to_corp = _tmp_gwids.set_index("mid")["corp"].to_dict()
    
    matched_mids = []
    gwid_matches = []
    processor_matches = []
    corp_matches = []
    match_scores = []
    low_confidence_count = 0
    no_match_count = 0
    
    for idx, row in result_df.iterrows():
        m_raw = row["midid"]
        if pd.isnull(m_raw) or str(m_raw).strip() == "":
            matched_mids.append("")
            gwid_matches.append("")
            processor_matches.append("")
            corp_matches.append("")
            match_scores.append(None)
            continue
        
        result = match_mid_score(str(m_raw))
        if result is not None:
            score, real_mid = result
            matched_mids.append(real_mid)
            match_scores.append(score)
            mid_key = str(real_mid).strip()
            gwid_val = mid_to_gwid.get(mid_key, "")
            processor_val = mid_to_processor.get(mid_key, "")
            corp_val = mid_to_corp.get(mid_key, "")
            gwid_matches.append(gwid_val)
            processor_matches.append(processor_val)
            corp_matches.append(corp_val)
            
            if score < 85:
                low_confidence_count += 1
                print(f"⚠️  Low confidence match (score={score}): midid='{m_raw}' → matched_mid='{real_mid}'")
        else:
            matched_mids.append("")
            gwid_matches.append("")
            processor_matches.append("")
            corp_matches.append("")
            match_scores.append(None)
            no_match_count += 1
            print(f"❌ No match found for midid: '{m_raw}'")
    
    result_df["matched_mid"] = matched_mids
    result_df["gwid"] = gwid_matches
    result_df["processor"] = processor_matches
    result_df["corp"] = corp_matches
    result_df["match_score"] = match_scores
    
    # Validation summary
    print(f"\n=== Fuzzy Matching Summary ===")
    print(f"✓ High confidence matches (≥85%): {len(result_df) - low_confidence_count - no_match_count}")
    print(f"⚠️  Low confidence matches (<85%): {low_confidence_count}")
    print(f"❌ No matches found: {no_match_count}")
    
    # Breakdown by bank
    print(f"\n=== Results by Bank ===")
    for bank_name, group in result_df.groupby(bank_name_col):
        total = len(group)
        extracted = group["midid"].notna().sum()
        matched_count = ((group["matched_mid"].notna()) & (group["matched_mid"] != "")).sum()
        print(f"{bank_name}: {total} rows, {extracted} extracted ({extracted/total*100:.1f}%), {matched_count} matched ({matched_count/total*100:.1f}%)")
    
    return result_df


# Run the multi-bank extraction
result_df = process_multi_bank(
    central_df,
    bank_name_col="bank_name",
    details_col="details", 
    description_col="description",
    restrict_to_credit=True
)

# Diagnostic analysis
print(f"\n" + "="*80)
print("EXTRACTION DIAGNOSTICS")
print("="*80)

missing_mid = result_df[result_df["midid"].isna() | (result_df["midid"].astype(str).str.strip() == "")]
total_rows = len(result_df)
num_missing = len(missing_mid)

print(f"\nTotal rows after extraction: {total_rows}")
print(f"Rows missing MID: {num_missing} ({(num_missing/total_rows*100 if total_rows else 0):.2f}%)")
print(f"Rows with MID: {total_rows - num_missing} ({((total_rows-num_missing)/total_rows*100 if total_rows else 0):.2f}%)")

# Breakdown by bank for missing MIDs
print(f"\n=== Missing MIDs by Bank ===")
for bank_name in result_df["bank_name"].unique():
    bank_df = result_df[result_df["bank_name"] == bank_name]
    bank_missing = missing_mid[missing_mid["bank_name"] == bank_name]
    print(f"{bank_name}: {len(bank_missing)}/{len(bank_df)} missing ({len(bank_missing)/len(bank_df)*100:.1f}%)")

# Analyze why MIDs are missing (heuristics)
cd = result_df["clean_description"].fillna("").astype(str)

# Check for Chase patterns
has_indid = cd.str.contains("indid")
has_indname_origid = cd.str.contains("indname") | cd.str.contains("origid")

# Check for BofA patterns  
has_id_colon = cd.str.contains("id:")
has_indn_colon = cd.str.contains("indn:")

print(f"\n=== Pattern Analysis (Missing MIDs) ===")

# Chase pattern failures
chase_missing = missing_mid[missing_mid["bank_name"] == "CHASE"]
if len(chase_missing) > 0:
    print(f"\nChase ({len(chase_missing)} missing):")
    no_indid = chase_missing[~has_indid.loc[chase_missing.index]]
    indid_no_end = chase_missing[has_indid.loc[chase_missing.index] & ~has_indname_origid.loc[chase_missing.index]]
    print(f"  - Missing 'indid' token: {len(no_indid)}")
    print(f"  - Has 'indid' but missing end token ('indname'/'origid'): {len(indid_no_end)}")
    print(f"  - Other/regex didn't match: {len(chase_missing) - len(no_indid) - len(indid_no_end)}")

# BofA pattern failures
bofa_missing = missing_mid[missing_mid["bank_name"] == "BofA"]
if len(bofa_missing) > 0:
    print(f"\nBofA ({len(bofa_missing)} missing):")
    no_id = bofa_missing[~has_id_colon.loc[bofa_missing.index]]
    id_no_indn = bofa_missing[has_id_colon.loc[bofa_missing.index] & ~has_indn_colon.loc[bofa_missing.index]]
    print(f"  - Missing 'id:' token: {len(no_id)}")
    print(f"  - Has 'id:' but missing 'indn:' token: {len(id_no_indn)}")
    print(f"  - Other/regex didn't match: {len(bofa_missing) - len(no_id) - len(id_no_indn)}")

# Show samples
print(f"\n=== Sample Missing MIDs (first 5 per bank) ===")
for bank_name in missing_mid["bank_name"].unique():
    bank_samples = missing_mid[missing_mid["bank_name"] == bank_name].head(5)
    if len(bank_samples) > 0:
        print(f"\n{bank_name}:")
        print(bank_samples[["bank_name", "description", "clean_description"]].to_string())

print("\n" + "="*80)
print("✓ Multi-bank extraction complete!")
print("="*80)

result_df

# Split into two DataFrames: result_df (no "rel" in 'credit_charge'), releases_df (has "rel" in 'credit_charge')
# Split into two DataFrames: result_df (no reserve releases), releases_df (reserve releases)
releases_df = result_df[
    result_df["credit_charge"].str.contains("rel", na=False) | 
    result_df["credit_charge"].str.contains("rr", na=False) |
    result_df["credit_charge"].str.contains("resrv", na=False)
]
result_df = result_df[
    ~(result_df["credit_charge"].str.contains("rel", na=False) | 
      result_df["credit_charge"].str.contains("rr", na=False) |
      result_df["credit_charge"].str.contains("resrv", na=False))
]

result_df["credit_charge"].unique()

import os
from typing import List, Dict, Optional
import anthropic



def generate_credit_designation_descriptions(
    designations: List[str],
    api_key: Optional[str] = None
) -> Dict[str, str]:
    """
    Generate 2-3 word descriptions for credit transaction designations in ONE API call.
    
    MUCH FASTER: Uses a single batch prompt instead of individual calls.
    
    Args:
        designations: List of credit transaction designation strings
        api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
    
    Returns:
        Dict[str, str]: Dictionary mapping designation -> description
    """
    # Get API key
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if api_key is None:
            raise ValueError(
                "API key not provided. Either pass it as a parameter or set the "
                "ANTHROPIC_API_KEY environment variable."
            )
    
    # Filter out None/NaN values
    clean_designations = [d for d in designations if d and str(d).strip() and str(d).lower() != 'nan']
    
    if not clean_designations:
        return {}
    
    client = anthropic.Anthropic(api_key=api_key)
    
    # Create batch prompt with all designations at once
    designations_list = "\n".join([f"{i+1}. {d}" for i, d in enumerate(clean_designations)])
    
    prompt = f"""You are analyzing credit transaction designations from Chase or Bank of America bank statements.

For EACH designation below, provide a concise 2-3 word description. Format your response as:
1. designation_code | Description Here
2. designation_code | Description Here

Examples:
- deposit → Merchant Deposit
- mtotdep → Monthly Total Deposit
- merchdep → Merchant Deposit Payment
- tpresrel → Reserve Release
- bkrddep → Bankcard Deposit

Here are the designations to describe:
{designations_list}

Provide ONLY the numbered list with pipe-separated designation|description pairs. No extra text."""
    
    print(f"Generating descriptions for {len(clean_designations)} designations in ONE batch call...")
    
    try:
        message = client.messages.create(
            model="claude-3-5-haiku-20241022",  # FIXED: Use Haiku (it exists and is fast!)
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text.strip()
        
        # Parse response
        results = {}
        for line in response_text.split('\n'):
            line = line.strip()
            if not line or not any(c.isalpha() for c in line):
                continue
            
            # Remove leading number and period
            if line[0].isdigit():
                line = line.split('.', 1)[-1].strip()
            
            # Split on pipe or arrow or colon
            if '|' in line:
                parts = line.split('|', 1)
            elif '→' in line:
                parts = line.split('→', 1)
            elif ':' in line:
                parts = line.split(':', 1)
            else:
                # Try splitting on first space after first word
                parts = line.split(None, 1)
            
            if len(parts) == 2:
                designation = parts[0].strip()
                description = parts[1].strip()
                results[designation] = description
                print(f"  ✓ {designation} → {description}")
        
        # Handle any missing designations with fallback
        for d in clean_designations:
            if d not in results:
                results[d] = f"Transaction: {d.title()}"
                print(f"  ⚠ {d} → {results[d]} (fallback)")
        
        print(f"\n✓ Generated {len(results)} descriptions in ONE API call!")
        return results
        
    except Exception as e:
        print(f"❌ Error generating descriptions: {e}")
        # Return fallback descriptions
        return {d: f"Transaction: {d.title()}" for d in clean_designations}


# Example usage:
descriptions_dict = generate_credit_designation_descriptions(result_df["credit_charge"].unique())
print(descriptions_dict)

# Build 3 pivot tables from result_df: by GWID, by Processor, by Corp

# Ensure numeric amounts
result_df["amount"] = pd.to_numeric(result_df["amount"], errors="coerce").fillna(0.0)

# 1) GWID columns
gwid_df = (
    result_df.dropna(subset=["gwid"])
    .pivot_table(index="credit_charge", columns="gwid", values="amount", aggfunc="sum", fill_value=0)
)
# optional: sort gwid columns numerically if they are digits
try:
    gwid_df = gwid_df.reindex(sorted(gwid_df.columns, key=lambda x: int(str(x))), axis=1)
except Exception:
    pass

# 2) Processor columns
processor_df = (
    result_df.dropna(subset=["processor"])
    .pivot_table(index="credit_charge", columns="processor", values="amount", aggfunc="sum", fill_value=0)
    .sort_index(axis=1)
)

# 3) Corp columns (expects a corp-like column)
corp_col = "corp" if "corp" in result_df.columns else ("merchant_group" if "merchant_group" in result_df.columns else None)
if corp_col is None:
    raise ValueError("No corp column found on result_df (expected 'corp' or 'merchant_group').")

corps_df = (
    result_df.dropna(subset=[corp_col])
    .pivot_table(index="credit_charge", columns=corp_col, values="amount", aggfunc="sum", fill_value=0)
    .sort_index(axis=1)
)

gwid_df

processor_df

corps_df

import pandas as pd, numpy as np, math

# Ensure numeric
central_df["amount"] = pd.to_numeric(central_df["amount"], errors="coerce")
result_df["amount"]  = pd.to_numeric(result_df["amount"],  errors="coerce")

# Totals
total_central_all     = central_df["amount"].sum()
total_central_credit  = central_df.loc[central_df["details"].astype(str).str.upper()=="CREDIT", "amount"].sum()
total_result          = result_df["amount"].sum()

def pivot_total_exclude_totals(pvt: pd.DataFrame) -> float:
    # Exclude 'totals' column if present
    cols = [col for col in pvt.columns if str(col).lower() != "totals"]
    return pd.DataFrame(pvt)[cols].select_dtypes(include="number").to_numpy().sum()

total_gwid      = pivot_total_exclude_totals(gwid_df)
total_processor = pivot_total_exclude_totals(processor_df)
total_corps     = pivot_total_exclude_totals(corps_df)

# Pretty output header
print("=" * 44)
print("  📊 CENTRAL & PIVOTS TOTALS SUMMARY")
print("=" * 44)
rows = [
    ("Central Total (ALL)",              total_central_all),
    ("Central Total (CREDIT)",           total_central_credit),
    ("Result_df (final merged)",         total_result),
    ("GWID Pivot (sum, excl. totals)",                 total_gwid),
    ("Processor Pivot (sum, excl. totals)",            total_processor),
    ("Corp Pivot (sum, excl. totals)",                 total_corps)
]

for desc, val in rows:
    print(f"{desc:<36} : {val:,.2f}")

print("\nConsistency Checks")
print("-" * 44)

checks = {
    "GWID Pivot == Result"          : math.isclose(total_gwid,      total_result, rel_tol=1e-9, abs_tol=1e-6),
    "Processor Pivot == Result"     : math.isclose(total_processor, total_result, rel_tol=1e-9, abs_tol=1e-6),
    "Corp Pivot == Result"          : math.isclose(total_corps,     total_result, rel_tol=1e-9, abs_tol=1e-6),
    "Central (CREDIT) == Result"    : math.isclose(total_central_credit, total_result, rel_tol=1e-9, abs_tol=1e-6)
}
for name, value in checks.items():
    print(f"{name:<32}: {'✔️' if value else '❌'}")

print("=" * 44)

# Map credit charge codes to descriptions (from AI-generated dict)
desc_map = descriptions_dict  # From previous cell





def map_credit_charge(idx):
    """Map credit charge codes to human-readable descriptions."""
    if isinstance(idx, (list, pd.Index, np.ndarray)):
        return [desc_map.get(x, x) for x in idx]
    else:
        return desc_map.get(idx, idx)



# 0) Ensure amounts are numeric
result_df["amount"] = pd.to_numeric(result_df["amount"], errors="coerce").fillna(0.0)

# 1) Build pivots
print("Building pivot tables...")

# GWID pivot
gwid_df = (
    result_df.dropna(subset=["gwid"])
    .pivot_table(index="credit_charge", columns="gwid", values="amount", aggfunc="sum", fill_value=0)
)
try:
    gwid_df = gwid_df.reindex(sorted(gwid_df.columns, key=lambda x: int(str(x))), axis=1)
except Exception:
    pass

# Processor pivot
processor_df = (
    result_df.dropna(subset=["processor"])
    .pivot_table(index="credit_charge", columns="processor", values="amount", aggfunc="sum", fill_value=0)
    .sort_index(axis=1)
)

# Corp pivot (handle missing column gracefully)
corp_col = None
if "corp" in result_df.columns:
    corp_col = "corp"
elif "merchant_group" in result_df.columns:
    corp_col = "merchant_group"

if corp_col is None:
    print("⚠ Warning: No corp column found - skipping corp pivot")
    corps_df = pd.DataFrame()
else:
    corps_df = (
        result_df.dropna(subset=[corp_col])
        .pivot_table(index="credit_charge", columns=corp_col, values="amount", aggfunc="sum", fill_value=0)
        .sort_index(axis=1)
    )

print(f"✓ Created {len(gwid_df.columns)} GWID columns")
print(f"✓ Created {len(processor_df.columns)} Processor columns")
if not corps_df.empty:
    print(f"✓ Created {len(corps_df.columns)} Corp columns")

# 2) Add totals column to each pivot
for _df in [gwid_df, processor_df, corps_df]:
    if not _df.empty:
        _df["TOTALS"] = _df.sum(axis=1)

# 3) Add totals row to each pivot
for _df in [gwid_df, processor_df, corps_df]:
    if not _df.empty:
        _df.loc["TOTALS"] = _df.sum(axis=0)

# 4) Helper: prep DataFrame for Excel (uppercase headers, make index a column)
def prep_for_excel(df: pd.DataFrame, index_name: str = "CREDIT_NAME") -> pd.DataFrame:
    """
    Prepare pivot DataFrame for Excel export.
    - Resets index to make credit_charge a column
    - Adds human-readable descriptions
    - Formats column names for export
    """
    if df.empty:
        return df
    
    out = df.copy().reset_index()
    
    # Identify the credit charge column
    if out.columns[0] != "credit_charge":
        credit_col = out.columns[0]
    else:
        credit_col = "credit_charge"
    
    # Add human-readable descriptions
    descs = []
    for val in out[credit_col]:
        if val == "TOTALS":
            descs.append("")
        else:
            descs.append(desc_map.get(val, ""))
    
    out.insert(1, "CREDIT_DESC", descs)
    
    # Uppercase all column names
    out.columns = [str(c).upper() for c in out.columns]
    
    # Rename first column to index_name
    if out.columns[0] != index_name:
        out = out.rename(columns={out.columns[0]: index_name})
    
    # Change to proper case for export
    export_columns = list(out.columns)
    new_columns = []
    for col in export_columns:
        if col == "CREDIT_NAME":
            new_columns.append("Credit Name")
        elif col == "CREDIT_DESC":
            new_columns.append("Credit Desc")
        else:
            new_columns.append(col)
    out.columns = new_columns
    
    return out

# 5) Prepare all pivots for export
gwid_out = prep_for_excel(gwid_df)
processor_out = prep_for_excel(processor_df)
corps_out = prep_for_excel(corps_df) if not corps_df.empty else pd.DataFrame()

print("\n✓ All pivot tables prepared for export!")
print(f"  - GWID pivot: {len(gwid_out)} rows × {len(gwid_out.columns)} columns")
print(f"  - Processor pivot: {len(processor_out)} rows × {len(processor_out.columns)} columns")
if not corps_out.empty:
    print(f"  - Corp pivot: {len(corps_out)} rows × {len(corps_out.columns)} columns")

# Display sample
print("\nGWID Pivot (first 5 rows):")
gwid_out.head()

# ============================================================================
# SAVE DATA FOR CRM_REPORT_INTEGRATOR_WITH_BANK_STATEMENTS.PY
# ============================================================================
# Save all required variables to pickle files so they can be loaded by the CRM report script
# This allows the scripts to run independently on a server

from pathlib import Path
import pickle

# Define data directory (same directory as scripts)
data_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
data_dir = Path.cwd()  
# Save all required DataFrames and variables
print("\n" + "="*80)
print("SAVING DATA FOR CRM REPORT INTEGRATOR")
print("="*80)

try:
    # Save DataFrames
    with open(data_dir / 'deprec_gwids_df.pkl', 'wb') as f:
        pickle.dump(gwids_df, f)
    print("✓ Saved: gwids_df")
    
    with open(data_dir / 'deprec_gwid_df.pkl', 'wb') as f:
        pickle.dump(gwid_df, f)
    print("✓ Saved: gwid_df")
    
    with open(data_dir / 'deprec_processor_df.pkl', 'wb') as f:
        pickle.dump(processor_df, f)
    print("✓ Saved: processor_df")
    
    with open(data_dir / 'deprec_corps_df.pkl', 'wb') as f:
        pickle.dump(corps_df, f)
    print("✓ Saved: corps_df")
    
    with open(data_dir / 'deprec_result_df.pkl', 'wb') as f:
        pickle.dump(result_df, f)
    print("✓ Saved: result_df")
    
    with open(data_dir / 'deprec_releases_df.pkl', 'wb') as f:
        pickle.dump(releases_df, f)
    print("✓ Saved: releases_df")
    
    with open(data_dir / 'deprec_central_df.pkl', 'wb') as f:
        pickle.dump(central_df, f)
    print("✓ Saved: central_df")

    # Save date metadata
    with open(data_dir / 'deprec_date_metadata.pkl', 'wb') as f:
        pickle.dump(date_metadata, f)
    print("✓ Saved: date_metadata (min/max dates from result_df)")
    print(f"  Date range: {date_metadata['min_date_display']} → {date_metadata['max_date_display']}")

    print("\n✓ All data saved successfully! CRM report integrator can now load this data.")
    print(f"✓ Data saved in: {data_dir}")
    print("="*80 + "\n")

except Exception as e:
    print(f"\n❌ ERROR: Failed to save data for CRM report integrator: {e}")
    print("⚠️  CRM report integrator may not have access to required data.")
    raise