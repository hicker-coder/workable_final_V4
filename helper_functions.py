
import os
import pandas as pd
from pprint import pprint


def read_file(file_path: str) -> pd.DataFrame:
    # Derive the file extension using os module

    SUPPORTED_EXTENSIONS = {
        ".csv": pd.read_csv,
        ".xlsx": pd.read_excel,
        ".xls": pd.read_excel,
        ".json": pd.read_json,
        ".parquet": pd.read_parquet,
        ".pickle": pd.read_pickle,
        ".feather": pd.read_feather,
        ".html": pd.read_html,
        ".sql": pd.read_sql,
        ".txt": pd.read_table
    }
    READ_FUNCTIONS = {
        ".csv": pd.read_csv,
        ".xlsx": pd.read_excel,
        ".xls": pd.read_excel,
        ".json": pd.read_json,
        ".parquet": pd.read_parquet,
        ".pickle": pd.read_pickle,
        ".feather": pd.read_feather,
        ".html": pd.read_html,
        ".sql": pd.read_sql,
        ".txt": pd.read_table
    }
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()

    # Ensure the file extension is supported
    assert file_extension in SUPPORTED_EXTENSIONS, "Unsupported file format. Only CSV, Excel, JSON, Parquet, Pickle, Feather, HTML, SQL, and Text files are supported."

    # Retrieve the appropriate read function based on the file extension
    read_function = READ_FUNCTIONS[file_extension]

    # Read the file using the read function
    df = read_function(file_path)

    return df

def validate_dataframe(df, required_cols):
    """
    Validates the dataframe for required columns and emptiness.
    """
    assert isinstance(df, pd.DataFrame), "Input must be a pandas DataFrame"

    # Check if required columns are present
    required_cols = [col.strip().lower() for col in required_cols]
    missing_cols = [col for col in required_cols if col not in [col.strip().lower() for col in df.columns]]

    if missing_cols:
        print("Missing columns in dataframe:")
        pprint(missing_cols, indent=4)
        return False

    # Check if the dataframe is empty
    if df.empty:
        print("Dataframe is empty.")
        return False

    return True
