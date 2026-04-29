import pandas as pd


def flatten_dict_columns(df):
    """
    Converts nested dictionary columns into normal flat columns.
    Example:
    name = {"given": "Amit", "family": "Das"}
    becomes:
    name_given, name_family
    """
    df = df.copy()

    for col in list(df.columns):
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = pd.json_normalize(df[col])
            expanded.columns = [
                f"{col}_{sub_col}".lower().replace(" ", "_")
                for sub_col in expanded.columns
            ]

            df = df.drop(columns=[col])
            df = pd.concat([df.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)

    return df


def clean_data(df):
    rows_in = len(df)

    # 1. standardize column names
    df = df.copy()
    df.columns = [
        col.strip().lower().replace(" ", "_")
        for col in df.columns
    ]

    # 2. flatten nested JSON/dict columns
    df = flatten_dict_columns(df)

    # 3. count nulls
    nulls_handled = int(df.isnull().sum().sum())

    # 4. fill null values
    df = df.fillna("unknown")

    # 5. remove duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    duplicates_removed = before - len(df)

    rows_out = len(df)

    issues = {
        "duplicates_removed": int(duplicates_removed),
        "nulls_handled": nulls_handled,
        "encoding_fixed": 0
    }

    return df, rows_in, rows_out, issues