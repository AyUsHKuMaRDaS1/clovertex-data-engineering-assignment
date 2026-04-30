"""Genomics filtering logic for clinically meaningful variant calls."""

import pandas as pd


def filter_genomics(cleaned_data):
    """Filter genomics variants for high-quality pathogenic or likely pathogenic calls."""
    if "genomics_variants" not in cleaned_data:
        return None

    df = cleaned_data["genomics_variants"].copy()

    # Normalize the join key to a shared patient_id field
    if "patient_ref" in df.columns:
        df = df.rename(columns={"patient_ref": "patient_id"})

    # Normalize clinical significance values for consistent filtering
    df["clinical_significance"] = df["clinical_significance"].astype(str).str.strip().str.title()

    # convert numeric columns
    df["read_depth"] = pd.to_numeric(df["read_depth"], errors="coerce")
    df["allele_frequency"] = pd.to_numeric(df["allele_frequency"], errors="coerce")

    # filter logic
    filtered_df = df[
        (df["read_depth"] > 10) &
        (df["allele_frequency"] > 0.2) &
        (df["clinical_significance"].isin(["Pathogenic", "Likely Pathogenic"]))
    ]

    return filtered_df