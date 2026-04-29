import pandas as pd


def filter_genomics(cleaned_data):
    if "genomics_variants" not in cleaned_data:
        return None

    df = cleaned_data["genomics_variants"].copy()

    # fix join column
    if "patient_ref" in df.columns:
        df = df.rename(columns={"patient_ref": "patient_id"})

    # ✅ ADD THIS LINE HERE (IMPORTANT)
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