import os
import pandas as pd


def save_to_parquet(df, name):
    output_path = os.path.join(os.getcwd(), "datalake", "refined")
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, f"{name}.parquet")
    df.to_parquet(file_path, index=False)

    print(f"Saved: {file_path}")


def save_partitioned_lab_results(cleaned_data):
    if "site_gamma_lab_results" not in cleaned_data:
        print("No lab results found")
        return

    lab_df = cleaned_data["site_gamma_lab_results"].copy()

    if "patient_ref" in lab_df.columns:
        lab_df = lab_df.rename(columns={"patient_ref": "patient_id"})

    # Convert numeric columns safely before saving parquet
    if "test_value" in lab_df.columns:
        lab_df["test_value"] = pd.to_numeric(lab_df["test_value"], errors="coerce")

    if "reference_range_low" in lab_df.columns:
        lab_df["reference_range_low"] = pd.to_numeric(
            lab_df["reference_range_low"],
            errors="coerce"
        )

    if "reference_range_high" in lab_df.columns:
        lab_df["reference_range_high"] = pd.to_numeric(
            lab_df["reference_range_high"],
            errors="coerce"
        )

    output_path = os.path.join(
        os.getcwd(),
        "datalake",
        "refined",
        "lab_results"
    )

    os.makedirs(output_path, exist_ok=True)

    if "test_name" in lab_df.columns:
        lab_df.to_parquet(
            output_path,
            index=False,
            partition_cols=["test_name"]
        )

        print(f"Partitioned lab results saved: {output_path}")
    else:
        file_path = os.path.join(
            os.getcwd(),
            "datalake",
            "refined",
            "lab_results.parquet"
        )

        lab_df.to_parquet(file_path, index=False)
        print(f"Lab results saved without partition: {file_path}")