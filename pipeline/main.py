import os
import sys
import pandas as pd

sys.path.append(os.getcwd())
from ingestion.load_data import load_data
from cleaning.clean_data import clean_data
from cleaning.unify import unify_patients
from transformation.join import join_all
from transformation.genomics import filter_genomics
from utils.save import save_to_parquet, save_partitioned_lab_results
from utils.datalake import copy_raw_data
from utils.manifest import generate_all_manifests
from stats.analytics import run_task_3
from plots.visualization import run_task_4


def main():
    print("\nCopying Raw Data...\n")
    copy_raw_data()

    data = load_data()
    cleaned_data = {}
    quality_rows = []

    print("\nCleaning Data...\n")

    for name, df in data.items():
        df_clean, rows_in, rows_out, issues = clean_data(df)
        cleaned_data[name] = df_clean

        quality_rows.append({
            "dataset": name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "nulls_handled": issues["nulls_handled"],
            "duplicates_removed": issues["duplicates_removed"],
            "encoding_fixed": issues["encoding_fixed"]
        })

        print(f"{name}:")
        print(f"  Rows in: {rows_in}")
        print(f"  Rows out: {rows_out}")
        print(f"  Issues: {issues}\n")

    print("\nUnifying Patients...\n")
    patients_df = unify_patients(cleaned_data)

    print("\nJoining All Datasets...\n")
    final_df = join_all(cleaned_data, patients_df)

    print("Final rows:", len(final_df))
    print("Final columns:", len(final_df.columns))

    print("\nFiltering Genomics...\n")
    filtered_genomics = filter_genomics(cleaned_data)

    if filtered_genomics is not None:
        print("Filtered genomics rows:", len(filtered_genomics))

    print("\nSaving Refined Outputs...\n")
    save_to_parquet(patients_df, "patients")
    save_to_parquet(final_df, "final_dataset")

    if filtered_genomics is not None:
        save_to_parquet(filtered_genomics, "genomics_filtered")

    save_partitioned_lab_results(cleaned_data)

    print("\nRunning Task 3 Analytics...\n")
    run_task_3(patients_df, cleaned_data, filtered_genomics)

    print("\nSaving Data Quality Metrics...\n")
    quality_df = pd.DataFrame(quality_rows)
    quality_path = os.path.join(
        os.getcwd(),
        "datalake",
        "consumption",
        "data_quality_metrics.parquet"
    )
    quality_df.to_parquet(quality_path, index=False)
    print(f"Saved: {quality_path}")

    print("\nRunning Task 4 Visualizations...\n")
    run_task_4()

    print("\nGenerating Manifests...\n")
    generate_all_manifests()

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()