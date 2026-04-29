import os
import pandas as pd
import matplotlib.pyplot as plt


PLOTS_PATH = os.path.join(os.getcwd(), "datalake", "consumption", "plots")


def save_plot(file_name):
    os.makedirs(PLOTS_PATH, exist_ok=True)
    path = os.path.join(PLOTS_PATH, file_name)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"Saved plot: {path}")


def plot_patient_demographics():
    patients = pd.read_parquet("datalake/refined/patients.parquet")

    patients["birth_date"] = pd.to_datetime(patients["birth_date"], errors="coerce")
    patients["age"] = 2026 - patients["birth_date"].dt.year

    plt.figure()
    patients["age"].dropna().plot(kind="hist", bins=20)
    plt.title("Patient Age Distribution")
    plt.xlabel("Age")
    plt.ylabel("Patient Count")
    save_plot("age_distribution.png")

    plt.figure()
    patients["gender"].value_counts().plot(kind="bar")
    plt.title("Gender Split")
    plt.xlabel("Gender")
    plt.ylabel("Patient Count")
    save_plot("gender_split.png")


def plot_diagnosis_frequency():
    df = pd.read_parquet("datalake/consumption/diagnosis_frequency.parquet")

    plt.figure(figsize=(10, 6))
    plt.barh(df["chapter"], df["patient_count"])
    plt.title("Top 15 ICD-10 Chapters by Patient Count")
    plt.xlabel("Patient Count")
    plt.ylabel("ICD-10 Chapter")
    plt.gca().invert_yaxis()
    save_plot("diagnosis_frequency.png")


def plot_lab_distributions():
    labs = pd.read_parquet("datalake/refined/lab_results")

    labs["test_name"] = labs["test_name"].astype(str).str.lower().str.strip()
    labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")

    reference_ranges = {
        "hba1c": {"normal_low": 4.0, "normal_high": 5.6},
        "creatinine": {"normal_low": 0.6, "normal_high": 1.2}
    }

    for test_name, ranges in reference_ranges.items():
        test_df = labs[labs["test_name"] == test_name]

        plt.figure()
        test_df["test_value"].dropna().plot(kind="hist", bins=20)

        plt.axvline(ranges["normal_low"], linestyle="--", label="Normal Low")
        plt.axvline(ranges["normal_high"], linestyle="--", label="Normal High")

        plt.title(f"{test_name.upper()} Distribution with Reference Range")
        plt.xlabel("Test Value")
        plt.ylabel("Count")
        plt.legend()

        save_plot(f"{test_name}_distribution.png")


def plot_genomics_scatter():
    df = pd.read_parquet("datalake/refined/genomics_filtered.parquet")

    df["allele_frequency"] = pd.to_numeric(df["allele_frequency"], errors="coerce")
    df["read_depth"] = pd.to_numeric(df["read_depth"], errors="coerce")

    plt.figure()

    for label, group in df.groupby("clinical_significance"):
        plt.scatter(
            group["allele_frequency"],
            group["read_depth"],
            label=label
        )

    plt.title("Allele Frequency vs Read Depth")
    plt.xlabel("Allele Frequency")
    plt.ylabel("Read Depth")
    plt.legend()

    save_plot("genomics_scatter.png")


def plot_high_risk_summary():
    high_risk = pd.read_parquet("datalake/consumption/high_risk_patients.parquet")
    patients = pd.read_parquet("datalake/refined/patients.parquet")

    high_risk_count = len(high_risk)
    other_count = len(patients) - high_risk_count

    plt.figure()
    plt.bar(["High Risk", "Other Patients"], [high_risk_count, other_count])
    plt.title("High-Risk Patient Cohort Summary")
    plt.xlabel("Patient Group")
    plt.ylabel("Patient Count")

    save_plot("high_risk_summary.png")


def plot_data_quality():
    df = pd.read_parquet("datalake/consumption/data_quality_metrics.parquet")

    x = range(len(df))

    plt.figure(figsize=(10, 6))
    plt.bar(x, df["nulls_handled"], label="Nulls Handled")
    plt.bar(
        x,
        df["duplicates_removed"],
        bottom=df["nulls_handled"],
        label="Duplicates Removed"
    )

    plt.xticks(x, df["dataset"], rotation=45, ha="right")
    plt.title("Data Quality Overview")
    plt.xlabel("Dataset")
    plt.ylabel("Issue Count")
    plt.legend()

    save_plot("data_quality_overview.png")


def create_plots_readme():
    os.makedirs(PLOTS_PATH, exist_ok=True)

    readme_path = os.path.join(PLOTS_PATH, "plots_README.md")

    content = """# Plots README

## age_distribution.png
Shows the age distribution of patients using cleaned patient data.

## gender_split.png
Shows patient count by gender.

## diagnosis_frequency.png
Shows the top 15 ICD-10 chapters by unique patient count.

## hba1c_distribution.png
Shows HbA1c lab value distribution with normal reference range boundaries.

## creatinine_distribution.png
Shows creatinine lab value distribution with normal reference range boundaries.

## genomics_scatter.png
Shows allele frequency vs read depth, grouped by clinical significance.

## high_risk_summary.png
Summarizes patients flagged as high risk based on HbA1c > 7.0 and pathogenic/likely pathogenic variants.

## data_quality_overview.png
Summarizes pipeline quality metrics such as nulls handled and duplicates removed.
"""

    with open(readme_path, "w") as file:
        file.write(content)

    print(f"Saved: {readme_path}")


def run_task_4():
    plot_patient_demographics()
    plot_diagnosis_frequency()
    plot_lab_distributions()
    plot_genomics_scatter()
    plot_high_risk_summary()
    plot_data_quality()
    create_plots_readme()