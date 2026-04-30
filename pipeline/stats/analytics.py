"""Analytics helpers for generating consumption outputs and anomaly reports."""

import os
import json
import pandas as pd


CONSUMPTION_PATH = os.path.join(os.getcwd(), "datalake", "consumption")


def save_consumption(df, file_name):
    """Persist a dataframe to the consumption layer as parquet."""
    os.makedirs(CONSUMPTION_PATH, exist_ok=True)
    path = os.path.join(CONSUMPTION_PATH, file_name)
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")


def patient_summary(patients_df):
    """Create summary metrics for patient demographics and persist them."""
    df = patients_df.copy()

    df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")
    df["age"] = 2026 - df["birth_date"].dt.year

    age_summary = pd.DataFrame({
        "summary_type": ["age_mean", "age_min", "age_max"],
        "category": ["age", "age", "age"],
        "value": [df["age"].mean(), df["age"].min(), df["age"].max()]
    })

    gender_summary = df["gender"].value_counts().reset_index()
    gender_summary.columns = ["category", "value"]
    gender_summary["summary_type"] = "gender_split"

    site_summary = df["site"].value_counts().reset_index()
    site_summary.columns = ["category", "value"]
    site_summary["summary_type"] = "site_distribution"

    result = pd.concat(
        [age_summary, gender_summary, site_summary],
        ignore_index=True,
        sort=False
    )

    save_consumption(result, "patient_summary.parquet")
    return result


def lab_statistics(cleaned_data):
    """Compute lab test statistics and identify readings outside reference ranges."""
    labs = cleaned_data["site_gamma_lab_results"].copy()

    if "patient_ref" in labs.columns:
        labs = labs.rename(columns={"patient_ref": "patient_id"})

    labs["test_name"] = labs["test_name"].astype(str).str.lower().str.strip()
    labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")
    labs["collection_date"] = pd.to_datetime(labs["collection_date"], errors="coerce")

    ref_path = os.path.join(os.getcwd(), "data", "reference", "lab_test_ranges.json")

    with open(ref_path, "r") as file:
        ranges = json.load(file)

    def outside_range(row):
        test = row["test_name"]
        value = row["test_value"]

        if test not in ranges or pd.isna(value):
            return False

        return (
            value < ranges[test]["normal_low"]
            or value > ranges[test]["normal_high"]
        )

    labs["outside_reference_range"] = labs.apply(outside_range, axis=1)

    stats = labs.groupby("test_name")["test_value"].agg(
        mean="mean",
        median="median",
        std="std"
    ).reset_index()

    abnormal = labs.groupby("test_name")["outside_reference_range"].sum().reset_index()
    abnormal = abnormal.rename(columns={"outside_reference_range": "abnormal_count"})

    stats = stats.merge(abnormal, on="test_name", how="left")
    stats["metric_type"] = "lab_statistics"

    trend_rows = []

    for test in ["hba1c", "creatinine"]:
        test_df = labs[labs["test_name"] == test].copy()
        test_df = test_df.sort_values(["patient_id", "collection_date"])

        for patient_id, group in test_df.groupby("patient_id"):
            if len(group) < 2:
                continue

            first_value = group.iloc[0]["test_value"]
            last_value = group.iloc[-1]["test_value"]

            if last_value > first_value:
                trend = "worsening"
            elif last_value < first_value:
                trend = "improving"
            else:
                trend = "stable"

            trend_rows.append({
                "metric_type": "patient_trend",
                "test_name": test,
                "patient_id": patient_id,
                "first_value": first_value,
                "last_value": last_value,
                "trend": trend
            })

    trend_df = pd.DataFrame(trend_rows)

    result = pd.concat([stats, trend_df], ignore_index=True, sort=False)

    save_consumption(result, "lab_statistics.parquet")
    return result


def diagnosis_frequency(cleaned_data):
    """Summarize diagnosis counts by ICD-10 chapter and persist the results."""
    diagnosis = cleaned_data["diagnoses_icd10"].copy()

    ref_path = os.path.join(os.getcwd(), "data", "reference", "icd10_chapters.csv")
    icd_ref = pd.read_csv(ref_path)

    diagnosis["icd10_code"] = diagnosis["icd10_code"].astype(str)

    # flexible chapter mapping
    if "code_prefix" in icd_ref.columns:
        icd_ref["code_prefix"] = icd_ref["code_prefix"].astype(str)

        def map_chapter(code):
            for _, row in icd_ref.iterrows():
                if code.startswith(row["code_prefix"]):
                    return row.get("chapter", "unknown")
            return "unknown"

        diagnosis["chapter"] = diagnosis["icd10_code"].apply(map_chapter)

    elif "icd10_code" in icd_ref.columns:
        diagnosis = diagnosis.merge(icd_ref, on="icd10_code", how="left")

        if "chapter" not in diagnosis.columns:
            diagnosis["chapter"] = "unknown"

    else:
        diagnosis["chapter"] = "unknown"

    result = (
        diagnosis.groupby("chapter")["patient_id"]
        .nunique()
        .reset_index(name="patient_count")
        .sort_values("patient_count", ascending=False)
        .head(15)
    )

    save_consumption(result, "diagnosis_frequency.parquet")
    return result


def variant_hotspots(filtered_genomics):
    """Identify top pathogenic genes and save variant hotspot summaries."""
    df = filtered_genomics.copy()

    df["clinical_significance"] = (
        df["clinical_significance"]
        .astype(str)
        .str.strip()
        .str.title()
    )

    df["allele_frequency"] = pd.to_numeric(
        df["allele_frequency"],
        errors="coerce"
    )

    pathogenic = df[
        df["clinical_significance"].isin(
            ["Pathogenic", "Likely Pathogenic"]
        )
    ]

    top_genes = (
        pathogenic["gene"]
        .value_counts()
        .head(5)
        .index
    )

    result = (
        pathogenic[pathogenic["gene"].isin(top_genes)]
        .groupby("gene")
        .agg(
            variant_count=("gene", "count"),
            mean_allele_frequency=("allele_frequency", "mean"),
            allele_frequency_25th=("allele_frequency", lambda x: x.quantile(0.25)),
            allele_frequency_75th=("allele_frequency", lambda x: x.quantile(0.75))
        )
        .reset_index()
        .sort_values("variant_count", ascending=False)
    )

    save_consumption(result, "variant_hotspots.parquet")
    return result


def high_risk_patients(cleaned_data, filtered_genomics):
    """Find patients with both elevated lab values and pathogenic genomics variants."""
    labs = cleaned_data["site_gamma_lab_results"].copy()

    if "patient_ref" in labs.columns:
        labs = labs.rename(columns={"patient_ref": "patient_id"})

    labs["test_name"] = labs["test_name"].astype(str).str.lower().str.strip()
    labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")

    high_hba1c = labs[
        (labs["test_name"] == "hba1c")
        & (labs["test_value"] > 7.0)
    ]["patient_id"].dropna().unique()

    genomics = filtered_genomics.copy()

    genomics["clinical_significance"] = (
        genomics["clinical_significance"]
        .astype(str)
        .str.strip()
        .str.title()
    )

    pathogenic_patients = genomics[
        genomics["clinical_significance"].isin(
            ["Pathogenic", "Likely Pathogenic"]
        )
    ]["patient_id"].dropna().unique()

    high_risk_ids = sorted(set(high_hba1c).intersection(set(pathogenic_patients)))

    result = pd.DataFrame({
        "patient_id": high_risk_ids,
        "risk_reason": "HbA1c above 7.0 and pathogenic/likely pathogenic genomics variant"
    })

    save_consumption(result, "high_risk_patients.parquet")
    return result


def anomaly_flags(patients_df, cleaned_data, filtered_genomics):
    """Flag demographic, laboratory, medication, and genomics anomalies."""
    anomalies = []

    patients = patients_df.copy()
    patients["birth_date"] = pd.to_datetime(patients["birth_date"], errors="coerce")
    patients["age"] = 2026 - patients["birth_date"].dt.year

    for _, row in patients.iterrows():
        if pd.isna(row["age"]) or row["age"] < 0 or row["age"] > 120:
            anomalies.append({
                "patient_id": row["patient_id"],
                "anomaly_type": "invalid_age",
                "reason": "Age is missing, negative, or greater than 120"
            })

    labs = cleaned_data["site_gamma_lab_results"].copy()

    if "patient_ref" in labs.columns:
        labs = labs.rename(columns={"patient_ref": "patient_id"})

    labs["test_name"] = labs["test_name"].astype(str).str.lower().str.strip()
    labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")

    ref_path = os.path.join(os.getcwd(), "data", "reference", "lab_test_ranges.json")

    with open(ref_path, "r") as file:
        ranges = json.load(file)

    for _, row in labs.iterrows():
        test = row["test_name"]
        value = row["test_value"]

        if test in ranges and not pd.isna(value):
            if (
                value < ranges[test]["critical_low"]
                or value > ranges[test]["critical_high"]
            ):
                anomalies.append({
                    "patient_id": row["patient_id"],
                    "anomaly_type": "critical_lab_value",
                    "reason": f"{test} value {value} outside critical limits"
                })

    meds = cleaned_data["medications_log"].copy()

    meds["start_date"] = pd.to_datetime(meds["start_date"], errors="coerce")
    meds["end_date"] = pd.to_datetime(meds["end_date"], errors="coerce")

    invalid_meds = meds[
        meds["end_date"].notna()
        & meds["start_date"].notna()
        & (meds["end_date"] < meds["start_date"])
    ]

    for _, row in invalid_meds.iterrows():
        anomalies.append({
            "patient_id": row["patient_id"],
            "anomaly_type": "invalid_medication_dates",
            "reason": "Medication end_date is before start_date"
        })

    genomics = filtered_genomics.copy()
    genomics["allele_frequency"] = pd.to_numeric(
        genomics["allele_frequency"],
        errors="coerce"
    )
    genomics["read_depth"] = pd.to_numeric(
        genomics["read_depth"],
        errors="coerce"
    )

    invalid_genomics = genomics[
        (genomics["allele_frequency"] < 0)
        | (genomics["allele_frequency"] > 1)
        | (genomics["read_depth"] <= 0)
    ]

    for _, row in invalid_genomics.iterrows():
        anomalies.append({
            "patient_id": row["patient_id"],
            "anomaly_type": "genomics_inconsistency",
            "reason": "Invalid allele frequency or read depth"
        })

    result = pd.DataFrame(anomalies)

    save_consumption(result, "anomaly_flags.parquet")
    return result


def run_task_3(patients_df, cleaned_data, filtered_genomics):
    """Execute all task 3 analytics functions sequentially."""
    patient_summary(patients_df)
    lab_statistics(cleaned_data)
    diagnosis_frequency(cleaned_data)
    variant_hotspots(filtered_genomics)
    high_risk_patients(cleaned_data, filtered_genomics)
    anomaly_flags(patients_df, cleaned_data, filtered_genomics)