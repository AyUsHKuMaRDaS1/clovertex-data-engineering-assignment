"""Dataset joining logic that links cleaned source tables by patient ID."""


def join_all(cleaned_data, patients_df):
    """Merge cleaned source datasets onto the unified patient table."""
    final_df = patients_df.copy()

    # Lab results
    if "site_gamma_lab_results" in cleaned_data:
        lab_df = cleaned_data["site_gamma_lab_results"].copy()

        if "patient_ref" in lab_df.columns:
            lab_df = lab_df.rename(columns={"patient_ref": "patient_id"})

        final_df = final_df.merge(
            lab_df,
            on="patient_id",
            how="left",
            suffixes=("", "_lab")
        )

    # Genomics variants
    if "genomics_variants" in cleaned_data:
        geno_df = cleaned_data["genomics_variants"].copy()

        if "patient_ref" in geno_df.columns:
            geno_df = geno_df.rename(columns={"patient_ref": "patient_id"})

        final_df = final_df.merge(
            geno_df,
            on="patient_id",
            how="left",
            suffixes=("", "_geno")
        )

    # Diagnoses
    if "diagnoses_icd10" in cleaned_data:
        diag_df = cleaned_data["diagnoses_icd10"].copy()

        final_df = final_df.merge(
            diag_df,
            on="patient_id",
            how="left",
            suffixes=("", "_diag")
        )

    # Medications
    if "medications_log" in cleaned_data:
        med_df = cleaned_data["medications_log"].copy()

        final_df = final_df.merge(
            med_df,
            on="patient_id",
            how="left",
            suffixes=("", "_med")
        )

    return final_df