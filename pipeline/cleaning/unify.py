import pandas as pd


def unify_patients(data):
    patient_tables = []

    for name, df in data.items():
        if "patients" not in name:
            continue

        df = df.copy()

        # alpha columns
        if "sex" in df.columns:
            df = df.rename(columns={"sex": "gender"})

        if "date_of_birth" in df.columns:
            df = df.rename(columns={"date_of_birth": "birth_date"})

        # beta columns
        if "patientid" in df.columns:
            df = df.rename(columns={"patientid": "patient_id"})

        if "birthdate" in df.columns:
            df = df.rename(columns={"birthdate": "birth_date"})

        # beta nested name columns after flattening
        if "name_given" in df.columns:
            df = df.rename(columns={"name_given": "first_name"})

        if "name_family" in df.columns:
            df = df.rename(columns={"name_family": "last_name"})

        # add site
        if "alpha" in name:
            df["site"] = "alpha"
        elif "beta" in name:
            df["site"] = "beta"
        else:
            df["site"] = "unknown"

        required_cols = [
            "patient_id",
            "first_name",
            "last_name",
            "gender",
            "birth_date",
            "site"
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = "unknown"

        df = df[required_cols]

        patient_tables.append(df)

    patients = pd.concat(patient_tables, ignore_index=True)

    patients = patients.drop_duplicates(subset=["patient_id"])

    return patients