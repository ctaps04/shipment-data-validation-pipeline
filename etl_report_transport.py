import pandas as pd
from datetime import datetime
import sys
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, filename="etl_report_transport.log", filemode="w",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

OVERWEIGHT_USERS = {
    "overweight_ops_1@company.com",
    "overweight_ops_2@company.com",
    "overweight_ops_3@company.com",
    "overweight_ops_4@company.com"
}

def load_data(path):
    return pd.read_excel(path)

def validate_primary_reference(df):
    if not df["Primary Reference"].is_unique:
        raise ValueError("Primary Reference must be unique")

def validate_status(df):
    valid_status = {"Booked", "In Transit"}

    if not df["Status"].isin(valid_status).all():
        raise ValueError("Status contains invalid values")

def validate_overweight(df, overweight_users):
    mask = (
        (df["Weight"] >= 49000) &
        (~df["Create By"].astype(str).str.lower().isin(overweight_users))
    )

    if mask.any():
        return df[mask]

    return None

def clean_and_validate_weight(df):
    df["Weight_raw"] = df["Weight"]
    df["Weight"] = df["Weight"].astype(str).str.replace(",", "")
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")

    if df["Weight"].isnull().any():
        bad_weights = df[df["Weight"].isnull()]
        print(bad_weights[["Primary Reference", "Weight"]].head(20))
        raise ValueError("Weight has non-numeric values")

    if (df["Weight"] <= 0).any():
        raise ValueError("Weight has zero or negative values")

    invalid = validate_overweight(df, OVERWEIGHT_USERS)

    if invalid is not None:
        print("Invalid overweight shipments:", invalid[["Primary Reference", "Weight", "Create By"]].head(20))
        raise ValueError("Weight has unrealistically high values")

def validate_create_date(df):
    df["Create Date"] = pd.to_datetime(df["Create Date"], errors="coerce")

    if df["Create Date"].isnull().any():
        raise ValueError("Create Date has null values")

    if (df["Create Date"] > datetime.now()).any():
        raise ValueError("Create Date contains future dates")

    if (df["Create Date"] < datetime(2020, 1, 1)).any():
        raise ValueError("Create Date contains unrealistic old dates")

def clean_and_validate_states(df):
    df["Origin State"] = df["Origin State"].astype(str).str.strip().str.upper()
    df["Dest State"] = df["Dest State"].astype(str).str.strip().str.upper()

    if df["Origin State"].isnull().any():
        raise ValueError("Origin State has null values")

    if df["Dest State"].isnull().any():
        raise ValueError("Dest State has null values")

    us_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    }

    canadian_provinces = {
        'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'ON', 'PE', 'QC', 'SK',
        'NT', 'NU', 'YT'
    }

    valid_states = us_states | canadian_provinces

    if not df["Origin State"].isin(valid_states).all():
        raise ValueError("Origin State contains invalid values")

    if not df["Dest State"].isin(valid_states).all():
        raise ValueError("Dest State contains invalid values")

    if not (df["Origin State"].str.len() == 2).all():
        raise ValueError("Origin State has invalid length")

    if not (df["Dest State"].str.len() == 2).all():
        raise ValueError("Dest State has invalid length")

    df["origin_in_canada"] = df["Origin State"].isin(canadian_provinces)
    df["dest_in_canada"] = df["Dest State"].isin(canadian_provinces)

def parse_ship_and_delivery_ranges(df):
    ship_split = df["Target Ship (Range)"].str.split("-", expand=True)

    if ship_split.shape[1] != 2:
        raise ValueError("Target Ship range malformed")

    df["Ship Start"] = pd.to_datetime(ship_split[0], errors="coerce")
    df["Ship End"] = pd.to_datetime(ship_split[1], errors="coerce")

    delivery_split = df["Target Delivery (Range)"].str.split("-", expand=True)

    if delivery_split.shape[1] != 2:
        raise ValueError("Target Delivery range malformed")

    df["Delivery Start"] = pd.to_datetime(delivery_split[0], errors="coerce")
    df["Delivery End"] = pd.to_datetime(delivery_split[1], errors="coerce")

    if(df["Ship Start"] > df["Ship End"]).any():
        raise ValueError("Ship Start after Ship End")

    if(df["Delivery Start"] > df["Delivery End"]).any():
        raise ValueError("Delivery Start after Delivery End")

    df["delivery_date_valid"] = df["Delivery Start"] >= df["Ship Start"]
    df["delivery_before_shipping"] = df["Delivery Start"] < df["Ship Start"]

def clean_text_columns(df):
    text_cols = [
        "Origin Name", "Dest Name",
        "Origin City", "Dest City",
    ]

    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.upper()

def main(path):
    try:
        logging.info("Starting pipeline ETL process.")
        df = load_data(path)

        validate_primary_reference(df)
        logging.info("Primary Reference validated.")

        validate_status(df)
        logging.info("Status validated.")

        clean_and_validate_weight(df)
        logging.info("Weight cleaned and validated.")

        validate_create_date(df)
        logging.info("Create Date validated.")

        clean_and_validate_states(df)
        logging.info("States cleaned and validated.")

        parse_ship_and_delivery_ranges(df)
        logging.info("Ship and Delivery ranges parsed.")

        clean_text_columns(df)
        logging.info("Text columns cleaned.")

        logging.info("Pipeline finished successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    path = sys.argv[1]
    main(path)