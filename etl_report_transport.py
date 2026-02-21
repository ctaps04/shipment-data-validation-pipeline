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

CRITICAL_ERRORS = {
    "null_weight",
    "null_date",
    "future_date",
    "duplicate_primary_reference",
    "null_primary_reference",
    "empty_primary_reference"
}

def load_data(path):
    return pd.read_excel(path)

def build_error(df, mask):
    excel_indices = (df[mask].index + 2).tolist()
    return {
        "count": int(mask.sum()),
        "indices": excel_indices
    }
    
def clean_primary_reference(df):
    df["Primary Reference"] = df["Primary Reference"].str.strip()

    return df

def validate_primary_reference(df):
    errors = {}

    null_mask = df["Primary Reference"].isnull()
    if null_mask.any():
        errors["null_primary_reference"] = build_error(df, null_mask)

    empty_mask = df["Primary Reference"] == ""
    if empty_mask.any():
        errors["empty_primary_reference"] = build_error(df, empty_mask)

    duplicate_mask = df["Primary Reference"].duplicated(keep=False)
    if duplicate_mask.any():
        errors["duplicate_primary_reference"] = build_error(df, duplicate_mask)

    return errors

def validate_status(df):
    errors = {}
    valid_status = {"Booked", "In Transit"}
    invalid_mask = ~df["Status"].isin(valid_status)

    if invalid_mask.any():
        errors["invalid_status"] = build_error(df, invalid_mask)

    return errors

def validate_overweight(df, overweight_users):
    mask = (
        (df["Weight"] >= 49000) &
        (~df["Create By"].astype(str).str.strip().str.lower().isin(overweight_users))
    )

    if mask.any():
        return build_error(df, mask)

    return None

def clean_weight(df):
    df["Weight_raw"] = df["Weight"]
    df["Weight"] = df["Weight"].astype(str).str.replace(",", "")
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")

    return df

def validate_weight(df):
    errors = {}

    null_mask = df["Weight"].isnull()
    if null_mask.any():
        errors["null_weight"] = build_error(df, null_mask)

    invalid_format_mask = (
        df["Weight_raw"].notnull() &
        df["Weight"].isnull()
    )

    if invalid_format_mask.any():
        errors["invalid_weight_format"] = build_error(df, invalid_format_mask)
    
    non_positive_mask = df["Weight"] <= 0
    if non_positive_mask.any():
        errors["non_positive"] = build_error(df, non_positive_mask)

    overweight_error = validate_overweight(df, OVERWEIGHT_USERS)
    if overweight_error:
        errors["overweight"] = overweight_error
    
    return errors

def clean_create_date(df):
    df["Create Date"] = pd.to_datetime(df["Create Date"], errors="coerce")
    return df

def validate_create_date(df):
    errors = {}

    null_mask = df["Create Date"].isnull()
    if null_mask.any():
        errors["null_date"] = build_error(df, null_mask)

    future_date_mask = df["Create Date"] > datetime.now()
    if future_date_mask.any():
        errors["future_date"] = build_error(df, future_date_mask)

    old_date_mask = df["Create Date"] < datetime(2020, 1, 1)
    if old_date_mask.any():
        errors["too_old"] = build_error(df, old_date_mask)

    return errors

def clean_states(df):
    df["Origin State"] = df["Origin State"].str.strip().str.upper()
    df["Dest State"] = df["Dest State"].str.strip().str.upper()

    return df

def validate_states(df):
    errors = {}

    null_origin_mask = df["Origin State"].isnull()
    null_dest_mask = df["Dest State"].isnull()
    if null_origin_mask.any():
        errors["origin_null"] = build_error(df, null_origin_mask)
    if null_dest_mask.any():
        errors["dest_null"] = build_error(df, null_dest_mask)

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

    invalid_origin_mask = ~df["Origin State"].isin(valid_states)
    invalid_dest_mask = ~df["Dest State"].isin(valid_states)
    if invalid_origin_mask.any():
        errors["origin_invalid"] = build_error(df, invalid_origin_mask)
    if invalid_dest_mask.any():
        errors["dest_invalid"] = build_error(df, invalid_dest_mask)

    bad_origin_length = df["Origin State"].str.len() != 2
    bad_dest_length = df["Dest State"].str.len() != 2
    if bad_origin_length.any():
        errors["origin_length"] = build_error(df, bad_origin_length)
    if bad_dest_length.any():
        errors["dest_length"] = build_error(df, bad_dest_length)

    return errors

def clean_ship_and_delivery_ranges(df):
    ship_split = df["Target Ship (Range)"].str.split("-", expand=True)
    df["Ship Start"] = pd.to_datetime(ship_split[0], errors="coerce")
    df["Ship End"] = pd.to_datetime(ship_split[1], errors="coerce")

    delivery_split = df["Target Delivery (Range)"].str.split("-", expand=True)
    df["Delivery Start"] = pd.to_datetime(delivery_split[0], errors="coerce")
    df["Delivery End"] = pd.to_datetime(delivery_split[1], errors="coerce")

    return df

def validate_ship_and_delivery_ranges(df):
    errors = {}
    
    null_ship_mask = df[["Ship Start", "Ship End"]].isnull().any(axis=1)
    null_delivery_mask = df[["Delivery Start", "Delivery End"]].isnull().any(axis=1)
    if null_ship_mask.any():
        errors["null_ship_dates"] = build_error(df, null_ship_mask)
    if null_delivery_mask.any():
        errors["null_delivery_dates"] = build_error(df, null_delivery_mask)
    
    ship_order_invalid = df["Ship Start"] > df["Ship End"]
    delivery_order_invalid = df["Delivery Start"] > df["Delivery End"]
    if ship_order_invalid.any():
        errors["ship_start_after_end"] = build_error(df, ship_order_invalid)
    if delivery_order_invalid.any():
        errors["delivery_start_after_end"] = build_error(df, delivery_order_invalid)

    delivery_before_shipping = df["Delivery Start"] < df["Ship Start"]
    if delivery_before_shipping.any():
        errors["delivery_before_shipping"] = build_error(df, delivery_before_shipping)

    return errors

def clean_text_columns(df):
    for col in ["Origin Name", "Dest Name", "Origin City", "Dest City"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df

def enforce_policy(all_errors):
    critical_found = []

    for domain, errors in all_errors.items():
        for error_type, error_data in errors.items():
            count = error_data["count"]
            indices = error_data["indices"]
            logger.error(f"[{domain}] {error_type} - rows affected: {count}, indices: {indices}")

            if error_type in CRITICAL_ERRORS:
                critical_found.append((domain, error_type, count))
            
    if critical_found:
        summary = ", ".join(
            [f"{d}.{e} ({n} rows)" for d, e, n in critical_found]
        )
        raise ValueError(f"Pipeline failed due to critical errors: {summary}")

def main(path):
    try:
        logging.info("Starting pipeline ETL process.")
        df = load_data(path)

        df = clean_primary_reference(df)
        reference_errors = validate_primary_reference(df)

        df = clean_weight(df)
        weight_errors = validate_weight(df)

        df = clean_create_date(df)
        date_errors = validate_create_date(df)

        status_errors = validate_status(df)

        df = clean_states(df)
        state_errors = validate_states(df)

        df = clean_ship_and_delivery_ranges(df)
        range_errors = validate_ship_and_delivery_ranges(df)

        df = clean_text_columns(df)

        all_errors = {
            "primary_reference": reference_errors,
            "weight": weight_errors,
            "create_date": date_errors,
            "status": status_errors,
            "states": state_errors,
            "ranges": range_errors,
        }

        enforce_policy(all_errors)

        logging.info("Pipeline finished successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    path = sys.argv[1]
    main(path)