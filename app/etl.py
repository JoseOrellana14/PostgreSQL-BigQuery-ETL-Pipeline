from app.organizations import extract_organizations, trasnform_organizations, load_organizations
from app.common.utils import get_load_date, get_last_loaded_timestamp

def run_etl():
    """Run the full ETL process."""
    load_date = get_load_date()
    print(f"[DEBUG] load_date generated (Bolivian Zone): {load_date}")

    """Defining the ETL"""
    DATASET = "prod_dw"

    # =================
    # Organizations ETL
    # =================
    last_loaded_organizations = get_last_loaded_timestamp("organizations", dataset=DATASET)
    print(f"Last loaded organizations: {last_loaded_organizations}")

    print("Extracting organizations data...")
    organizations = extract_organizations(last_loaded_organizations)
    print(f"Extracted organizations data.")

    print("Transforming organizations data...")
    organizations_df = trasnform_organizations(organizations, load_date)
    print(f"Transformed organizations data.")

    print("Loading organizations data into BigQuery...")
    load_organizations(organizations_df)
    print("Organizations data loaded.")



if __name__ == "__main__":
    run_etl()