from app.organizations import extract_organizations, transform_organizations, load_organizations
from app.users import extract_users, transform_users
from app.buyer_leads import extract_buyer_leads, transform_buyer_leads
from app.seller_leads import extract_seller_leads, transform_seller_leads
from app.property_units import extract_property_units, transform_property_units
from app.property_opportunities import extract_property_opportunities, transform_property_opportunities
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
    organizations_df = transform_organizations(organizations, load_date)
    print(f"Transformed organizations data.")

    print("Loading organizations data into BigQuery...")
    load_organizations(organizations_df)
    print("Organizations data loaded.")

    # =================
    # Users ETL
    # =================
    last_loaded_users = get_last_loaded_timestamp("users", dataset=DATASET)
    print(f"Last loaded users: {last_loaded_users}")

    print("Extracting users data...")
    users = extract_users(last_loaded_users)
    print(f"Extracted users data.")

    print("Transforming users data...")
    users_df = transform_users(users, load_date)
    print(f"Transformed users data.")

    print("Loading users data into BigQuery...")
    # load_organizations(users_df)
    print("Users data loaded.")

    # =================
    # Buyer Leads ETL
    # =================
    last_loaded_buyer_leads = get_last_loaded_timestamp("buyer_leads", dataset=DATASET)
    print(f"Last loaded buyer leads: {last_loaded_buyer_leads}")

    print("Extracting buyer leads data...")
    buyer_leads = extract_buyer_leads(last_loaded_buyer_leads)
    print(f"Extracted buyer leads data.")

    print("Transforming buyer leads data...")
    buyer_leads_df = transform_buyer_leads(buyer_leads, load_date)
    print(f"Transformed buyer leads data.")

    print("Loading buyer leads data into BigQuery...")
    # load_organizations(buyer_leads_df)
    print("buyer leads data loaded.")

    # =================
    # Seller Leads ETL
    # =================
    last_loaded_seller_leads = get_last_loaded_timestamp("seller_leads", dataset=DATASET)
    print(f"Last loaded seller leads: {last_loaded_seller_leads}")

    print("Extracting seller leads data...")
    seller_leads = extract_seller_leads(last_loaded_seller_leads)
    print(f"Extracted seller leads data.")

    print("Transforming seller leads data...")
    seller_leads_df = transform_seller_leads(seller_leads, load_date)
    print(f"Transformed seller leads data.")

    print("Loading seller leads data into BigQuery...")
    # load_organizations(seller_leads_df)
    print("seller leads data loaded.")

    # =================
    # Property Units ETL
    # =================
    last_loaded_property_units = get_last_loaded_timestamp("property_units", dataset=DATASET)
    print(f"Last loaded property units: {last_loaded_property_units}")

    print("Extracting property units data...")
    property_units = extract_property_units(last_loaded_property_units)
    print(f"Extracted property units data.")

    print("Transforming property units data...")
    property_units_df = transform_property_units(property_units, load_date)
    print(f"Transformed property units data.")

    print("Loading property units data into BigQuery...")
    # load_organizations(property_units_df)
    print("property units data loaded.")

    # =================
    # Property Opportunities ETL
    # =================
    last_loaded_property_opportunities = get_last_loaded_timestamp("property_opportunities", dataset=DATASET)
    print(f"Last loaded property opportunities: {last_loaded_property_opportunities}")

    print("Extracting property opportunities data...")
    property_opportunities = extract_property_opportunities(last_loaded_property_opportunities)
    print(f"Extracted property opportunities data.")

    print("Transforming property opportunities data...")
    property_opportunities_df = transform_property_opportunities(property_opportunities, load_date)
    print(f"Transformed property opportunities data.")

    print("Loading property opportunities data into BigQuery...")
    # load_organizations(property_opportunities_df)
    print("property opportunities data loaded.")


if __name__ == "__main__":
    run_etl()