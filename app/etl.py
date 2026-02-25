from app.organizations import extract_organizations, transform_organizations, load_organizations, ORGANIZATION_SCHEMA_PATH
from app.users import extract_users, transform_users, USER_SCHEMA_PATH
from app.listings import extract_listings, transform_listings, LISTING_SCHEMA_PATH
from app.opportunities import extract_opportunities, transform_opportunities, OPPORTUNITY_SCHEMA_PATH
from app.chat_messages import extract_chat_messages, transform_chat_messages, CHAT_MESSAGE_SCHEMA_PATH
from app.appointments import extract_appointments, transform_appointments, APPOINTMENT_SCHEMA_PATH
from app.leads import extract_leads, transform_leads, LEAD_SCHEMA_PATH
from app.listing_projects import extract_listing_projects, transform_listing_projects, LISTING_PROJECT_SCHEMA_PATH
from app.common.utils import get_load_date, get_last_loaded_timestamp, load_dataframe_with_merge

def run_etl():
    """Run the full ETL process."""
    load_date = get_load_date()
    print(f"[DEBUG] load_date generated (Bolivian Zone): {load_date}")

    """Defining the ETL"""
    DATASET = "capital_solutions_real_state"

    # =================
    # Organizations ETL
    # =================
    last_loaded_organizations = get_last_loaded_timestamp("raw_organizations", dataset=DATASET)
    print(f"Last loaded organizations: {last_loaded_organizations}")

    print("Extracting organizations data...")
    organizations = extract_organizations(last_loaded_organizations)
    print(f"Extracted organizations data.")

    print("Transforming organizations data...")
    organizations_df = transform_organizations(organizations, load_date)
    print(f"Transformed organizations data.")

    print("Loading organizations data into BigQuery...")
    load_dataframe_with_merge(
    df=organizations_df,
    bq_table_env_var="BQ_ORGANIZATIONS_TABLE",
    schema_path=ORGANIZATION_SCHEMA_PATH,
    key_column="organization_id",
    updated_at_col="updated_at",
    )
    print("Organizations data loaded.")

    # =================
    # Users ETL
    # =================
    last_loaded_users = get_last_loaded_timestamp("raw_users", dataset=DATASET)
    print(f"Last loaded users: {last_loaded_users}")

    print("Extracting users data...")
    users = extract_users(last_loaded_users)
    print(f"Extracted users data.")

    print("Transforming users data...")
    users_df = transform_users(users, load_date)
    print(f"Transformed users data.")

    print("Loading users data into BigQuery...")
    load_dataframe_with_merge(
    df=users_df,
    bq_table_env_var="BQ_USERS_TABLE",
    schema_path=USER_SCHEMA_PATH,
    key_column="user_id",
    updated_at_col="updated_at",
    )
    print("Users data loaded.")

    # =================
    # Listings ETL
    # =================
    last_loaded_listings = get_last_loaded_timestamp("raw_listings", dataset=DATASET)
    print(f"Last loaded listings: {last_loaded_listings}")

    print("Extracting listings data...")
    listings = extract_listings(last_loaded_listings)
    print(f"Extracted listings data.")

    print("Transforming listings data...")
    listings_df = transform_listings(listings, load_date)
    print(f"Transformed listings data.")

    print("Loading listings data into BigQuery...")
    load_dataframe_with_merge(
    df=listings_df,
    bq_table_env_var="BQ_LISTINGS_TABLE",
    schema_path=LISTING_SCHEMA_PATH,
    key_column="listing_id",
    updated_at_col="updated_at",
    )
    print("listings data loaded.")

    # =================
    # Opportunities ETL
    # =================
    last_loaded_opportunities = get_last_loaded_timestamp("raw_opportunities", dataset=DATASET)
    print(f"Last loaded opportunities: {last_loaded_opportunities}")

    print("Extracting opportunities data...")
    opportunities = extract_opportunities(last_loaded_opportunities)
    print(f"Extracted opportunities data.")

    print("Transforming opportunities data...")
    opportunities_df = transform_opportunities(opportunities, load_date)
    print(f"Transformed opportunities data.")

    print("Loading opportunities data into BigQuery...")
    load_dataframe_with_merge(
    df=opportunities_df,
    bq_table_env_var="BQ_OPPORTUNITIES_TABLE",
    schema_path=OPPORTUNITY_SCHEMA_PATH,
    key_column="opportunity_id",
    updated_at_col="updated_at",
    )
    print("opportunities data loaded.")

    # =================
    # Chat messages ETL
    # =================
    last_loaded_chat_messages = get_last_loaded_timestamp("raw_chat_messages", dataset=DATASET)
    print(f"Last loaded chat messages: {last_loaded_chat_messages}")

    print("Extracting chat messages data...")
    chat_messages = extract_chat_messages(last_loaded_chat_messages)
    print(f"Extracted chat messages data.")

    print("Transforming chat messages data...")
    chat_messages_df = transform_chat_messages(chat_messages, load_date)
    print(f"Transformed chat messages data.")

    print("Loading chat messages data into BigQuery...")
    load_dataframe_with_merge(
    df=chat_messages_df,
    bq_table_env_var="BQ_CHAT_MESSAGES_TABLE",
    schema_path=CHAT_MESSAGE_SCHEMA_PATH,
    key_column="chat_message_id",
    updated_at_col="updated_at",
    )
    print("chat messages data loaded.")

    # =================
    # Appointments ETL
    # =================
    last_loaded_appointments = get_last_loaded_timestamp("raw_appointments", dataset=DATASET)
    print(f"Last loaded appointments: {last_loaded_appointments}")

    print("Extracting appointments data...")
    appointments = extract_appointments(last_loaded_appointments)
    print(f"Extracted appointments data.")

    print("Transforming appointments data...")
    appointments_df = transform_appointments(appointments, load_date)
    print(f"Transformed appointments data.")

    print("Loading appointments data into BigQuery...")
    load_dataframe_with_merge(
    df=appointments_df,
    bq_table_env_var="BQ_APPOINTMENTS_TABLE",
    schema_path=APPOINTMENT_SCHEMA_PATH,
    key_column="appointment_id",
    updated_at_col="updated_at",
    )
    print("appointments data loaded.")

    # =================
    # Leads ETL
    # =================
    last_loaded_leads = get_last_loaded_timestamp("raw_leads", dataset=DATASET)
    print(f"Last loaded leads: {last_loaded_leads}")

    print("Extracting leads data...")
    leads = extract_leads(last_loaded_leads)
    print(f"Extracted leads data.")

    print("Transforming leads data...")
    leads_df = transform_leads(leads, load_date)
    print(f"Transformed leads data.")

    print("Loading leads data into BigQuery...")
    load_dataframe_with_merge(
    df=leads_df,
    bq_table_env_var="BQ_LEADS_TABLE",
    schema_path=LEAD_SCHEMA_PATH,
    key_column="lead_id",
    updated_at_col="updated_at",
    )
    print("leads data loaded.")

    # =================
    # Listing Projects ETL
    # =================
    last_loaded_listing_projects = get_last_loaded_timestamp("raw_listing_projects", dataset=DATASET)
    print(f"Last loaded listing projects: {last_loaded_listing_projects}")

    print("Extracting listing projects data...")
    listing_projects = extract_listing_projects(last_loaded_listing_projects)
    print(f"Extracted listing projects data.")

    print("Transforming listing projects data...")
    listing_projects_df = transform_listing_projects(listing_projects, load_date)
    print(f"Transformed listing projects data.")

    print("Loading listing projects data into BigQuery...")
    load_dataframe_with_merge(
    df=listing_projects_df,
    bq_table_env_var="BQ_LISTING_PROJECTS_TABLE",
    schema_path=LISTING_PROJECT_SCHEMA_PATH,
    key_column="listing_project_id",
    updated_at_col="updated_at",
    )
    print("listing projects data loaded.")


if __name__ == "__main__":
    run_etl()