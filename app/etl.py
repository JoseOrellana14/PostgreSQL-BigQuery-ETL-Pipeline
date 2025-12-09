from app.organizations import extract_organizations, transform_organizations, load_organizations, ORGANIZATION_SCHEMA_PATH
from app.users import extract_users, transform_users, USER_SCHEMA_PATH
from app.buyer_leads import extract_buyer_leads, transform_buyer_leads, BUYER_LEAD_SCHEMA_PATH
from app.seller_leads import extract_seller_leads, transform_seller_leads, SELLER_LEAD_SCHEMA_PATH
from app.property_units import extract_property_units, transform_property_units, PROPERTY_UNIT_SCHEMA_PATH
from app.property_opportunities import extract_property_opportunities, transform_property_opportunities, PROPERTY_OPPORTUNITY_SCHEMA_PATH
from app.property_sales import extract_property_sales, transform_property_sales, PROPERTY_SALE_SCHEMA_PATH
from app.chat_messages import extract_chat_messages, transform_chat_messages, CHAT_MESSAGE_SCHEMA_PATH
from app.appointments import extract_appointments, transform_appointments, APPOINTMENT_SCHEMA_PATH
from app.common.utils import get_load_date, get_last_loaded_timestamp, load_dataframe_with_merge

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
    last_loaded_users = get_last_loaded_timestamp("users", dataset=DATASET)
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
    load_dataframe_with_merge(
    df=buyer_leads_df,
    bq_table_env_var="BQ_BUYER_LEADS_TABLE",
    schema_path=BUYER_LEAD_SCHEMA_PATH,
    key_column="buyer_lead_id",
    updated_at_col="updated_at",
    )
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
    load_dataframe_with_merge(
    df=seller_leads_df,
    bq_table_env_var="BQ_SELLER_LEADS_TABLE",
    schema_path=SELLER_LEAD_SCHEMA_PATH,
    key_column="seller_lead_id",
    updated_at_col="updated_at",
    )
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
    load_dataframe_with_merge(
    df=property_units_df,
    bq_table_env_var="BQ_PROPERTY_UNITS_TABLE",
    schema_path=PROPERTY_UNIT_SCHEMA_PATH,
    key_column="property_unit_id",
    updated_at_col="updated_at",
    )
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
    load_dataframe_with_merge(
    df=property_opportunities_df,
    bq_table_env_var="BQ_PROPERTY_OPPORTUNITIES_TABLE",
    schema_path=PROPERTY_OPPORTUNITY_SCHEMA_PATH,
    key_column="property_opportunity_id",
    updated_at_col="updated_at",
    )
    print("property opportunities data loaded.")

    # =================
    # Property Sales ETL
    # =================
    last_loaded_property_sales = get_last_loaded_timestamp("property_sales", dataset=DATASET)
    print(f"Last loaded property sales: {last_loaded_property_sales}")

    print("Extracting property sales data...")
    property_sales = extract_property_sales(last_loaded_property_sales)
    print(f"Extracted property sales data.")

    print("Transforming property sales data...")
    property_sales_df = transform_property_sales(property_sales, load_date)
    print(f"Transformed property sales data.")

    print("Loading property sales data into BigQuery...")
    load_dataframe_with_merge(
    df=property_sales_df,
    bq_table_env_var="BQ_PROPERTY_SALES_TABLE",
    schema_path=PROPERTY_SALE_SCHEMA_PATH,
    key_column="property_sale_id",
    updated_at_col="updated_at",
    )
    print("property sales data loaded.")

    # =================
    # Chat messages ETL
    # =================
    last_loaded_chat_messages = get_last_loaded_timestamp("chat_messages", dataset=DATASET)
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
    last_loaded_appointments = get_last_loaded_timestamp("appointments", dataset=DATASET)
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



if __name__ == "__main__":
    run_etl()