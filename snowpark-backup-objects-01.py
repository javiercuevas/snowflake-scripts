from snowflake.snowpark import Session
import os

# --- Configure your Snowflake connection ---
connection_parameters = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "<your_warehouse>",
    "database": "<your_database>",
    "schema": "<your_schema>"
}

session = Session.builder.configs(connection_parameters).create()

# --- Target schema and stage ---
target_schema = "<your_schema>"
target_stage = "@my_stage"   # must exist beforehand

# --- Collect tables and views ---
tables = session.sql(f"""
    SELECT table_name, table_type
    FROM {connection_parameters['database']}.information_schema.tables
    WHERE table_schema = '{target_schema}'
""").collect()

views = session.sql(f"""
    SELECT table_name
    FROM {connection_parameters['database']}.information_schema.views
    WHERE table_schema = '{target_schema}'
""").collect()

# --- Helper: save DDL to stage ---
def save_ddl_to_stage(object_name, object_type):
    ddl = session.sql(f"SELECT GET_DDL('{object_type}', '{target_schema}.{object_name}')").collect()[0][0]
    file_name = f"{object_name}.sql"
    
    # Write to local temp file
    with open(file_name, "w") as f:
        f.write(ddl)
    
    # Put file into stage
    session.file.put(file_name, target_stage, auto_compress=False, overwrite=True)
    
    # Clean up local file
    os.remove(file_name)

# --- Process tables ---
for t in tables:
    save_ddl_to_stage(t["TABLE_NAME"], t["TABLE_TYPE"])

# --- Process views ---
for v in views:
    save_ddl_to_stage(v["TABLE_NAME"], "VIEW")

print("DDL extraction complete. Files saved to stage.")
