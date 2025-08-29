import pandas as pd
import time

from helpers.db.db_query_methods import *

# MySQL connection settings
host = "localhost"
user = "root"
port = 3307



def get_table_name():
    return "test_table"

def get_database_name():
    return "test_database"


def init_db(records_max=2000000):
    """Initialize the database and create necessary tables and populate the table"""
    
    execute_query(f"DROP DATABASE IF EXISTS {get_database_name()}")
    
    execute_query(f"CREATE DATABASE IF NOT EXISTS {get_database_name()}")

    init_tables()

    populate_table(records_max)

def init_tables():
    """Initialize the necessary tables"""

    # TODO make it so that you can init with different primary keys
    base_schema = """
        `Index` INT,
        `Name` VARCHAR(255),
        `Description` TEXT,
        `Brand` VARCHAR(255),
        `Category` VARCHAR(255),
        `Price` FLOAT,
        `Currency` VARCHAR(10),
        `Stock` INT,
        `EAN` VARCHAR(50),
        `Color` VARCHAR(100),
        `Size` VARCHAR(100),
        `Availability` VARCHAR(50),
        `Internal_ID` VARCHAR(255),
        PRIMARY KEY (`Index`)
    """

    table_name = get_table_name()
    db_name = get_database_name()

    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {base_schema}
        )
    """

    execute_query(drop_query, database=db_name)

    execute_query(create_query, database=db_name)



def populate_table(records_max=2000000):
    csv_file = "../../products-2000000.csv"

    # Read only up to records_max rows
    df = pd.read_csv(csv_file, nrows=records_max)

    # Rename Internal ID column for MySQL compatibility
    df.rename(columns={"Internal ID": "Internal_ID"}, inplace=True)

    # Replace NaN with None (for DB insertion)
    df = df.where(pd.notnull(df), None)

    # Convert DataFrame → list of tuples
    data = [tuple(row) for row in df.values.tolist()]

    # Build insert template with placeholder for table name
    columns = ", ".join(f"`{c}`" for c in df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql_template = f"INSERT INTO {{table}} ({columns}) VALUES ({placeholders})"

    batch_size = 10000

    table = get_table_name()

    print(f"\nStarting insert into '{table}' (max {records_max} rows)...")
    start_time = time.time()

    for start in range(0, len(data), batch_size):
        batch = data[start:start + batch_size]
        execute_many_query(insert_sql_template.format(table=table), batch, database=get_database_name())

    elapsed = time.time() - start_time
    print(f"Finished inserting {len(data)} rows into '{table}' in {elapsed:.2f} seconds.")


