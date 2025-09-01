import pandas as pd
import time
from helpers.db.db_query_methods import *

# ------------------------
# Config
# ------------------------
HOST = "localhost"
USER = "root"
PORT = 3307

def get_database_name():
    return "test_database"

# ------------------------
# Schema Setup
# ------------------------
def init_db(records_max=2000000):
    """Drop + recreate the database and load data"""

    if records_max < 1 or records_max > 20000000:
        raise ValueError("records_max must be between 1 and 2,000,000")

    db_name = get_database_name()

    execute_query(f"DROP DATABASE IF EXISTS {db_name}")
    execute_query(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print("Created empty database")

    init_tables()
    populate_tables(records_max)

    execute_query(f"ANALYZE TABLE products;", database=db_name)
    execute_query(f"ANALYZE TABLE categories;", database=db_name)
    execute_query(f"ANALYZE TABLE brands;", database=db_name)
    execute_query(f"ANALYZE TABLE colors;", database=db_name)
    execute_query(f"ANALYZE TABLE availability;", database=db_name)


def init_tables():
    """Initialize normalized tables (products, brands, categories, colors, availability)"""
    db_name = get_database_name()

    # Drop in reverse dependency order
    for table in ["products", "brands", "categories", "colors", "availability"]:
        execute_query(f"DROP TABLE IF EXISTS {table}", database=db_name)

    # Brands
    execute_query("""
        CREATE TABLE brands (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
    """, database=db_name)

    # Categories
    execute_query("""
        CREATE TABLE categories (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
    """, database=db_name)

    # Colors
    execute_query("""
        CREATE TABLE colors (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) UNIQUE
        )
    """, database=db_name)

    # Availability
    execute_query("""
        CREATE TABLE availability (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) UNIQUE
        )
    """, database=db_name)

    # Products
    execute_query("""
        CREATE TABLE products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            description TEXT,
            brand_id INT,
            category_id INT,
            price FLOAT,
            currency VARCHAR(10),
            stock INT,
            ean VARCHAR(50),
            color_id INT,
            size VARCHAR(100),
            availability_id INT,
            internal_id VARCHAR(255),

            FOREIGN KEY (brand_id) REFERENCES brands(id),
            FOREIGN KEY (category_id) REFERENCES categories(id),
            FOREIGN KEY (color_id) REFERENCES colors(id),
            FOREIGN KEY (availability_id) REFERENCES availability(id)
        )
    """, database=db_name)
    print("Created empty tables")


# ------------------------
# Data Population
# ------------------------
def populate_tables(records_max=2000000):
    """Populate tables from CSV into normalized structure"""
    db_name = get_database_name()
    csv_file = "../../products-2000000.csv"

    # Read CSV
    df = pd.read_csv(csv_file, nrows=records_max)
    df.rename(columns={"Internal ID": "Internal_ID"}, inplace=True)
    df = df.where(pd.notnull(df), None)  # Replace NaN with None

    # Extract unique values for lookup tables
    brands = sorted(set(df["Brand"].dropna()))
    categories = sorted(set(df["Category"].dropna()))
    colors = sorted(set(df["Color"].dropna()))
    availabilities = sorted(set(df["Availability"].dropna()))

    print("Starting to insert data")

    # Insert into lookup tables
    _insert_lookup("brands", brands, db_name)
    _insert_lookup("categories", categories, db_name)
    _insert_lookup("colors", colors, db_name)
    _insert_lookup("availability", availabilities, db_name)

    # Build mapping dictionaries (name -> id)
    brand_map = _build_lookup_map("brands", db_name)
    category_map = _build_lookup_map("categories", db_name)
    color_map = _build_lookup_map("colors", db_name)
    avail_map = _build_lookup_map("availability", db_name)

    # Transform product rows to normalized structure
    product_rows = []
    for _, row in df.iterrows():
        product_rows.append((
            row["Name"],
            row["Description"],
            brand_map.get(row["Brand"]),
            category_map.get(row["Category"]),
            row["Price"],
            row["Currency"],
            row["Stock"],
            row["EAN"],
            color_map.get(row["Color"]),
            row["Size"],
            avail_map.get(row["Availability"]),
            row["Internal_ID"]
        ))

    # Insert products in batches
    insert_sql = """
        INSERT INTO products
        (name, description, brand_id, category_id, price, currency, stock,
         ean, color_id, size, availability_id, internal_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    _batch_insert("products", insert_sql, product_rows, db_name)


# ------------------------
# Helpers
# ------------------------
def _insert_lookup(table, values, db_name):
    """Insert unique values into a lookup table (id, name)"""
    if not values:
        return
    sql = f"INSERT INTO {table} (name) VALUES (%s)"
    execute_many_query(sql, [(v,) for v in values], database=db_name)


def _build_lookup_map(table, db_name):
    """Build a dict mapping name -> id from a lookup table"""
    rows = execute_query(f"SELECT id, name FROM {table}", database=db_name)
    return {name: id for (id, name) in rows}


def _batch_insert(table, insert_sql, rows, db_name, batch_size=10000):
    """Insert rows in batches for performance"""
    print(f"\nStarting insert into '{table}' ({len(rows)} rows)...")
    start = time.time()
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        execute_many_query(insert_sql, batch, database=db_name)
    print(f"Finished inserting {len(rows)} rows into '{table}' in {time.time() - start:.2f}s.")
