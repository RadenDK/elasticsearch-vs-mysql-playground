import pymysql
from pymysql.err import OperationalError
from contextlib import contextmanager
import pymysql
import pandas as pd


from helpers.db.db_setup_methods import *

# MySQL connection settings
host = "localhost"
user = "root"
port = 3307

@contextmanager
def get_connection(database=None):
    """Context manager for database connections"""
    connection = None
    cursor = None
    try:
        # Connect to database
        connection_params = {'host': host, 'user': user, 'port': port}
        if database:
            connection_params['database'] = database
            
        connection = pymysql.connect(**connection_params)
        cursor = connection.cursor()
        yield cursor
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def test_connection(database=None):
    """Test connection to MySQL database"""
    try:
        with get_connection(database) as (cursor):
            cursor.execute("SELECT 1")
            return True, f"Successfully connected to MySQL{'database ' + database if database else 'server'}!"
    except OperationalError as e:
        return False, f"Failed to connect to MySQL: {e}"


import time
import pandas as pd

def execute_query(query, params=None, database=None, print_as_df=False, show_metrics=False):
    """Execute a single query and optionally fetch results as DataFrame with timing + cursor metadata"""

    with get_connection(database) as cursor:
        start_time = time.time()
        cursor.execute(query, params or ())
        elapsed = (time.time() - start_time) * 1000  # ms

        rows = None
        if cursor.description:  # SELECT-like
            rows = cursor.fetchall()
            if print_as_df:
                columns = [col[0] for col in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                display(df)


        if show_metrics:
            if cursor.description:  # SELECT
                print(f"[QUERY METRICS] {cursor.rowcount} rows fetched, {elapsed:.2f} ms")
            else:  # INSERT/UPDATE/DELETE
                print(f"[QUERY METRICS] {cursor.rowcount} rows affected, {elapsed:.2f} ms")
                if cursor.lastrowid:
                    print(f"  Last insert ID: {cursor.lastrowid}")

    if (print_as_df or show_metrics):
        return
    
    return rows



def execute_many_query(query, params_list, database=None):
    """Execute a query multiple times with different parameters"""
    with get_connection(database) as (cursor):
        cursor.executemany(query, params_list)



def clear_mysql_cache(database=None):
    """Clear MySQL cache and buffers"""
    if database is None:
        from helpers.db.db_setup_methods import get_database_name
        database = get_database_name()
        
    try:
        # Try MySQL 8+ approach
        execute_query("FLUSH TABLES", database=database)
        execute_query("FLUSH HOSTS", database=database)
        execute_query("RESET PERSIST", database=database)
    except Exception:
        # Fallback for older MySQL versions
        try:
            execute_query("RESET QUERY CACHE", database=database)
            execute_query("FLUSH QUERY CACHE", database=database)
        except Exception:
            # Last resort
            execute_query("FLUSH TABLES", database=database)
            
    print("MySQL cache cleared")