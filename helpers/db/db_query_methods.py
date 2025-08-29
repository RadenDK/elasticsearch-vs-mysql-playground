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


def execute_query(query, params=None, database=None, as_df=False):
    """Execute a single query and optionally fetch results as DataFrame"""
    with get_connection(database) as cursor:
        cursor.execute(query, params or ())
        if cursor.description:  # means it's a SELECT (or similar returning rows)
            rows = cursor.fetchall()
            if as_df:
                columns = [col[0] for col in cursor.description]
                return pd.DataFrame(rows, columns=columns)
            return rows
        return None


def execute_many_query(query, params_list, database=None):
    """Execute a query multiple times with different parameters"""
    with get_connection(database) as (cursor):
        cursor.executemany(query, params_list)

