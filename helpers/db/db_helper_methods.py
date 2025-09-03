from helpers.db.db_query_methods import *
import pandas as pd

def show_table_indexes(table_name, database):
    """
    Retrieve and display all indexes for a given table.
    
    Args:
        table_name (str): Name of the table
        database (str): Database name
        
    Returns:
        DataFrame: Pandas DataFrame containing index information
    """
    query = """
        SELECT 
            index_name AS 'Index Name',
            GROUP_CONCAT(column_name ORDER BY seq_in_index) AS 'Columns',
            index_type AS 'Type',
            non_unique AS 'Non Unique'
        FROM 
            information_schema.statistics
        WHERE 
            table_schema = %s AND table_name = %s
        GROUP BY 
            index_name, index_type, non_unique
        ORDER BY 
            index_name
    """
    
    with get_connection(database) as cursor:
        cursor.execute(query, (database, table_name))
        rows = cursor.fetchall()
        
        if not rows:
            print(f"No indexes found for table '{table_name}' in database '{database}'")
            return None
            
        headers = ['Index Name', 'Columns', 'Type', 'Non Unique']
        df = pd.DataFrame(rows, columns=headers)
        
        # Convert Non Unique from 1/0 to boolean True/False for clarity
        if 'Non Unique' in df.columns:
            df['Non Unique'] = df['Non Unique'].map({1: True, 0: False})
            
        return df
    
def drop_non_clustered_indexes(table_name, database, output=False):
    """
    Drop all non-clustered (non-PRIMARY) indexes from a table.
    No confirmation needed as this is for test data.
    
    Args:
        table_name (str): Name of the table
        database (str): Database name
        
    Returns:
        int: Number of indexes dropped
    """
    # First get all indexes
    query = """
        SELECT 
            index_name
        FROM 
            information_schema.statistics
        WHERE 
            table_schema = %s 
            AND table_name = %s
            AND index_name != 'PRIMARY'
        GROUP BY 
            index_name
    """
    
    with get_connection(database) as cursor:
        cursor.execute(query, (database, table_name))
        indexes = [row[0] for row in cursor.fetchall()]
        
        if not indexes:
            if output: print(f"No non-clustered indexes found in '{table_name}'")
            return 0

        if output: print(f"Found {len(indexes)} non-clustered indexes to drop")

        # Drop each index
        dropped = 0
        for idx in indexes:
            try:
                drop_query = f"DROP INDEX `{idx}` ON `{table_name}`"
                cursor.execute(drop_query)
                if output: print(f"Dropped index '{idx}'")
                dropped += 1
            except Exception as e:
                if output: print(f"Error dropping index '{idx}': {str(e)}")

        if output: print(f"Successfully dropped {dropped} of {len(indexes)} indexes")
        return dropped