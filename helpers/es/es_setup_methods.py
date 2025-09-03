import json
import time
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from requests.auth import HTTPBasicAuth
import requests

# --- Config ---
ES_URL = "http://localhost:9201"
ES_USER = "elastic"
ES_PASSWORD = "apppw"

CSV_FILE = "../../products-2000000.csv"

# Connect once globally
es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASSWORD))

def test_connection():
    """Test connection to Elasticsearch"""
    try:
        info = es.info()
        return True, f"Connected to Elasticsearch {info['version']['number']}"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

def get_index_name():
    return "my_index"

def get_default_mapping():
    mapping = {
        "mappings": {
            "properties": {
                "title": {
                    "type": "text",
                    "fields": {
                        "standard": {"type": "text", "analyzer": "standard"},
                    }
                },
            }
        }
    }
    return mapping

def get_default_settings():
    settings = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "default_ngram": {
                        "tokenizer": "default_ngram_tokenizer"
                    }
                },
                "tokenizer": {
                    "default_ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 3,
                        "max_gram": 4,
                        "token_chars": ["letter", "digit"]
                    }
                }
            }
        }
    }
    return settings

def init_index(index_name=None, mapping=None, settings=None, documents_records=None):
    """Initialize an index with optional mappings and settings"""
    try:
        # Set defaults if not provided
        index_name = index_name or get_index_name()
        mapping = mapping or get_default_mapping()
        settings = settings or get_default_settings()
        
        # Delete the index if it already exists
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"Deleted existing index: {index_name}")

        # Create the new index
        es.indices.create(index=index_name, body={**mapping, **settings})
        print(f"Created index: {index_name}")

        # Load data if CSV file is provided
        load_csv(CSV_FILE, index_name, records_limit=documents_records)

        return True, f"Index {index_name} initialized successfully"
    except Exception as e:
        return False, f"Failed to initialize index: {str(e)}"

def load_csv(csv_file, index_name, records_limit=None, batch_size=1000, lowercase_headers=True, exclude_columns=None):
    """
    Load a CSV file into Elasticsearch.
    - records_limit: how many rows to insert (None = all)
    - batch_size: how many docs to send in each bulk insert
    - lowercase_headers: whether to convert column names to lowercase
    - exclude_columns: list of column names to exclude from indexing
    """
    try:
        # Set default excluded columns if not provided
        if exclude_columns is None:
            exclude_columns = ['currency', 'ean', 'internal id']
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Lowercase column headers if requested
        if lowercase_headers:
            df.columns = map(str.lower, df.columns)
            # Also lowercase the exclude_columns list for consistency
            exclude_columns = [col.lower() for col in exclude_columns]
        
        # Drop excluded columns if they exist
        columns_to_drop = [col for col in exclude_columns if col in df.columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            print(f"Excluded columns: {', '.join(columns_to_drop)}")
        
        # Replace NaN with None
        df = df.where(pd.notnull(df), None)

        # Limit records if specified
        if records_limit:
            df = df.head(records_limit)

        # Convert to records
        records = df.to_dict(orient="records")

        # Bulk index
        start_time = time.time()
        print(f"Inserting {len(records)} docs into '{index_name}'")

        for start in range(0, len(records), batch_size):
            end = start + batch_size
            batch = records[start:end]
            actions = [
                {"_index": index_name, "_source": rec}
                for rec in batch
            ]
            helpers.bulk(es, actions)

        elapsed = time.time() - start_time
        print(f"Inserted {len(records)} docs in {elapsed:.2f} seconds")
        return True
    except Exception as e:
        print(f"Error loading CSV: {str(e)}")
        return False