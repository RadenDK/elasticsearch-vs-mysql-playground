import json
import time
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from requests.auth import HTTPBasicAuth
import requests
from helpers.es.es_setup_methods import *

# --- Config ---
ES_URL = "http://localhost:9201"
ES_USER = "elastic"
ES_PASSWORD = "apppw"

CSV_FILE = "../../products-2000000.csv"

def analyze_text(payload):
    endpoint = f"{ES_URL}/{get_index_name()}/_analyze"

    response = requests.post(
        endpoint,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )

    if response.status_code != 200:
        raise RuntimeError(f"Analyze error: {response.text}")

    tokens = [t["token"] for t in response.json().get("tokens", [])]
    return tokens


def search_text(payload, index):
    endpoint = f"{ES_URL}/{index}/_search"

    response = requests.post(
        endpoint,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )

    if response.status_code != 200:
        raise RuntimeError(f"Search error: {response.text}")

    return response.json()


def get_mapping(index):
    endpoint = f"{ES_URL}/{index}/_mapping"

    response = requests.get(
        endpoint,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        raise RuntimeError(f"Mapping error: {response.text}")

    return response.json()

def get_doc_info(doc_id, index):
    endpoint = f"{ES_URL}/{index}/_doc/{doc_id}"

    response = requests.get(
        endpoint,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        raise RuntimeError(f"Document retrieval error: {response.text}")

    return response.json()