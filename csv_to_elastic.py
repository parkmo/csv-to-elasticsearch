# csv_to_elastic.py

from elasticsearch8 import Elasticsearch, helpers
import csv
import sys

elastic_host = 'http://localhost:9200'
elastic_api_key = None


def elastic_setup(InElasticHost):
    global elastic_host
    elastic_host = InElasticHost

    print(" \n----- ELASTIC-Setup -----\n ")
    print("Endpoint on '%s'" % (elastic_host))

def test_elastic():
    global elastic_host
    global elastic_api_key

#    print(f'elastic_api_key = [{elastic_api_key}]')
    client = Elasticsearch(elastic_host, api_key = elastic_api_key)
    resp = client.info()
    print(resp)

def import_to_elastic_from_dict(dict_document, index_name):
    global elastic_host
    global elastic_api_key
    print(dict_document)

    print(f'Import: dict to index {index_name}')
#    print(f'elastic_api_key = [{elastic_api_key}]')
    es = Elasticsearch(elastic_host, api_key = elastic_api_key)
    helpers.bulk(es, dict_document, index=index_name)
#    helpers.bulk(es, gendata())

def import_to_elastic_from_csv(file_name, index_name, delimiter):
    global elastic_host
    global elastic_api_key

    print(f'Import from File: {file_name} to index {index_name}')
#    print(f'elastic_api_key = [{elastic_api_key}]')
    es = Elasticsearch(elastic_host, api_key = elastic_api_key)
    with open(file_name.name) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        helpers.bulk(es, reader, index=index_name)
