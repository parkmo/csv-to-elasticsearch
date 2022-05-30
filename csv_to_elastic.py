# csv_to_elastic.py

from elasticsearch7 import Elasticsearch, helpers
import csv

elastic_host = 'http://localhost:9200'


def elastic_setup(InElasticHost):
    global elastic_host
    elastic_host = InElasticHost

    print(" \n----- ELASTIC-Setup -----\n ")
    print("Endpoint on '%s'" % (elastic_host))

def test_elastic():
    global elastic_host

    client = Elasticsearch(elastic_host)
    resp = client.info()
    print(resp)

def import_to_elastic_from_csv(file_name, index_name, delimiter):
    global elastic_host

    print(f'Import from File: {file_name} to index {index_name}')
    es = Elasticsearch(elastic_host)
    with open(file_name) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        helpers.bulk(es, reader, index=index_name)