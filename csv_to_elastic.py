# csv_to_elastic.py

import csv
import sys
from distutils.version import LooseVersion#, StrictVersion

elastic_host = 'http://localhost:9200'
elastic_api_key = None

def elastic_setup(InElasticHost, api_key, es_version):
    global elastic_host, elastic_api_key
    global Elasticsearch, helpers
    elastic_host = InElasticHost
    elastic_api_key = api_key

    # default es_version
    if ( es_version == None ):
        if ( elastic_api_key == None ):
            es_version = "7"
        else:
            es_version = "8"
    if ( LooseVersion(es_version) >= LooseVersion("8") ):
      from elasticsearch8 import Elasticsearch, helpers
    else:
      from elasticsearch7 import Elasticsearch, helpers

    print(" \n----- ELASTIC-Setup -----\n ")
    print("Endpoint on '%s'" % (elastic_host))

def test_elastic():
    global elastic_host
    global elastic_api_key

#    print(f'elastic_api_key = [{elastic_api_key}]')
    if ( elastic_api_key == None ):
      client = Elasticsearch(elastic_host)
    else:
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
