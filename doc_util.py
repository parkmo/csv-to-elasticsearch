# csv_to_elastic.py

import argparse
import sys
from distutils.version import LooseVersion#, StrictVersion
import logging
import json

g_logger = None

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

    g_logger.info(" \n----- ELASTIC-Setup -----\n ")
    g_logger.info("Endpoint on '%s'" % (elastic_host))

def make_match_dict_m(document_fields):
    lst_match = [ ]
    for cur_doc_field in document_fields:
        dict_match = { }
        dict_match['match_phrase'] = { }
        splited_filed = cur_doc_field.split(":")
        key = splited_filed[0]
        value = ":".join(splited_filed[1:])
        dict_match['match_phrase'][key] = value
        lst_match.append(dict_match)
    return lst_match

def check_exists_m(index, document_fields, should_match = 0):
    global elastic_host
    global elastic_api_key
    g_logger.info(f'index {index} {document_fields}')
    g_logger.debug(f'elastic_api_key = [{elastic_api_key}]')
    es = Elasticsearch(elastic_host, api_key = elastic_api_key)
    lst_match = make_match_dict_m(document_fields)
    if ( should_match == 0 ):
        should_match = len(lst_match)
    resp = es.search(index = index,
            query = { "bool": { "should": lst_match , "minimum_should_match": should_match } }
            )
    g_logger.debug(f'{resp}')
    print(len(resp['hits']['hits']))
    return len(resp['hits']['hits'])

def make_match_dict_1(document_fields):
    dict_match = { }
    for cur_doc_field in document_fields:
        splited_filed = cur_doc_field.split(":")
        key = splited_filed[0]
        value = ":".join(splited_filed[1:])
        dict_match[key] = value
    return dict_match

def check_exists_1(index, document_fields):
    global elastic_host
    global elastic_api_key
    g_logger.info(f'index {index} {document_fields}')
    g_logger.debug(f'elastic_api_key = [{elastic_api_key}]')
    es = Elasticsearch(elastic_host, api_key = elastic_api_key)
    dict_match = make_match_dict_1(document_fields)

    resp = es.search(index = index,
            query = { "match": dict_match }
            )
    g_logger.debug(f'{resp}')
    print (len(resp['hits']['hits']))
    return len(resp['hits']['hits'])

def error_func():
    g_logger.error("Error")

if __name__ == '__main__':
    dict_cmd_func = {
            None: error_func
#            , 'chkexist': check_exists_1
            , 'chkexist': check_exists_m
            }

    g_logger = logging.getLogger(__name__)
    FORMAT = '[%(asctime)s] %(message)s'
    parser = argparse.ArgumentParser(description='Import a CSV-File to ElasticSearch.')
    parser.add_argument('--api-key', required=False, type=str,
                        default=None, help='api key')
    parser.add_argument('--es-version', required=False, type=str,
                        default=None, help='"8" or "7" If it is not present and the api-key is present, it will be set to "8".')
    parser.add_argument('--cmd', required=True, type=str,
                        default=None, help='[chkexist]')
    parser.add_argument('--elastic-host', required=False, type=str,
                        default='http://localhost:9200',
                        help='elasticsearch host ( default http://localhost:9200 )')

    parser.add_argument('--loglevel', required=False, type=str,
                        default='warning',
                        help='Provide logging level. Example --loglevel debug, default=warning')
    parser.add_argument('--elastic-index', required=True, type=str,
                        help='elasticsearch index')
    parser.add_argument('--match', required=False, type=str,
                        default=[], nargs='+',
#                        help='filed:value A:B C:D'
                        help='field:value A:B'
                        )
    args = parser.parse_args()
    logging.basicConfig(format=FORMAT)
    g_logger.setLevel(level=args.loglevel.upper())
    g_logger.info("ES dcoument exist checker")
    elastic_setup(args.elastic_host, args.api_key, args.es_version)
    func_run = dict_cmd_func[args.cmd]
    func_run(args.elastic_index, args.match)
