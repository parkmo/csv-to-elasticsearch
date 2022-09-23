#!/usr/bin/python3
# Import a csv-file to elasticsearch

import argparse
import sys
import csv_mod
import csv_to_elastic

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import a CSV-File to ElasticSearch.')

    parser.add_argument('--csvfile', required=True, type=argparse.FileType('r', encoding='utf-8'),
                        help='path to csv-file to import - encoding utf-8')

    parser.add_argument('--elastic-host', required=False, type=str,
                        default='http://localhost:9200',
                        help='elasticsearch host ( default http://localhost:9200 )')

    parser.add_argument('--elastic-index', required=True, type=str,
                        help='elasticsearch index')

    parser.add_argument('--id-column', required=False, type=str,
                        default=None,
                        help='elasticsearch uses the _id with the data of this given csv-column')

    parser.add_argument('--csv-delimiter', required=False, type=str,
                        default=';',
                        help='csv delimiter ( default: ; )')

    parser.add_argument('--add-columns', required=False, type=str,
                        default=[], nargs='+',
                        help='add columns. A:B C:D')
    parser.add_argument('--ignore-columns', required=False, type=str,
                        default=[], nargs='+',
                        help='columns of csv to ignore for the import')

    parser.add_argument('--add-ts', required=False, type=str,
                        default=None, help='add @timestamp "{isoformat}" or "2022-09-22 11:22:33"')

    parser.add_argument('--api-key', required=False, type=str,
                        default=None, help='api key')
    args = parser.parse_args()
    csv_to_elastic.elastic_api_key = args.api_key

    csv_mod.csv_setup(args.csvfile, args.csv_delimiter, args.add_ts, args.add_columns)
    csv_mod.load()
    csv_mod.modifiy(args.ignore_columns, args.id_column)
    csv_mod.save()
    csv_to_elastic.elastic_setup(args.elastic_host)

    csv_to_elastic.test_elastic()
#    csv_to_elastic.import_to_elastic_from_csv(csv_mod.csvfile, args.elastic_index, csv_mod.delimiter)
    csv_to_elastic.import_to_elastic_from_dict(csv_mod.get_dict(), args.elastic_index)
