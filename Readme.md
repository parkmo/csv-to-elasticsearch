![python 3.9](https://img.shields.io/badge/python-3.9-blue)
![elasticsearch 7.4.0](https://img.shields.io/badge/elasticsearch-7.4.0-blue)
![elasticsearch 8.3.3](https://img.shields.io/badge/elasticsearch-8.3.3-blue)

# CSV-File to elasticsearch

Import a CSV-File to ElasticSearch.

## How it works
If required, the Elasticsearch host, the column name of the CSV to be used for the ID of the document in the Elasticsearch, the delimiter of the CSV used and the columns - which should not be imported into the Elasticsearch.

The CSV is read and checked with panda. If necessary, the data frame is adjusted and saved as a new csv.

With the Elasticserach Pyhton Client, the data is imported via bulk.

This script requires Python 3 with the 
elasticsearch7 and pandas modules 
to be installed 
- python -m pip install elasticsearch7
- python -m pip install pandas

## Usage

```shell
usage: main.py [-h] 
        --csvfile CSVFILE
        [--csv-save]
        [--with-test WITH_TEST]
        [--only-test]
        --elastic-index ELASTIC_INDEX 
        [--elastic-host ELASTIC_HOST]
        [--id-column ID_COLUMN]
        [--csv-delimiter CSV_DELIMITER]
        [--add-columns ADD_COLUMNS [ADD_COLUMNS ...]]
        [--ignore-columns IGNORE_COLUMNS [IGNORE_COLUMNS ...]]
        [--es-version ES_VERSION]
        [--add-ts ADD_TS]
        [--api-key API_KEY]
```

```shell
arguments:
  -h, --help            show this help message and exit
  --csvfile CSVFILE     path to csv-file to import - encoding utf-8
  --csv-save            csv update and save
  --with-test WITH_TEST
                        Run with test
  --only-test           Only test
  --elastic-index ELASTIC_INDEX
                        elasticsearch index
  --elastic-host ELASTIC_HOST
                        elasticsearch host ( default http://localhost:9200 )
  --id-column ID_COLUMN
                        elasticsearch uses the _id with the data of this given csv-column
  --csv-delimiter CSV_DELIMITER
                        csv delimiter ( default: ; )
  --add-columns ADD_COLUMNS [ADD_COLUMNS ...]
                        add columns. A:B C:D
  --ignore-columns IGNORE_COLUMNS [IGNORE_COLUMNS ...]
                        columns of csv to ignore for the import
  --es-version ES_VERSION
                        "8" or "7" If it is not present and the api-key is present, it will be set to "8".
  --add-ts ADD_TS       add @timestamp "{isoformat}" or "2022-09-22 11:22:33"
  --api-key API_KEY     api key
```

## Example 
sample.csv

| Nr | Nachname | Vorname   | Geburtstag | Geschlecht | Stadt       | Bemerkung   |
|----|----------|-----------|------------|------------|-------------|-------------|
| 1  | Stumpf   | Magdalena | 1930-05-20 | M          | Mayen       |             |
| 2  | Schomber | Guenther  | 1956-01-04 | W          | Waldmünchen | Apfelkuchen |

```shell
python main.py  \
        --csvfile sample.csv  \
        --elastic-index sample_index  \
        --ignore-columns Bemerkung Geschlecht "some other column name"  \
        --id-column Nr
```

imported to Elasticsearch
````json
{
        "_index" : "sample_index",
        "_type" : "_doc",
        "_id" : "1",
        "_score" : 1.0,
        "_source" : {
          "Nr" : "1",
          "Nachname" : "Stumpf",
          "Vorname" : "Magdalena",
          "Geburtstag" : "1930-05-20",
          "Stadt" : "Mayen"
        }
      },
      {
        "_index" : "sample_index",
        "_type" : "_doc",
        "_id" : "2",
        "_score" : 1.0,
        "_source" : {
          "Nr" : "2",
          "Nachname" : "Schomber",
          "Vorname" : "Guenther",
          "Geburtstag" : "1956-01-04",
          "Stadt" : "Waldmünchen"
        }
      },
````

## Test Environment for Elasticsearch with Kibana

To setup an elasticsearch for testing, just run 
```bash
docker-compose up -d
```

and use [`Kibana`](http://localhost:5601)
