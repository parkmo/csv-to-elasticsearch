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
        --elastic-index ELASTIC_INDEX 
        [--elastic-host ELASTIC_HOST]
        [--id-column ID_COLUMN]
        [--csv-delimiter CSV_DELIMITER]
        [--ignore-columns IGNORE_COLUMNS [IGNORE_COLUMNS ...]]
```

```shell
arguments:
  -h, --help            show this help message and exit
  --csvfile CSVFILE     path to csv-file to import - encoding utf-8
  --elastic-host ELASTIC_HOST
                        elasticsearch host ( default http://localhost:9200 )
  --elastic-index ELASTIC_INDEX
                        elasticsearch index
  --id-column ID_COLUMN
                        elasticsearch uses the _id with the data of this given
                        csv-column
  --csv-delimiter CSV_DELIMITER
                        csv delimiter ( default: ; )
  --ignore-columns IGNORE_COLUMNS [IGNORE_COLUMNS ...]
                        columns of csv to ignore for the import
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