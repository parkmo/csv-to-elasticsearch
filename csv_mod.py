# csv_mod.py
import pandas as pd
import sys
from datetime import datetime

csvfile = ''
delimiter = ';'
delete_columns = []
data = None
dataFrameChanged = False
add_timestamp = None
add_columns = [ ]

def csv_setup(Incsvfile, Indelimiter, Add_timestamp, Add_columns):
    global csvfile
    csvfile = Incsvfile

    global delimiter
    delimiter = Indelimiter
    global add_timestamp
    add_timestamp = Add_timestamp
    global add_columns
    add_columns = Add_columns

    print(" \n----- CSV-Setup -----\n ")
    print("CSV-File %s with delimiter %s'" % (csvfile, delimiter))

def load():
    print(" \n----- LOAD AND CHECK CSV -----\n ")
    global csvfile
    global delimiter
    global data
    global add_timestamp
    global add_columns

    # drop function which is used in removing or deleting rows or columns from the CSV files
    data = pd.read_csv(csvfile,
                       encoding="utf-8",
                       delimiter=delimiter,
                       #index_col=0,
                       on_bad_lines='skip',
                       encoding_errors='ignore',
                       low_memory=False)
    if ( data.empty == True ):
        data = None
        print(" \n----- Data is emtry -----\n ")
        sys.exit()
    print(f"Describe CSV:\n {data.describe(include='all')} \n")
    if ( add_timestamp != None ):
        if ( add_timestamp == "{isoformat}" ):
            data['@timestamp']=datetime.today().isoformat()
        else:
            data['@timestamp']=add_timestamp
    if ( len(add_columns) > 0 ):
        for cur_add_column in add_columns:
            splited_cur_col = cur_add_column.split(":")
            data[splited_cur_col[0]] = splited_cur_col[1]

#    data['_index']='my_index'

def get_dict():
    global data
    return data.to_dict(orient='records')

def save():
    print(" \n----- SAVE CSV -----\n ")
    global data
    global dataFrameChanged 

    if dataFrameChanged is True:
        print(f"Result - first Rows: \n {data.head(2)} \n")
        csvfile = 'mod_' + str(csvfile.name)
        pd.DataFrame.to_csv(data, csvfile, sep=delimiter, encoding='utf-8')

    print(f"Describe CSV:\n {data.describe(include='all')} \n")

def modifiy(deleteColumns, idColumn):
    print(" \n----- MODIFY AND CHECK CSV -----\n ")
    print("Columns to ignore %s'" % (deleteColumns))
    print("Column for id %s'" % (idColumn))
    global data
    global dataFrameChanged 
    print(f"Describe CSV:\n {data.describe(include='all')} \n")

    if idColumn is not None:
        data.set_index(idColumn, inplace=True)
        data['_id'] = data.index
        dataFrameChanged = True
    else:
        data.index.name = 'line'

    print(f"First Rows: \n {data.head(2)} \n")

    ignoreColumns = 0
    for column in deleteColumns:
        data.drop(column, inplace=True, axis=1, errors='ignore')
        ignoreColumns += 1
    if ignoreColumns > 0:
        print(f"dropped {ignoreColumns} columns to ignore")
        dataFrameChanged = True
