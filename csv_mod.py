# csv_mod.py
import pandas as pd
import sys
from datetime import datetime
from distutils.version import LooseVersion#, StrictVersion

csvfile = ''
delimiter = ';'
delete_columns = []
data = None
dataFrameChanged = False
add_timestamp = None
add_columns = [ ]

def validate_arguments(add_columns, cb_false = None):
    mesg = None
    if ( len(add_columns) > 0 ):
        for cur_add_column in add_columns:
            splited_cur_col = cur_add_column.split(":")
            if ( len(splited_cur_col) != 2 ):
                mesg = "error: add-columns has no delimiter ':'"
                break
            if ( len(splited_cur_col[0]) == 0 or len(splited_cur_col[1]) == 0 ):
                mesg = "error: add-columns size 0"
                break
    if ( mesg != None ):
        if (cb_false != None):
            return False, cb_false(mesg)
        return False, mesg
    return True, mesg

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
    pd_read_args = { 'filepath_or_buffer': csvfile
            ,'encoding': "utf-8", 'delimiter': delimiter
            ,'low_memory': False }

    # drop function which is used in removing or deleting rows or columns from the CSV files
    if ((LooseVersion(pd.__version__) <= LooseVersion("1.4.0")) == True):
        data = pd.read_csv(**pd_read_args)
    else:
        data = pd.read_csv(**pd_read_args
                       , on_bad_lines='skip'
                       , encoding_errors='ignore'
                       )
    if ( data.empty == True ):
        data = None
        print(" \n----- Data is emtry -----\n ")
        sys.exit()
    print(f"Describe CSV:\n {data.describe(include='all')} \n")
    if ( add_timestamp != None ):
        if ( add_timestamp == "{isoformat}" ):
            # astimezone() => + "+01:00"
            data['@timestamp']=datetime.today().astimezone().isoformat()
        else:
            data['@timestamp']=add_timestamp
    if ( len(add_columns) > 0 ):
        for cur_add_column in add_columns:
            splited_cur_col = cur_add_column.split(":")
            data[splited_cur_col[0]] = splited_cur_col[1]

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
