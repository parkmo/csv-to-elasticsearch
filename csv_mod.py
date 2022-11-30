# csv_mod.py
import pandas as pd
import sys, os
from datetime import datetime
from distutils.version import LooseVersion#, StrictVersion

csvfile = ''
delimiter = ';'
delete_columns = []
data = None
dataFrameChanged = False
add_timestamp = None
add_columns = [ ]
rename_columns_dict = { }

def validate_arguments(add_columns, cb_false = None):
    mesg = None
    if ( len(add_columns) > 0 ):
        for cur_add_column in add_columns:
            splited_cur_col = cur_add_column.split(":")
            if ( len(splited_cur_col) < 2 ):
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

def csv_setup(Incsvfile, Indelimiter, Add_timestamp, Add_columns, Rename_columns):
    global csvfile
    csvfile = Incsvfile

    global delimiter
    delimiter = Indelimiter
    global add_timestamp
    add_timestamp = Add_timestamp
    global add_columns
    add_columns = Add_columns
    global rename_columns_dict
    rename_columns_dict = makeRenameDict(Rename_columns)
    g_logger.info(" \n----- CSV-Setup -----\n ")
    g_logger.info("CSV-File %s with delimiter %s'" % (csvfile, delimiter))

def load():
    g_logger.info(" \n----- LOAD AND CHECK CSV -----\n ")
    global csvfile
    global delimiter
    global data
    global add_timestamp
    global add_columns
    pd_read_args = { 'filepath_or_buffer': csvfile
            ,'encoding': "utf-8", 'delimiter': delimiter
            ,'low_memory': False
            ,'keep_default_na': ""
            }

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
        g_logger.critical(" \n----- Data is emtry -----\n ")
        sys.exit()
    g_logger.info(f"Describe CSV:\n {data.describe(include='all')} \n")
    if ( add_timestamp != None ):
        if ( add_timestamp == "{isoformat}" ):
            # astimezone() => + "+01:00"
            data['@timestamp']=datetime.today().astimezone().isoformat()
        else:
            data['@timestamp']=add_timestamp
    if ( len(add_columns) > 0 ):
        for cur_add_column in add_columns:
            splited_cur_col = cur_add_column.split(":")
            key = splited_cur_col[0]
            value = ":".join(splited_cur_col[1:])
            data[key] = value

def makeRenameDict(lst_columns):
    dict_ret = { }
    if ( len(lst_columns) > 0 ):
        for cur_column in lst_columns:
            splited_cur_col = cur_column.split(":")
            key = splited_cur_col[0]
            value = ":".join(splited_cur_col[1:])
            dict_ret[key] = value
    return dict_ret


def get_dict():
    global data
    return data.to_dict(orient='records')

def save(optype):
    g_logger.info(" \n----- SAVE CSV -----\n ")
    global csvfile
    global data
    global dataFrameChanged 

    if dataFrameChanged is True:
        g_logger.info(f"Result - first Rows: \n {data.head(2)} \n")
        if ( optype == 'new' ):
            csvfile = str(csvfile.name)
            pname, fname = os.path.split(csvfile)
            if (pname == "" ):
                pname = "."
            csvfile = pname + '/mod_' + fname
        elif ( optype == 'update' ):
            csvfile = str(csvfile.name)
        else:
            g_logger.error(f" \n----- CSV-Save type Error [{optype}] -----\n ")
            return
        pd.DataFrame.to_csv(data, csvfile, index = False, sep=delimiter, encoding='utf-8')

    g_logger.debug(f"Describe CSV:\n {data.describe(include='all')} \n")

def modifiy(deleteColumns, idColumn):
    global data
    global dataFrameChanged 
    global rename_columns_dict
    g_logger.info(" \n----- MODIFY AND CHECK CSV -----\n ")
    g_logger.info("Columns to ignore %s'" % (deleteColumns))
    g_logger.info("Column for id %s'" % (idColumn))
    g_logger.debug(f"Describe CSV:\n {data.describe(include='all')} \n")
    if ( len(rename_columns_dict) > 0 ):
        data.rename(columns = rename_columns_dict, inplace = True)
        dataFrameChanged = True

    if idColumn is not None:
        data.set_index(idColumn, inplace=True)
        data['_id'] = data.index
        dataFrameChanged = True
    else:
        data.index.name = 'line'

    g_logger.info(f"First Rows: \n {data.head(2)} \n")

    ignoreColumns = 0
    for column in deleteColumns:
        data.drop(column, inplace=True, axis=1, errors='ignore')
        ignoreColumns += 1
    if ignoreColumns > 0:
        g_logger.info(f"dropped {ignoreColumns} columns to ignore")
        dataFrameChanged = True
