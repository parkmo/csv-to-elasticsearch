# csv_mod.py
import pandas as pd

csvfile = ''
delimiter = ';'
delete_columns = []


def csv_setup(Incsvfile, Indelimiter):
    global csvfile
    csvfile = Incsvfile

    global delimiter
    delimiter = Indelimiter

    print(" \n----- CSV-Setup -----\n ")
    print("CSV-File %s with delimiter %s'" % (csvfile, delimiter))


def modifiy(deleteColumns, idColumn):
    print(" \n----- MODIFY AND CHECK CSV -----\n ")
    print("Columns to ignore %s'" % (deleteColumns))
    print("Column for id %s'" % (idColumn))
    global csvfile
    global delimiter
    dataFrameChanged = False

    # drop function which is used in removing or deleting rows or columns from the CSV files
    data = pd.read_csv(csvfile,
                       encoding="utf-8",
                       delimiter=delimiter,
                       #index_col=0,
                       on_bad_lines='skip',
                       encoding_errors='ignore',
                       low_memory=False)

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

    if dataFrameChanged is True:
        print(f"Result - first Rows: \n {data.head(2)} \n")
        csvfile = 'mod_' + str(csvfile.name)
        pd.DataFrame.to_csv(data, csvfile, sep=delimiter, encoding='utf-8')

    print(f"Describe CSV:\n {data.describe(include='all')} \n")