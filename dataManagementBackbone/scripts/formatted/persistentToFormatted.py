import duckdb
import glob
import os
import re
from datetime import datetime as dt
def persToForm(sources):

    # Create a new database in formatted
    con = duckdb.connect(database="dataManagementBackbone/data/formatted/crimesPrices.db")

    # Delete any existing tables
    tables = con.execute("SHOW TABLES").df()
    for table in tables['name']:
        con.execute("DROP TABLE " + table)

    # Regex to remove non-alphanumeric characters 
    regex = re.compile(r'\W+')

    for source in sources:
        # Create a table for each version of data source
        cFiles = glob.glob("dataManagementBackbone/data/landing/persistent/" + source + "/*.csv")
        tables = []
        
        # Get last timestamp of previously processed source files
        with open("dataManagementBackbone/data/formatted/" + source + "Time.txt") as f:
                lastTimestamp = f.readlines()

        for file in cFiles:
            #Get timestamp to check whether file has been processed
            timestamp = os.path.basename(file).split("t-")[1].split(".")[0]
            if len(lastTimestamp) == 0 or timestamp > lastTimestamp[0]:
                # Get file base name for table name
                fileBase = os.path.basename(file).split(".")[0]
                # Remove non-alphanumeric characters
                fileBase = regex.sub('', fileBase)
                # Add tables name to tables
                tables.append(fileBase)
                # Save in a new table
                con.execute("CREATE OR REPLACE TABLE {0} AS SELECT * FROM read_csv_auto('{1}');".format(fileBase, file))

        # Log name of tables in souceTables.txt
        f = open("dataManagementBackbone/data/formatted/" + source + "Tables.txt", "w")
        f.writelines("\n".join(tables))
        f.close()

        # Log timestamp in sourceTime.txt
        f = open("dataManagementBackbone/data/formatted/" + source + "Time.txt", "w")
        f.writelines(dt.now().strftime("%Y-%m-%d-%H_%M_%S"))
        f.close()

    con.close()