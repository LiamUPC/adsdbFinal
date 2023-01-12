import duckdb
import pandas as pd

def formattedToTrusted(sources):
    # Connect to formatted database
    conForm = duckdb.connect(database='dataManagementBackbone/data/formatted/crimesPrices.db')


    # Connect to trusted database
    conTrus = duckdb.connect(database='dataManagementBackbone/data/trusted/crimesPrices.db')

    # Delete any existing tables
    tables = conTrus.execute("SHOW TABLES").df()
    for table in tables['name']:
        conTrus.execute("DROP TABLE " + table)
    
    for source in sources:
        temp = pd.DataFrame()
        with open("dataManagementBackbone/data/formatted/"+source+".txt") as f:
            tables = f.readlines()
        
        # If we have new tables to process
        if len(tables) > 0:
            # Read table names files
            for table in tables:
                temp = pd.concat([temp, conForm.table(table.strip()).to_df()])

            # Create a new table with sourceName without "Tables"
            newTable = source[0:-6]
            conTrus.execute('CREATE OR REPLACE TABLE {0} AS SELECT * FROM temp'.format(newTable))

    conForm.close()
    conTrus.close()