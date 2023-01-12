import duckdb

def removeSuffix():
    # Connect to most recent database
    con = duckdb.connect(database="dataManagementBackbone/data/trusted/crimesPrices.db")

    tables = con.execute("SHOW TABLES").df()

    if len(tables['name']) != 0:
        # Import tables to dataframes
        crimesDF = con.execute("SELECT * FROM crimes").df()

        # Remove suffix
        crimesDF['LSOA name'] = crimesDF['LSOA name'].str[:-4].str.strip()

        # Update tables with the modified data
        con.execute('CREATE OR REPLACE TABLE crimes AS SELECT * FROM crimesDF')

    con.close()