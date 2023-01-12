import duckdb

def lastOutCatImputation():
    # Connect to trusted database
    con = duckdb.connect(database="dataManagementBackbone/data/trusted/crimesPrices.db")

    tables = con.execute("SHOW TABLES").df()

    if len(tables['name']) != 0:
        # Import tables to dataframes
        crimesDF = con.execute("SELECT * FROM crimes").df()

        # Impute NaN to "Unknown"
        crimesDF['Last outcome category'].fillna('Unknown', inplace=True)

        # Update tables with the modified data
        con.execute('CREATE OR REPLACE TABLE crimes AS SELECT * FROM crimesDF')

    con.close()