import duckdb

def removeDuplicates():
    # Connect to most recent database
    con = duckdb.connect(database="dataManagementBackbone/data/trusted/crimesPrices.db")

    tables = con.execute("SHOW TABLES").df()

    if len(tables['name']) != 0:
        # Import tables to dataframes
        crimesDF = con.execute("SELECT * FROM crimes").df()
        pricesDF = con.execute("SELECT * FROM prices").df()

        crimesDF = crimesDF.drop_duplicates(keep='first')
        pricesDF = pricesDF.drop_duplicates(keep='first')

        # Update tables with the modified data
        con.execute('CREATE OR REPLACE TABLE crimes AS SELECT * FROM crimesDF')
        con.execute('CREATE OR REPLACE TABLE prices AS SELECT * FROM pricesDF')

    con.close()