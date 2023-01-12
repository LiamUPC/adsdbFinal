import duckdb

def commonAreas():
    # Connect to trusted database
    con = duckdb.connect(database="dataManagementBackbone/data/trusted/crimesPrices.db")

    tables = con.execute("SHOW TABLES").df()

    if len(tables['name']) != 0:
        # Import tables to dataframes
        crimesDF = con.execute("SELECT * FROM crimes").df()
        pricesDF = con.execute("SELECT * FROM prices").df()

        crimesAreas = crimesDF["LSOA name"].unique()
        pricesAreas = pricesDF["GeoName"].unique()

        commonAreas = list(set(crimesAreas) & set(pricesAreas))

        # Remove cases of areas that aren't in both datasets
        crimesDF = crimesDF[crimesDF["LSOA name"].isin(commonAreas)]
        pricesDF = pricesDF[pricesDF["GeoName"].isin(commonAreas)]

        # Update tables with the modified data
        con.execute('CREATE OR REPLACE TABLE crimes AS SELECT * FROM crimesDF')
        con.execute('CREATE OR REPLACE TABLE prices AS SELECT * FROM pricesDF')

    con.close()