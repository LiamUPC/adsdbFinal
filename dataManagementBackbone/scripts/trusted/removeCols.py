import duckdb

def removeCols():
    # Connect to trusted database
    con = duckdb.connect(database="dataManagementBackbone/data/trusted/crimesPrices.db")

    tables = con.execute("SHOW TABLES").df()

    if len(tables['name']) != 0:
        # Import tables to dataframes
        crimesDF = con.execute("SELECT * FROM crimes").df()
        pricesDF = con.execute("SELECT * FROM prices").df()

        # Drop redundant columns from crimes
        crimesDF = crimesDF.drop(['LSOA code', 'Crime ID', 'Context'], axis=1)

        # Drop redundant columns from prices
        pricesDF = pricesDF.drop(['LatestData', 'DurationFrom', 'DurationTo', 'GeoURI', 'OrganisationURI', 'GeoCode', 'PublishedDate'], axis=1)

        # Update tables with the modified data
        con.execute('CREATE OR REPLACE TABLE crimes AS SELECT * FROM crimesDF')
        con.execute('CREATE OR REPLACE TABLE prices AS SELECT * FROM pricesDF')

    con.close()