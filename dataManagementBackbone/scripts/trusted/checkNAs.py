import duckdb

def checkNAs():
    con = duckdb.connect("dataManagementBackbone/data/trusted/crimesPrices.db")

    tables = con.execute("SHOW TABLES").df()

    if len(tables['name']) != 0:
        # Import tables to dataframes
        crimesDF = con.execute("SELECT * FROM crimes").df()
        pricesDF = con.execute("SELECT * FROM prices").df()

        # Check if there any rows with NA values in crimes
        nanRows = crimesDF.isnull().values.any()
        if nanRows:
            answer = ''
            while answer not in ['y', 'n']:
                answer = input("\nThere are null values in crimes. Do you wish to remove these instances? ('y'/'n'): ")
            if answer == 'y':
                # Remove NA rows
                crimesDF.dropna(inplace=True)
                print("Rows with NAs in the crimes dataset were removed\n\n")

        # Check if there any rows with NA values in prices
        nanRows = pricesDF.isnull().values.any()
        if nanRows:
            answer = ''
            while answer not in ['y', 'n']:
                answer = input("\nThere are null values in prices. Do you wish to remove them? ('y'/'n'): ")
            if answer == 'y':
                # Remove NA rows
                pricesDF.dropna(inplace=True)
                print("Rows with NAs in the prices dataset were removed\n\n")