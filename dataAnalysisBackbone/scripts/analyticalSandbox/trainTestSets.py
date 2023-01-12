import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split

def trainTestSets():

    # Retrieving data
    con = duckdb.connect("dataManagementBackbone/data/exploitation/crimesPrices.db")
    df = con.execute("SELECT * FROM crimesPrices").df()

    # Target
    y = df.AveragePrice

    # Remove unnecessary columns for explanatory variables
    dropCols = [s for s in df.columns.to_list() if 'Lon' in s or 'Lat' in s] + ['AveragePrice', 'AveragePriceDetached', 'AveragePriceSemiDetached', 'AveragePriceTerraced', 'AveragePriceFlatOrMaisonette', 'Economically inactive', 'Employed', 'Unemployed']
    X = df.drop(dropCols, axis=1)

    # Normalise numeric using minmax scaling
    numCols = X.select_dtypes('number').columns.to_list()
    X[numCols] = (X[numCols]-X[numCols].min())/(X[numCols].max()-X[numCols].min())

    # Create dummy variables for District and Month
    X = X.join(pd.get_dummies(X['Month'])).iloc[:, 1:]
    X = X.join(pd.get_dummies(X['District'])).iloc[:, 1:]

    # Create training testing split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=0)
    X_train = pd.DataFrame(X_train)
    y_train = pd.DataFrame(y_train)
    X_test  = pd.DataFrame(X_test)
    y_test  = pd.DataFrame(y_test)

    con.close()

    con = duckdb.connect("dataAnalysisBackbone/data/analyticalSandbox/crimesPrices.db")

    # Delete any existing tables
    tables = con.execute("SHOW TABLES").df()
    for table in tables['name']:
        con.execute("DROP TABLE " + table)
    
    tables = ['X_train', 'X_test', 'y_train', 'y_test']
    for table in tables:
        con.execute("CREATE OR REPLACE TABLE {0} AS SELECT * FROM {0}".format(table))

    con.close()
