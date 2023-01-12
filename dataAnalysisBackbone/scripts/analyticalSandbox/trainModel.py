import duckdb
import pickle
from sklearn.linear_model import LinearRegression

def trainModel():

    con = duckdb.connect("dataAnalysisBackbone/data/analyticalSandbox/crimesPrices.db")

    X_train = con.execute("SELECT * FROM X_train").df()
    y_train = con.execute("SELECT * FROM y_train").df()
    
    con.close()

    # Fit model with training set
    model = LinearRegression().fit(X_train, y_train)
    pickle.dump(model, open('trainedModel.sav', 'wb'))