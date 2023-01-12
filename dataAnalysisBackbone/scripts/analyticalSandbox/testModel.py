import duckdb
import pickle
import math
import numpy as np

def testModel():

    con = duckdb.connect("dataAnalysisBackbone/data/analyticalSandbox/crimesPrices.db")

    X_train = con.execute("SELECT * FROM X_train").df()
    y_train = con.execute("SELECT * FROM y_train").df()
    X_test  = con.execute("SELECT * FROM X_test").df()
    y_test  = con.execute("SELECT * FROM y_test").df()

    con.close()

    model = pickle.load(open('trainedModel.sav', 'rb'))
    print('R-squared train')
    print(model.score(X_train, y_train))

    print('\nR-squared test')
    print(model.score(X_test, y_test))

    print('\nPrediction error')
    print('\nMean Absolute Error')
    print(np.sum(abs(y_test - model.predict(X_test))) / len(y_test))
    print('\nRoot Mean Squared Error')
    print(math.sqrt(np.sum((y_test - model.predict(X_test))*(y_test - model.predict(X_test))) / len(y_test)))