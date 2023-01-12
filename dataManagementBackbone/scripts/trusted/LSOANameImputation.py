import duckdb

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.neighbors import KNeighborsClassifier

def lsoaNameImputation():
     # Connect to most recent database
     con = duckdb.connect(database="dataManagementBackbone/data/trusted/crimesPrices.db")

     
     tables = con.execute("SHOW TABLES").df()

     if len(tables['name']) != 0:

          # Import tables to dataframes
          crimesDF = con.execute("SELECT * FROM crimes").df()

          # Imputation of LSOA name by using KNN
          datos = crimesDF[['Longitude','Latitude','LSOA name']]

          # Get rows with NAs for LSOA name
          nanRows = datos[datos['LSOA name'].isnull()]
          
          if len(nanRows) != 0:

               # Dataframe without NAs
               non_nan = datos.dropna()

               # Predictors
               X = non_nan[['Longitude','Latitude']].values
               # Target
               y = non_nan['LSOA name'].values
               
               # Split data into training and testing sets
               X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
               scaler = MinMaxScaler()
               X_train = scaler.fit_transform(X_train)
               X_test = scaler.transform(X_test)

               # Specify number of neighbours
               n_neighbors = 7

               # Fit KNN
               knn = KNeighborsClassifier(n_neighbors)
               knn.fit(X_train, y_train)
               # print('Accuracy of K-NN classifier on training set: {:.2f}'
               #      .format(knn.score(X_train, y_train)))
               # print('Accuracy of K-NN classifier on test set: {:.2f}'
               #      .format(knn.score(X_test, y_test)))

               # Test KNN
               # pred = knn.predict(X_test)
               # print(confusion_matrix(y_test, pred))
               # print(classification_report(y_test, pred))

               # Keep the predictors for the NA rows
               nanRows = nanRows[['Longitude', 'Latitude']]
               # Predict the target
               imp = knn.predict(nanRows)
               # Replace NAs with predicted values
               crimesDF.loc[crimesDF['LSOA name'].isnull(), 'LSOA name'] = imp

               con.execute('CREATE OR REPLACE TABLE crimes AS SELECT * FROM crimesDF')

     con.close()