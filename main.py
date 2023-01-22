import numpy as np
from pyod.models.knn import KNN
import pandas as pd
from kafka import KafkaConsumer

def getData():
    f = open("test.txt", "r")
    dane =[]
    for i in f:
        dane.append(float(i))
    f.close()
    return dane


# Uczenie sztucznei inteligencji
def learning(dane, pomiar):
    d = {'temp': dane, 'class': 0}
    learning_data = pd.DataFrame(data=d)

    # Normalizacja danych uczących
    x = learning_data['temp'].values.reshape(-1, 1)

    #
    clf = KNN(contamination=0.03, n_neighbors=5)
    clf.fit(x)

    X_test = np.array([[pomiar]])
    return clf.predict(X_test)


data = getData()
while True:
    bootstrap_servers = ['192.168.0.103:9092']
    topicName = 'temp_fin2'
    consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers)
    data = []
    for i in consumer:
        x = str(i.value)
        xsplit = x.split('--')
        temp = xsplit[1][:-1]
        print(temp)
        if learning(data, float(temp[1])) == [1]:
            print("Wykrto anomalię temperaturu o godzinie: ", xsplit[0])

