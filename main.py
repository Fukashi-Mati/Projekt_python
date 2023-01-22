import numpy as np
from pyod.models.knn import KNN
import pandas as pd
from kafka import KafkaConsumer


# Pobranie danych uczących z pliku
def getData():
    f = open("test.txt", "r")
    dane =[]
    for i in f:
        dane.append(float(i))
    f.close()
    return dane


# Uczenie sztucznei inteligencji
def learning(dane, pomiar):
    # Ustawienie danych testowych w zbiór mozliwy do dostarczenia do nauki sztucznej inteligencji
    d = {'temp': dane, 'class': 0}
    learning_data = pd.DataFrame(data=d)

    # Normalizacja danych uczących
    x = learning_data['temp'].values.reshape(-1, 1)

    # Określenie parametrów uczenia metodą najbliższych sąsiadów
    clf = KNN(contamination=0.03, n_neighbors=5)

    # Uczenie sztucznej inteligencji na podstawie zbioru testowego
    clf.fit(x)

    # Przypisanie danych z pomiaru do tablicy
    X_test = np.array([[pomiar]])

    # Zwrócenie wyniku predykcji
    return clf.predict(X_test)


# Pobranie danych uczących
data = getData()
# Pętla realizująca pobranie przetworzenie danych
while True:
    # Połączenie się z serwerem Kafki
    bootstrap_servers = ['192.168.0.103:9092']
    topicName = 'temp_fin2'
    consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers)
    # Przetworzenie pobranych wiadomości
    for i in consumer:
        # Zamiana wiadomości z binarnej na tekstową oraz usunięcie zbędznych znaków
        x = str(i.value)
        xsplit = x.split('--')
        temp = xsplit[1][:-1]
        # Przekazanie temperatury do sprawdzenia, w przypadku wykrycia anomalii program wyświetli ją na ekranie
        if learning(data, float(temp[1])) == [1]:
            print("Wykrto anomalię temperaturu o godzinie: ", xsplit[0], "temperatura: ", xsplit[1])
