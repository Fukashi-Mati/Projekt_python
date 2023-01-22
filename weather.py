from pykafka import KafkaClient
from sense_emu import SenseHat
from datetime import date
import time
import sys

# Funkcja Pobierająca i wysyłająca temperaturę
def getAndSendTemperature(sens):
    # Pobranie temperatury oraz daty
    temperature = sens.temp
    datawrite = date.now()

    #Połączenie się z serwerm Kafki do którego wysyłamy dane
    KAFKA_HOST = "192.168.0.103:9092"
    client = KafkaClient(hosts=KAFKA_HOST)
    topic = client.topics["temp_fin"]

    # Utworzenie wiadomości z odczytanej temperatury i daty oraz wysyłanie danych
    with topic.get_sync_producer() as producer:
        message = str(datawrite) + '--' + str(temperature)
        encoded_message = message.encode("utf-8")
        producer.produce(encoded_message)


# Inicjacja czujników z symulatora
sense = SenseHat()
# Pobranie wartości próbkowania
num = int(sys.argv[1])
num2 = 0
# Pętla realizująca pobór temperatury i realizująca próbkowanie
while True:
    if num >= num2:
        num2 += 1
    else:
        getAndSendTemperature(sense)
        num2 = 0
    time.sleep(1)
