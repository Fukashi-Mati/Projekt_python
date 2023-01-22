from pykafka import KafkaClient
from sense_emu import SenseHat
from datetime import date
import time
import sys
def getAndSendTemperature(sens):

    temperature = sens.temp
    KAFKA_HOST = "192.168.0.103:9092"
    client = KafkaClient(hosts=KAFKA_HOST)
    topic = client.topics["temp_fin"]
    datawrite = date.now()
    with topic.get_sync_producer() as producer:
        message = str(datawrite) + '--' + str(temperature)
        encoded_message = message.encode("utf-8")
        producer.produce(encoded_message)


sense = SenseHat()
num = int(sys.argv[1])
num2 = 0
while True:
    if num >= num2:

        num2 += 1
    else:
        getAndSendTemperature(sense)
        num2 = 0
    time.sleep(1)
