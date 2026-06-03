from kafka import KafkaConsumer
import json
import time


while True:
    consumer = KafkaConsumer(
        'compu3_a_compu1',
        bootstrap_servers='10.25.239.108:9092', #ip de compu 2
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )   

    print("Esperando respuesta...")

    for msg in consumer:

        datos = msg.value

        timestamp_original = datos["timestamp"]

        tiempo_actual = int(time.time() * 1000)

        latencia = tiempo_actual - timestamp_original

        print("\nMensaje recibido:")
        print(datos)

        print(f"Tiempo ida y vuelta: {latencia} ms")

