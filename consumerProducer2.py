from kafka import KafkaConsumer, KafkaProducer
import json

print("[INIT] Creando consumidor...")
consumer = KafkaConsumer(
    "compu1_a_compu2",  # tópico de entrada
    bootstrap_servers="10.25.239.108:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("[INIT] Creando productor...")
try:
    producer = KafkaProducer(
        bootstrap_servers="10.25.233.63:9092",  # broker de salida
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    print("[READY] Productor conectado correctamente")
except Exception as e:
    print("[ERROR] No se pudo conectar al broker de producción:", e)
    raise

print("[READY] Esperando mensajes en loop...")

try:
    for msg in consumer:
        print("[EVENT] Mensaje recibido en tópico 'compu1_a_compu2'")
        print("[DEBUG] Raw:", msg)
        datos = msg.value
        print("[INFO] Decodificado:", datos)

        try:
            print("[ACTION] Enviando mensaje a tópico 'compu2_a_compu3'...")
            producer.send("compu2_a_compu3", value=datos).get(timeout=10)
            print("[SUCCESS] Mensaje reenviado correctamente a PC3")
        except Exception as e:
            print("[ERROR] Fallo al enviar mensaje a PC3:", e)

except Exception as e:
    print("[ERROR] Ocurrió un problema en el loop principal:", e)

finally:
    print("[CLOSE] Cerrando conexiones...")
    try:
        consumer.close()
        print("[CLOSE] Consumidor cerrado")
    except Exception as e:
        print("[WARN] Error al cerrar consumidor:", e)

    try:
        producer.close()
        print("[CLOSE] Productor cerrado")
    except Exception as e:
        print("[WARN] Error al cerrar productor:", e)

    print("[EXIT] Programa terminado")