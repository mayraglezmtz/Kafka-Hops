
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'compu1-producer',
  brokers: ['10.25.239.108:9092'] // IP del broker remoto
});

const producer = kafka.producer();

async function enviar() {

    await producer.connect();

    const mensaje = {
        mensaje: "Hola desde PC1",
        timestamp: Date.now()
    };

    const result = await producer.send({
    topic: 'compu1_a_compu2',
    messages: [
        {
            value: JSON.stringify(mensaje)
        }
    ]
    });

    console.log(result);

    console.log("Mensaje enviado");
    console.log(mensaje);

    await producer.disconnect();
}

enviar();