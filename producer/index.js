const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

console.log("*** Producer starts... ***");

const kafka = new Kafka({
    clientId: 'producer',
    brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test' })


function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from + 1))) + from;
}

function queueMessage() {
    const o1 = randomizeIntegerBetween(1, 2);
    const o2 = randomizeIntegerBetween(1, 2);
    const o3 = randomizeIntegerBetween(2, 3);

    const uuid = uuidv4();
    const id = uuid.substring(24)

    let obj = { o1, o2, o3 };
    let objJSON = JSON.stringify(obj);

    return producer
        .send({
            topic: 'task',
            messages: [
                {
                    key: `${id}`,
                    value: `{"o1":${o1}, "o2":${o2}, "o3":${o3}}`
                }
            ]
        })
        .then(console.log(`Task ID: ${id} succesfully to stream`))
}

const run = async () => {
    await producer.connect()
    setInterval(queueMessage, 2500)

    await consumer.connect()
    await consumer.subscribe({ topic: 'answer' })
    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            console.log(`Topic: ${topic} Id: ${message.key} Message: ${message.value}`)
        }
    })

}

run().catch(console.error)