const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: 'consumer',
    brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'kafka' })

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: 'task', fromBeginning: true })
    let answer = ''
    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const obj = JSON.parse(message.value);
            answer = `Calculation ${obj.o1}+${obj.o2}=${obj.o3} is `
            if (obj.o1 + obj.o2 === obj.o3) {
                answer += "true.";
            } else {
                answer += "FALSE!";
            }
            console.log(`Topic: ${topic} Id: ${message.key} Message: ${answer}`)
            giveAnswer(answer)
        },

    })
}

const giveAnswer = async (answer) => {
    const uuid = uuidv4();
    const id = uuid.substring(24)

    await producer.connect()
    await producer.send({
        topic: 'answer',
        messages: [
            {
                key: `${id}`,
                value: `${answer}`
            }
        ]
    })
        .then(console.log(`Answer ${id} sent successfully to stream`))
}

run().catch(console.error)