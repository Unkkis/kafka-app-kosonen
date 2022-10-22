//import Kafka from 'node-rdkafka';         //new ES6/ES2015 modules way
const Kafka = require('node-rdkafka');      //Old CommonJS way
console.log("*** Consumer starts... ***");

const consumer = Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092'
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('Consumer ready...');
    consumer.subscribe(['task']);
    consumer.consume();
}).on('data', (data) => {
    console.log(`received message: ${data.value}`)
    checkCalculation(data.value)
});

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, { topic: 'answer' });

function checkCalculation(data) {
    // let answer = `${data}`
    // let calculation = answer.slice(17, 25)
    //console.log(answer.slice(17, 25))

    const success = stream.write(Buffer.from(
        `Answer from consumer to producer is: `
    ));

    if (success) {
        console.log('Message succesfully to stream');
    } else {
        console.log('Problem writing to stream..');
    }
}