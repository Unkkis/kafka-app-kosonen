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
    let answer = `${data}`
    let n1 = parseInt(answer.slice(16, 17))
    let n2 = parseInt(answer.slice(20, 21))
    let n3 = parseInt(answer.slice(24, 25))

    let answerCalc = n1 + n2
    let answerBack = "NOT RIGHT"
    if (answerCalc === n3) {
        answerBack = "RIGHT"
    }

    const success = stream.write(Buffer.from(
        `Your calculation (${n1}+${n2}=${n3}) is ${answerBack} `
    ));

    if (success) {
        console.log('Message succesfully to stream');
    } else {
        console.log('Problem writing to stream..');
    }
}