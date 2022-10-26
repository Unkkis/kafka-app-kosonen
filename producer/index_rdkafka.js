//import Kafka from 'node-rdkafka';         //new ES6/ES2015 modules way
const Kafka = require('node-rdkafka');      //Old CommonJS way
console.log("*** Producer starts... ***");

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, { topic: 'task' });

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from + 1))) + from;
}

function queueMessage() {
    const o1 = randomizeIntegerBetween(1, 2);
    const o2 = randomizeIntegerBetween(1, 2);
    const o3 = randomizeIntegerBetween(2, 3);


    const success = stream.write(Buffer.from(
        `Is this correct ${o1} + ${o2} = ${o3}?`
    ));
    /*
        const success = stream.write(Buffer.from(
            `{"o1":${o1}, "o2":${o2}, "o3":${o3}}`
        ));
    */

    if (success) {
        console.log('Message succesfully to stream');
    } else {
        console.log('Problem writing to stream..');
    }
}

//setInterval needs to be given a call-back function
//that environment can call later => you'll have to give a function
//definition / function object, cannot just straight call queueMessage
setInterval(() => {
    queueMessage();
}, 2500);

const consumer = Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092'
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('Consumer in producer node ready...');
    consumer.subscribe(['answer']);
    consumer.consume();
}).on('data', (data) => {
    console.log(`received message: ${data.value}`)

});