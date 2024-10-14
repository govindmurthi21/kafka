const {Kafka} = require("kafkajs");
const Connection = require("rabbitmq-client").Connection;
const rabbit = new Connection('amqp://guest:guest@localhost:5672')
rabbit.on('error', (err) => {
  console.log('RabbitMQ connection error', err)
})
rabbit.on('connection', () => {
  console.log('Connection successfully (re)established')
})
const pub = rabbit.createPublisher({
    // Enable publish confirmations, similar to consumer acknowledgements
    confirm: true,
    // Enable retries
    maxAttempts: 2,
  })

const run = async () => {
    try {
        const kafka = new Kafka({
            "clientId": "1001",
            "brokers": ["localhost:9092"]
        });

        const consumer = kafka.consumer({"groupId": "test"});
        await consumer.connect();
        console.log('connected');

        await consumer.subscribe({"topic": "firsttopic", "fromBeginning": true});

        await consumer.run({
            "eachMessage": async (result) => {
                const message = JSON.stringify({
                    message: result.message.value.toString().replaceAll("\\", "").replaceAll("\"", ""),
                    insertDate: '10/12/2024'
                });

                fetch('http://localhost:8010/predict/spamnospam', {
                    method: 'POST',
                    body: message,
                    headers: {
                        'Content-type': 'application/json',
                    }
                })
                .then(res => res.json())
                .then(json => pub.send(
                    'secondQueue', {
                        'message': json.message,
                        'spamOrNot': json.spamOrNot,
                        'kafkaTopic': result.topic,
                        'kafkaPartition': result.partition,
                        'kafkaOffset': result.message.offset
                    }
                ))
                .catch (err => console.log(err))
            }
        })

       
    } catch (ex) {
        console.error(ex);
    } finally {
    }
}

run();