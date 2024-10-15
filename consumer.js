const {Kafka} = require("kafkajs");
const Connection = require("rabbitmq-client").Connection;
const rabbit = new Connection('amqp://guest:guest@localhost:5672')

const rabbitConnect = () => {
    rabbit.on('error', (err) => {
        console.log('RabbitMQ connection error', err)
      })
      rabbit.on('connection', () => {
        console.log('Connection successfully (re)established')
      })
}

let pub;

const rabbitConsume = () => {
    pub = rabbit.createPublisher({
        // Enable publish confirmations, similar to consumer acknowledgements
        confirm: true,
        // Enable retries
        maxAttempts: 2,
      });
    const sub = rabbit.createConsumer({
        queue: 'msg-analysis-processor-queue',
        queueOptions: {durable: false},
        // handle 2 messages at a time
        qos: {prefetchCount: 1}
      }, (msg) => {
        const analysisMessage = JSON.stringify({
            message: msg.body.message,
            insertDate: '10/12/2024'
        });
    
        fetch('http://localhost:8010/predict/spamnospam', {
            method: 'POST',
            body: analysisMessage,
            headers: {
                'Content-type': 'application/json',
            }
        })
        .then(res => res.json())
        .then(json => pub.send(
            'msg-analysis-receiver-queue', {
                ...msg.body,
                'spamOrNot': json.spamOrNot,
                'processorId': msg.consumerTag,
                'processorQueue': msg.routingKey,
                'processorTime': new Date().toDateString()
            }
        ))
        .catch (err => console.log(err))
      });
}

const run = async () => {
    try {
        const kafka = new Kafka({
            "clientId": "1001",
            "brokers": ["localhost:9092"]
        });

        const consumer = kafka.consumer({"groupId": "test"});
        await consumer.connect();
        console.log('connected');

        await consumer.subscribe({"topic": "msganalysistopic", "fromBeginning": true});

        await consumer.run({
            "eachMessage": async (result) => {
                const message = {
                    'kafkaTopic': result.topic,
                    'kafkaPartition': result.partition,
                    'kafkaOffset': result.message.offset,
                    'message': result.message.value.toString().replaceAll("\\", "").replaceAll("\"", "")
                }
                pub.send('msg-analysis-processor-queue', message);
            }
        })

       
    } catch (ex) {
        console.error(ex);
    } finally {
    }
}
rabbitConnect();
rabbitConsume();
run();
