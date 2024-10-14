const {Kafka} = require("kafkajs");
const msg = process.argv[2];

const run = async () => {
    try {
        const kafka = new Kafka({
            "clientId": "1001",
            "brokers": ["localhost:9092"]
        });

        const producer = kafka.producer();
        await producer.connect();
        console.log('connected');
        console.log(msg);

        const partition = 0;

        const result = await producer.send({
            "topic": "firsttopic",
            "messages": [{"value": msg, "partition": partition}]
        })

        console.log(`Sent Successfully - ${JSON.stringify(result)}`);

        await producer.disconnect();

    } catch (ex) {
        console.error(ex);
    } finally {
        process.exit(0)
    }
}

run();