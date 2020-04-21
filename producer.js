const {Kafka} = require("kafkajs");
const msg = process.argv[2];

const run = async () => {
    try {
        const kafka = new Kafka({
            "clientId": "kafkaapp",
            "brokers": ["youripaddress:9092"]
        });

        const producer = kafka.producer();
        await producer.connect();
        console.log('connected');

        const partition = msg[0].toLowerCase() < "n" ? 0 : 1;

        const result = await producer.send({
            "topic": "Users",
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