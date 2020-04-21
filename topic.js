const {Kafka} = require("kafkajs");

const run = async () => {
    try {
        const kafka = new Kafka({
            "clientId": "kafkaapp",
            "brokers": ["youripaddress:9092"]
        });

        const admin = kafka.admin();
        await admin.connect();
        console.log('connected');

        await admin.createTopics({
            "topics": [{"topic": "Users", "numPartitions": 2}]
        });

        console.log("Created Successfully");

        await admin.disconnect();

    } catch (ex) {
        console.error(ex);
    } finally {
        process.exit(0)
    }
}

run();