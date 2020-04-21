const {Kafka} = require("kafkajs");

const run = async () => {
    try {
        const kafka = new Kafka({
            "clientId": "kafkaapp",
            "brokers": ["youripaddress:9092"]
        });

        const consumer = kafka.consumer({"groupId": "test"});
        await consumer.connect();
        console.log('connected');

        await consumer.subscribe({"topic": "Users", "fromBeginning": true});

        await consumer.run({
            "eachMessage": async (result) => {
                console.log(`Consumed Successfully - ${result.message.value} on partition - ${result.partition}`);
            }
        })

       
    } catch (ex) {
        console.error(ex);
    } finally {
    }
}

run();