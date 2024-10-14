const WebSocket = require("ws");
const Connection = require("rabbitmq-client").Connection;
const {Kafka} = require("kafkajs");

// Initialize:
const rabbit = new Connection('amqp://guest:guest@localhost:5672')
rabbit.on('error', (err) => {
  console.log('RabbitMQ connection error', err)
})
rabbit.on('connection', () => {
  console.log('Connection successfully (re)established')
})

const PORT = 3000;
const DEFAULT_INTERVAL = 10000;

// Create a WebSocket server
const wss = new WebSocket.Server({ port: PORT });
let webs = null;

wss.on("connection", (ws) => {
   console.log("Client connected");
   webs = ws;
   //Send random numbers at the specified interval
   // const generateRandomNumbers = () => {
   //    if (ws.readyState === WebSocket.OPEN) {
   //       console.log(messages.length);
   //       ws.send(1, (err) => {
   //          console.log(err);
   //       });
   //    }
   // };

   // // Default interval is 1 second
   //let timer = setInterval(generateRandomNumbers, DEFAULT_INTERVAL);

   ws.on("message", async (message) => {
      try {
         const kafka = new Kafka({
             "clientId": "1001",
             "brokers": ["localhost:9092"]
         });
      
         const producer = kafka.producer();
         await producer.connect();
         console.log('connected');
         console.log(message.toString());
      
         const partition = 0;
      
         const result = await producer.send({
             "topic": "firsttopic",
             "messages": [{"value": message, "partition": partition}]
         })
      
         console.log(`Sent Successfully - ${JSON.stringify(result)}`);
      
         await producer.disconnect();
      
      } catch (ex) {
         console.error(ex);
      }
   });

   ws.on("close", () => {
      console.log("Client disconnected");
      // clearInterval(timer);
   });
});

console.log(`WebSocket server listening on port ${PORT}`);

const sub = rabbit.createConsumer({
   queue: 'secondQueue',
   queueOptions: {durable: false},
   // handle 2 messages at a time
   qos: {prefetchCount: 1}
 }, (msg) => {
   console.log(msg);
   const result = {
      ...msg.body,
      rabbitId: msg.consumerTag,
      rabbitQueue: msg.routingKey,
      rabbitTime: new Date(msg.timestamp).toLocaleDateString()
   };
   console.log(result);
   if (webs.readyState === WebSocket.OPEN) {
      webs.send(JSON.stringify(result));
   }
 });

// Consume messages from a queue:
// See API docs for all options

