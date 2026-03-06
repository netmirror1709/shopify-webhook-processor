const express = require('express');
const amqp = require('amqplib');
require('dotenv').config();
const QUEUE_NAME = process.env.RABBITMQ_QUEUE;
const app = express();
const PORT = process.env.PORT;
let connection = null;
let channel = null;

app.use(express.json());

async function connect() {
    if (connection && channel) return channel;

    try {
        connection = await amqp.connect(process.env.RABBITMQ_URL);
        channel = await connection.createChannel();
        
        await channel.assertQueue(QUEUE_NAME, { durable: true });
        
        connection.on('error', (err) => {
            console.error('RabbitMQ Connection Error:', err);
            connection = null;
            channel = null;
        });

        console.log('✅ Connected to RabbitMQ');
        return channel;
    } catch (error) {
        console.error('❌ Failed to connect to RabbitMQ:', error);
        throw error;
    }
}

async function publishMessage(message) {
    const channel = await connect();
    console.log(JSON.stringify(message));
    const msgBuffer = Buffer.from(JSON.stringify(message));
    return channel.sendToQueue(QUEUE_NAME, msgBuffer, { 
        persistent: true 
    });
}

app.post('/webhook', async (req, res) => {
    try {
        const payload = {
            headers: req.headers, 
            body: req.body
        };
        console.log(payload);
        await publishMessage(payload);
        res.status(200).send('OK');
        console.log(`Message queued successfully`);
    } catch (error) {
        console.error('Failed to process webhook:', error);
        res.status(500).send('Internal Server Error');
    }
});

app.get('/webhook', async (req, res) => {
    res.status(200).send('{"Status": "Healthy"}');
});

async function startServer() {
    try {
        await connect();
        app.listen(PORT, () => {
            console.log(`🚀 Server running on port ${PORT}`);
        });
    } catch (error) {
        console.error('❌ Failed to start server:', error);
        process.exit(1);
    }
}

module.exports = {startServer};