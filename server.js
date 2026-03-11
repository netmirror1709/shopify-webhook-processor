const express = require('express');
const amqp = require('amqplib');
const crypto = require('crypto');
require('dotenv').config();
const QUEUE_NAME = process.env.RABBITMQ_QUEUE;
const app = express();
const PORT = process.env.PORT || 3003;
let connection = null;
let channel = null;

app.use(express.json({
    verify: (req, res, buf) => {
        req.rawBody = buf;
    }
}));

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function connect(retries = 10, delay = 5000) {
    if (connection && channel) return channel;

    for (let i = 0; i < retries; i++) {
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
            console.error(`❌ Failed to connect to RabbitMQ (attempt ${i + 1}/${retries}):`, error.message);
            if (i === retries - 1) throw error;
            await sleep(delay);
        }
    }
}

async function publishMessage(message) {
    const channel = await connect();
    const msgBuffer = Buffer.from(JSON.stringify(message));
    return channel.sendToQueue(QUEUE_NAME, msgBuffer, {
        persistent: true
    });
}

function verifyShopifyWebhook(req) {
    const hmacHeader = req.headers['x-shopify-hmac-sha256'];
    const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
    if (!hmacHeader || !secret) {
        return false;
    }

    const hash = crypto
        .createHmac('sha256', secret)
        .update(req.rawBody, 'utf8')
        .digest('base64');

    return hash === hmacHeader;
}

app.post('/webhook', async (req, res) => {
    try {
        if (!verifyShopifyWebhook(req)) {
            console.warn('⚠️ Unauthorized webhook request: Invalid signature');
            return res.status(401).send('Unauthorized');
        }

        const payload = {
            headers: req.headers,
            body: req.body
        };
        console.log(`📥 Received webhook for topic: ${req.headers['x-shopify-topic']} / ID: ${req.headers['x-shopify-webhook-id']}`);
        await publishMessage(payload);
        res.status(200).send('OK');
        console.log(`✅ Message queued successfully`);
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

module.exports = { startServer };