const amqp = require('amqplib');
const { Pool } = require('pg');
const axios = require('axios');
require('dotenv').config();

const QUEUE_NAME = process.env.RABBITMQ_QUEUE || 'shopify_webhooks_queue';
const PREFETCH_COUNT = parseInt(process.env.CONSUMER_PREFETCH_COUNT) || 1;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://syncuser:syncpass@postgres:5432/syncdb",
});

let connection = null;
let channel = null;
let isShuttingDown = false;

async function startConsumer() {
  console.log('🚀 Starting Shopify Inventory Updater...');

  await connect();
  await consumeMessage(async (message) => {
    if (isShuttingDown) {
      console.log('⚠️ Shutting down, skipping new messages');
      return;
    }
    
    const body = message.body || message;
    const lineItems = body.line_items || [];

    console.log(`📦 Processing order with ${lineItems.length} line items`);
    for (const item of lineItems) {
      console.log(`Variant ID: ${item.variant_id}, Quantity: ${item.quantity}`);
      await updateInventory(item.variant_id, item.quantity);
    }
    console.log("✅ Finished processing order line items");
  });

  console.log('✅ Consumer is running and listening for messages...');
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Received SIGINT, shutting down gracefully...');
  isShuttingDown = true;

  // Wait for current message processing to complete
  await new Promise(resolve => setTimeout(resolve, 5000));

  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
  isShuttingDown = true;
  await new Promise(resolve => setTimeout(resolve, 5000));
  process.exit(0);
});

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error);
});

process.on('unhandledRejection', (error) => {
  console.error('💥 Unhandled Rejection:', error);
});






const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function connect(retries = 10, delay = 5000) {
  if (connection && channel) return channel;

  for (let i = 0; i < retries; i++) {
    try {
      connection = await amqp.connect(process.env.RABBITMQ_URL);
      channel = await connection.createChannel();

      await channel.assertQueue(QUEUE_NAME, { durable: true });
      await channel.prefetch(PREFETCH_COUNT);

      connection.on('error', (err) => {
        console.error('❌ RabbitMQ Connection Error:', err);
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

async function consumeMessage(onMessage) {
  const ch = await connect();

  return ch.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    try {
      const content = JSON.parse(msg.content.toString());
      await onMessage(content);
      ch.ack(msg);
      console.log('✅ Message acknowledged');
    } catch (error) {
      console.error('❌ Error processing message:', error);

      // Reject and requeue for retry
      ch.nack(msg, false, true);
    }
  }, { noAck: false });
}

// Shopify API base
const SHOPIFY_API_VERSION = '2026-01';

// Adjust inventory via Shopify Admin API (GraphQL)
async function adjustShopifyInventory({
  inventoryItemId,
  locationId,
  delta,
  shopDomain,
  storeAccessKey
}) {
  const url = `https://${shopDomain}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

  const query = `
    mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
      inventoryAdjustQuantities(input: $input) {
        inventoryAdjustmentGroup {
          createdAt
          reason
          changes {
            name
            delta
          }
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const variables = {
    input: {
      reason: "CORRECTION",
      name: "available",
      changes: [
        {
          delta: delta,
          inventoryItemId: `gid://shopify/InventoryItem/${String(inventoryItemId).split('/').pop()}`,
          locationId: `gid://shopify/Location/${String(locationId).split('/').pop()}`
        }
      ]
    }
  };

  const response = await axios.post(
    url,
    {
      query,
      variables
    },
    {
      headers: {
        'X-Shopify-Access-Token': storeAccessKey,
        'Content-Type': 'application/json'
      },
      timeout: 10000
    }
  );

  if (response.data.errors) {
     throw new Error(JSON.stringify(response.data.errors));
  }
  
  const data = response.data && response.data.data;
  const userErrors = data && data.inventoryAdjustQuantities && data.inventoryAdjustQuantities.userErrors;
  if (userErrors && userErrors.length > 0) {
     throw new Error(JSON.stringify(userErrors));
  }

  return response.data;
}

async function updateInventory(variantId, delta) {
  try {
    // 4️⃣ Process each variant in a DB transaction with row locking
    const client = await pool.connect();

    try {
      await client.query('BEGIN');


      // 🔒 Fetch and LOCK all group rows for this variant
      const lockQuery = `
          SELECT 
            "groupId",
            "inventoryItemId",
            "locationId",
            "storeDomain",
            "storeAccessKey"
          FROM "InventoryGroupVariant"
          WHERE "groupId" = (SELECT "groupId" FROM "InventoryGroupVariant"
          WHERE "variantId" LIKE '%' || $1 || '%') AND "variantId" NOT LIKE '%' || $1 || '%'
          FOR UPDATE SKIP LOCKED
        `;

      const lockResult = await client.query(lockQuery, [variantId]);
      const groupRows = lockResult.rows;

      if (groupRows.length === 0) {
        console.warn(`⚠️ No inventory group found for variant_id: ${variantId}`);
        return;
      }

      console.log(`🔒 Locked ${groupRows.length} rows for variant_id: ${variantId}, groupId: ${groupRows[0].groupId}`);

      // 🔄 Adjust inventory on Shopify for EACH locked row
      for (const row of groupRows) {
        try {
          const result = await adjustShopifyInventory({
            inventoryItemId: String(row.inventoryItemId).split('/').pop(),
            locationId: String(row.locationId).split('/').pop(),
            delta: -delta, // Deduct inventory (negative delta)
            shopDomain: row.storeDomain,
            storeAccessKey: row.storeAccessKey
          });

          console.log(`✅ Inventory adjusted for inventory_item_id: ${row.inventoryItemId}, location: ${row.locationId}`, result);

        } catch (shopifyError) {
          const errorMessage = (shopifyError.response && shopifyError.response.data) || shopifyError.message;
          console.error(`❌ Failed to adjust inventory for row:`, row, errorMessage);
          throw shopifyError;
        }
      }


      await client.query('COMMIT');
      console.log('✅ Transaction committed successfully');

    } catch (dbError) {
      await client.query('ROLLBACK');
      console.error('❌ Database transaction failed:', dbError);
      throw dbError;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('❌ Webhook handler error:', error);
    throw error;
  }
};

module.exports = { startConsumer };
