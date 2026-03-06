const amqp = require('amqplib');
require('dotenv').config();
const QUEUE_NAME = process.env.RABBITMQ_QUEUE || 'shopify_webhooks_queue';
const PREFETCH_COUNT = parseInt(process.env.CONSUMER_PREFETCH_COUNT) || 1;

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
        console.log("Message");
  const lineItems = message.body.line_items || [];
  
  console.log('=== Order Line Items ===');
  lineItems.forEach(item => {
    console.log(`Variant ID: ${item.variant_id}, Quantity: ${item.quantity}`);

    updateInventory(item.variant_id, item.quantity);

  });
        console.log("Message");
        //await handleInventoryUpdate(message);
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






async function connect() {
    if (connection && channel) return channel;

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
        console.error('❌ Failed to connect to RabbitMQ:', error);
        throw error;
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

const { Pool } = require('pg');
const axios = require('axios');

const pool = new Pool({
  connectionString: "postgresql://kong:kong@kong-database:5432/kong",
});

// Shopify API base
const SHOPIFY_API_VERSION = '2025-04';

// Adjust inventory via Shopify Admin API
async function adjustShopifyInventory({
  inventoryItemId,
  locationId,
  delta,
  shopDomain,
  accessToken
}) {
  const url = `https://${shopDomain}/admin/api/${SHOPIFY_API_VERSION}/inventory_levels/adjust.json`;
  
  const response = await axios.post(
    url,
    {
      inventory_item_id: inventoryItemId,
      location_id: locationId,
      available_adjustment: delta // positive or negative integer
    },
    {
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      },
      timeout: 10000
    }
  );
  
  return response.data;
}

async function updateInventory(variantId, delta)  {
  try {
    variantId = 46264568774806;
    // 4️⃣ Process each variant in a DB transaction with row locking
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      

        // 🔒 Fetch and LOCK all group rows for this variant
        const lockQuery = `
          SELECT 
            group_id,
            inventory_item_id,
            location_id,
            shopify_domain_url,
            shopify_access_token
          FROM inventory_groups
          WHERE group_id = (SELECT group_id FROM inventory_groups
          WHERE variant_id = $1)
          FOR UPDATE SKIP LOCKED
        `;
        
        const lockResult = await client.query(lockQuery, [variantId]);
        const groupRows = lockResult.rows;
        
        if (groupRows.length === 0) {
          console.warn(`⚠️ No inventory group found for variant_id: ${variantId}`);
          return;
        }

        console.log(`🔒 Locked ${groupRows.length} rows for variant_id: ${variantId}, group_id: ${groupRows[0].group_id}`);
        
        // 🔄 Adjust inventory on Shopify for EACH locked row
        for (const row of groupRows) {
          try {
            const result = await adjustShopifyInventory({
              inventoryItemId: row.inventory_item_id,
              locationId: row.location_id,
              delta: -delta, // Deduct inventory (negative delta)
              shopDomain: row.shopify_domain_url,
              accessToken: row.shopify_access_token
            });
            
            console.log(`✅ Inventory adjusted for inventory_item_id: ${row.inventory_item_id}, location: ${row.location_id}`, result);
            
            
          } catch (shopifyError) {
            console.error(`❌ Failed to adjust inventory for row:`, row, shopifyError.response?.data || shopifyError.message);
            // Decide: continue or rollback? For partial failures, you may want to rollback:
            // await client.query('ROLLBACK');
            // return res.status(500).send('Inventory adjustment failed');
          }
        }
      
      
      await client.query('COMMIT');
      console.log('✅ Transaction committed successfully');
      
    } catch (dbError) {
      await client.query('ROLLBACK');
      console.error('❌ Database transaction failed:', dbError);
    } finally {
      client.release();
    }  
  } catch (error) {
    console.error('❌ Webhook handler error:', error);
  }
};

module.exports = {startConsumer};