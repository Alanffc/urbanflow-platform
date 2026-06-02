import express, { Request, Response } from 'express';
import { Pool } from 'pg';
import { Kafka, Producer, Consumer } from 'kafkajs';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const port = process.env.PORT || 3001;

// Database Connection Configuration (PostgreSQL for payment records)
const dbPool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432'),
  user: process.env.DB_USER || 'urbanflow_user',
  password: process.env.DB_PASSWORD || 'urbanflow_password',
  database: process.env.DB_NAME || 'urbanflow_payments',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Kafka Client Configuration
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [kafkaBroker],
});

const producer: Producer = kafka.producer();
const consumer: Consumer = kafka.consumer({ groupId: 'payment-service-group' });

// Health check endpoint
app.get('/health', async (req: Request, res: Response): Promise<void> => {
  try {
    // Check PostgreSQL connection
    const dbCheck = await dbPool.query('SELECT 1');
    res.json({
      status: 'UP',
      database: dbCheck.rows.length === 1 ? 'CONNECTED' : 'DISCONNECTED',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'DOWN',
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

// Endpoint to simulate processing a payment transaction
app.post('/api/payments/charge', async (req: Request, res: Response): Promise<void> => {
  const { userId, journeyId, amount, paymentMethod, transactionDetails } = req.body;

  if (!userId || !journeyId || !amount || !paymentMethod) {
    res.status(400).json({ error: 'Missing required payment fields' });
    return;
  }

  const client = await dbPool.connect();
  try {
    await client.query('BEGIN');

    // 1. Insert transaction into relational database (PostgreSQL) for transactional consistency
    const queryText = `
      INSERT INTO transactions (user_id, journey_id, amount, payment_method, status, created_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
      RETURNING id, status, created_at;
    `;
    const result = await client.query(queryText, [userId, journeyId, amount, paymentMethod, 'COMPLETED']);
    const transaction = result.rows[0];

    // 2. Publish Payment Success event to Kafka for other microservices (e.g., Auditing, Notifications)
    const paymentEvent = {
      eventId: `pay-${transaction.id}`,
      eventType: 'PAYMENT_COMPLETED',
      timestamp: transaction.created_at,
      data: {
        transactionId: transaction.id,
        userId,
        journeyId,
        amount,
        paymentMethod,
        status: transaction.status,
        details: transactionDetails || {}
      }
    };

    await producer.send({
      topic: 'payment-events',
      messages: [
        {
          key: userId.toString(),
          value: JSON.stringify(paymentEvent),
        }
      ]
    });

    await client.query('COMMIT');

    res.status(201).json({
      success: true,
      message: 'Payment charged successfully',
      transaction: {
        id: transaction.id,
        status: transaction.status,
        amount,
        createdAt: transaction.created_at
      }
    });

  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to process charge:', error);
    res.status(500).json({ error: 'Internal Server Error during payment processing' });
  } finally {
    client.release();
  }
});

// Initialize Kafka and start server
async function startServer() {
  try {
    // Connect to Kafka Producer
    await producer.connect();
    console.log('Successfully connected Kafka Producer');

    // Connect and subscribe Kafka Consumer
    await consumer.connect();
    await consumer.subscribe({ topic: 'trip-completed-events', fromBeginning: false });
    console.log('Successfully subscribed Kafka Consumer to trip-completed-events');

    // Start consuming events
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          if (!message.value) return;
          const rawMessage = message.value.toString();
          const event = JSON.parse(rawMessage);
          
          console.log(`Received event on topic [${topic}]:`, event);
          
          // Logic for processing trip completion, calculating final fare, and auto-charging
          if (event.eventType === 'TRIP_COMPLETED') {
            console.log(`Processing auto-billing for completed trip: ${event.data.journeyId}`);
            // In production, this would trigger database updates and call billing logic
          }
        } catch (err) {
          console.error('Error handling consumed Kafka message:', err);
        }
      },
    });

    // Start Express listener
    app.listen(port, () => {
      console.log(`Payment Service listening on port ${port}`);
    });
  } catch (error) {
    console.error('Fatal initialization error:', error);
    process.exit(1);
  }
}

// Handle graceful shutdown
const shutdown = async () => {
  console.log('Shutting down server gracefully...');
  await producer.disconnect();
  await consumer.disconnect();
  await dbPool.end();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startServer();
