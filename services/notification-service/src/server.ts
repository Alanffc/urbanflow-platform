import express, { Request, Response } from 'express';
import { Kafka, Consumer } from 'kafkajs';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const port = process.env.PORT || 3006;

// Kafka setup
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [kafkaBroker],
});
const consumer: Consumer = kafka.consumer({ groupId: 'notification-service-group' });

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
  res.json({
    status: 'UP',
    timestamp: new Date().toISOString()
  });
});

async function startServer() {
  try {
    await consumer.connect();
    
    // Subscribe to both topics
    await consumer.subscribe({ topic: 'congestion-events', fromBeginning: false });
    await consumer.subscribe({ topic: 'reroute-events', fromBeginning: false });
    console.log('Notification Service subscribed to congestion-events and reroute-events');

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          if (!message.value) return;
          const rawMessage = message.value.toString();
          const event = JSON.parse(rawMessage);
          
          console.log(`[Notification Service] Consumed event from [${topic}]:`, event);
          
          if (topic === 'congestion-events') {
            console.log(`[PUSH NOTIFICATION] Sending alert to Passengers on Route ${event.data.routeId}: "Expect delays up to 30 minutes due to forecasted heavy congestion."`);
          } else if (topic === 'reroute-events') {
            console.log(`[TERMINAL PUSH] Sending route change command to Driver of Vehicle ${event.data.vehicleId}: "Rerouting triggered. Please follow the updated navigation map."`);
          }
        } catch (err) {
          console.error('Error processing consumed message in Notification Service:', err);
        }
      }
    });

    app.listen(port, () => {
      console.log(`Notification Service listening on port ${port}`);
    });
  } catch (error) {
    console.error('Failed to start Notification Service:', error);
  }
}

// Graceful Shutdown
const shutdown = async () => {
  console.log('Shutting down Notification Service...');
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startServer();
