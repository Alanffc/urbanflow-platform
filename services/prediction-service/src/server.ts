import express, { Request, Response } from 'express';
import { Kafka, Producer, Consumer } from 'kafkajs';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const port = process.env.PORT || 3005;

// Kafka setup
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const kafka = new Kafka({
  clientId: 'prediction-service',
  brokers: [kafkaBroker],
});

const producer: Producer = kafka.producer();
const consumer: Consumer = kafka.consumer({ groupId: 'prediction-service-group' });

// Simple in-memory telemetry window to track average speeds per route
// In production, this would read historical data patterns from AWS S3 (Data Lake)
// and run ML models (e.g. XGBoost, LSTM) on the current streams.
interface RouteSpeeds {
  speeds: number[];
  lastUpdate: number;
}
const routeSpeedWindow: Map<string, RouteSpeeds> = new Map();

const CONGESTION_SPEED_THRESHOLD_KMH = 15.0; // If average speed drops below 15 km/h
const MIN_SAMPLES_FOR_PREDICTION = 5;

// Health endpoint
app.get('/health', (req: Request, res: Response) => {
  res.json({
    status: 'UP',
    monitoredRoutesCount: routeSpeedWindow.size,
    timestamp: new Date().toISOString()
  });
});

async function processTelemetryEvent(event: any) {
  const { routeId, speedKmh, vehicleId, latitude, longitude } = event.data;
  
  if (!routeId || routeId === 'unassigned') return;

  // Get or initialize route telemetry sliding window
  let routeData = routeSpeedWindow.get(routeId);
  if (!routeData) {
    routeData = { speeds: [], lastUpdate: Date.now() };
    routeSpeedWindow.set(routeId, routeData);
  }

  // Keep last 10 speed readings
  routeData.speeds.push(speedKmh);
  if (routeData.speeds.length > 10) {
    routeData.speeds.shift();
  }
  routeData.lastUpdate = Date.now();

  // Evaluate congestion risk
  if (routeData.speeds.length >= MIN_SAMPLES_FOR_PREDICTION) {
    const avgSpeed = routeData.speeds.reduce((sum, s) => sum + s, 0) / routeData.speeds.length;
    
    if (avgSpeed < CONGESTION_SPEED_THRESHOLD_KMH) {
      console.log(`[Prediction Engine] Anomaly detected on route ${routeId}. Avg speed: ${avgSpeed.toFixed(2)} km/h. Triggering 30-min forecast alert.`);
      
      const congestionEvent = {
        eventId: `pred-cong-${routeId}-${Date.now()}`,
        eventType: 'CONGESTION_PREDICTED',
        timestamp: new Date().toISOString(),
        data: {
          routeId,
          averageSpeedKmh: avgSpeed,
          forecastHorizonMinutes: 30,
          severity: 'HIGH',
          affectedLocation: { latitude, longitude }
        }
      };

      // Publish prediction to Kafka
      await producer.send({
        topic: 'congestion-events',
        messages: [
          {
            key: routeId,
            value: JSON.stringify(congestionEvent),
          }
        ]
      });
    }
  }
}

async function startServer() {
  try {
    await producer.connect();
    await consumer.connect();
    
    await consumer.subscribe({ topic: 'telemetry-gps', fromBeginning: false });
    console.log('Prediction Engine subscribed to telemetry-gps topic');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          if (!message.value) return;
          const event = JSON.parse(message.value.toString());
          await processTelemetryEvent(event);
        } catch (err) {
          console.error('Error processing telemetry in prediction engine:', err);
        }
      }
    });

    app.listen(port, () => {
      console.log(`Prediction Service listening on port ${port}`);
    });

  } catch (error) {
    console.error('Failed to start Prediction Service:', error);
  }
}

// Graceful Shutdown
const shutdown = async () => {
  console.log('Shutting down Prediction Service...');
  await producer.disconnect();
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startServer();
