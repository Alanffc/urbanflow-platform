import express, { Request, Response } from 'express';
import { InfluxDB, Point, WriteApi } from '@influxdata/influxdb-client';
import { Kafka, Producer } from 'kafkajs';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const port = process.env.PORT || 3003;

// InfluxDB Connection Setup
const influxUrl = process.env.INFLUX_URL || 'http://localhost:8086';
const influxToken = process.env.INFLUX_TOKEN || 'urbanflow_super_secret_token_12345';
const influxOrg = process.env.INFLUX_ORG || 'urbanflow_org';
const influxBucket = process.env.INFLUX_BUCKET || 'telemetry_bucket';

const influxClient = new InfluxDB({ url: influxUrl, token: influxToken });
const writeApi: WriteApi = influxClient.getWriteApi(influxOrg, influxBucket);

// Kafka Connection Setup
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const kafka = new Kafka({
  clientId: 'telemetry-service',
  brokers: [kafkaBroker],
});
const producer: Producer = kafka.producer();

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
  res.json({
    status: 'UP',
    influxConnected: true,
    timestamp: new Date().toISOString()
  });
});

// Endpoint to ingest raw GPS telemetry from vehicles
app.post('/api/telemetry/gps', async (req: Request, res: Response): Promise<void> => {
  const { vehicleId, routeId, latitude, longitude, speedKmh, passengerCount, co2Emission } = req.body;

  if (!vehicleId || latitude === undefined || longitude === undefined) {
    res.status(400).json({ error: 'Missing required telemetry fields: vehicleId, latitude, longitude' });
    return;
  }

  try {
    const timestamp = new Date();

    // 1. Write telemetry point to InfluxDB Time-Series DB for high-speed writes
    const point = new Point('gps_ping')
      .tag('vehicle_id', vehicleId)
      .tag('route_id', routeId || 'unassigned')
      .floatField('latitude', latitude)
      .floatField('longitude', longitude)
      .floatField('speed_kmh', speedKmh || 0.0)
      .intField('passenger_count', passengerCount || 0)
      .floatField('co2_emission_g_km', co2Emission || 0.0)
      .timestamp(timestamp);

    writeApi.writePoint(point);
    // Flush periodically in production; for immediate durability in test:
    await writeApi.flush();

    // 2. Publish GPS position event to Kafka for real-time tracking, prediction, and routing services
    const gpsEvent = {
      eventId: `gps-${vehicleId}-${timestamp.getTime()}`,
      eventType: 'GPS_PING_RECEIVED',
      timestamp: timestamp.toISOString(),
      data: {
        vehicleId,
        routeId: routeId || 'unassigned',
        latitude,
        longitude,
        speedKmh: speedKmh || 0.0,
        passengerCount: passengerCount || 0,
        co2Emission: co2Emission || 0.0
      }
    };

    await producer.send({
      topic: 'telemetry-gps',
      messages: [
        {
          key: vehicleId,
          value: JSON.stringify(gpsEvent),
        }
      ]
    });

    res.status(202).json({
      success: true,
      message: 'Telemetry point accepted and broadcasted successfully',
      timestamp: timestamp.toISOString()
    });

  } catch (error) {
    console.error('Failed to ingest telemetry data:', error);
    res.status(500).json({ error: 'Internal Server Error during telemetry ingestion' });
  }
});

async function startServer() {
  try {
    await producer.connect();
    console.log('Telemetry Kafka Producer connected successfully');

    app.listen(port, () => {
      console.log(`Telemetry Service listening on port ${port}`);
    });
  } catch (error) {
    console.error('Failed to start Telemetry Service:', error);
  }
}

// Graceful Shutdown
const shutdown = async () => {
  console.log('Shutting down Telemetry Service...');
  try {
    await writeApi.close();
    await producer.disconnect();
  } catch (err) {
    console.error('Error during shutdown cleanups:', err);
  }
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startServer();
