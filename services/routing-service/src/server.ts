import express, { Request, Response } from 'express';
import neo4j, { Driver } from 'neo4j-driver';
import cors from 'cors';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const port = process.env.PORT || 3002;

// Neo4j Configuration
const neo4jUri = process.env.NEO4J_URI || 'bolt://localhost:7687';
const neo4jUser = process.env.NEO4J_USER || 'neo4j';
const neo4jPassword = process.env.NEO4J_PASSWORD || 'urbanflow_password';

const driver: Driver = neo4j.driver(neo4jUri, neo4j.auth.basic(neo4jUser, neo4jPassword));

// Kafka Configuration (Optional listening for route blockage events)
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const kafka = new Kafka({
  clientId: 'routing-service',
  brokers: [kafkaBroker],
});
const consumer = kafka.consumer({ groupId: 'routing-service-group' });

// Health check endpoint
app.get('/health', async (req: Request, res: Response): Promise<void> => {
  try {
    const session = driver.session();
    const result = await session.run('RETURN 1 AS val');
    await session.close();
    
    res.json({
      status: 'UP',
      database: result.records[0].get('val').toNumber() === 1 ? 'CONNECTED' : 'DISCONNECTED',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'DOWN',
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

// Interface for segments return format
interface RouteSegment {
  from: string;
  to: string;
  mode: string;
  line?: string;
  distanceKm: number;
  timeMin: number;
  costUsd: number;
  co2EmissionsG: number;
}

// Endpoint to calculate optimal multimodal route between two station IDs
app.get('/api/routes/plan', async (req: Request, res: Response): Promise<void> => {
  const { startId, endId, optimizeFor } = req.query; // optimizeFor: 'time', 'cost', 'co2'

  if (!startId || !endId) {
    res.status(400).json({ error: 'Missing startId or endId parameters' });
    return;
  }

  const session = driver.session();
  try {
    // Cypher query to find a path and aggregate parameters
    // Using a simpler path finder that searches paths up to 5 hops
    const optimizeProp = optimizeFor === 'co2' ? 'co2_emissions_g' : 
                         optimizeFor === 'cost' ? 'cost_usd' : 'time_min';

    const cypherQuery = `
      MATCH p = ShortestPath((start:Stop {id: $startId})-[:CONNECTS_TO*..5]->(end:Stop {id: $endId}))
      RETURN 
        nodes(p) as stops,
        relationships(p) as segments,
        reduce(total = 0.0, r IN relationships(p) | total + coalesce(r.time_min, 0.0)) as totalTime,
        reduce(total = 0.0, r IN relationships(p) | total + coalesce(r.cost_usd, 0.0)) as totalCost,
        reduce(total = 0.0, r IN relationships(p) | total + coalesce(r.co2_emissions_g, 0.0)) as totalCo2
    `;

    const result = await session.run(cypherQuery, { startId, endId });

    if (result.records.length === 0) {
      res.status(404).json({ error: 'No multimodal route path found between specified stops' });
      return;
    }

    const record = result.records[0];
    const stopsRaw = record.get('stops');
    const segmentsRaw = record.get('segments');

    const totalTime = record.get('totalTime');
    const totalCost = record.get('totalCost');
    const totalCo2 = record.get('totalCo2');

    const segments: RouteSegment[] = segmentsRaw.map((rel: any, idx: number) => {
      const fromNode = stopsRaw[idx];
      const toNode = stopsRaw[idx + 1];
      const props = rel.properties;

      return {
        from: fromNode.properties.name,
        to: toNode.properties.name,
        mode: props.mode,
        line: props.line,
        distanceKm: props.distance_km,
        timeMin: props.time_min,
        costUsd: props.cost_usd,
        co2EmissionsG: props.co2_emissions_g
      };
    });

    res.json({
      success: true,
      origin: stopsRaw[0].properties.name,
      destination: stopsRaw[stopsRaw.length - 1].properties.name,
      summary: {
        totalTimeMinutes: totalTime,
        totalCostUsd: totalCost,
        totalCo2Grams: totalCo2,
        segmentCount: segments.length
      },
      route: segments
    });

  } catch (error) {
    console.error('Routing calculation failed:', error);
    res.status(500).json({ error: 'Failed to compute route path' });
  } finally {
    await session.close();
  }
});

async function startServer() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'reroute-events', fromBeginning: false });
    
    // In background, listen for dynamic re-route adjustments to recalculate paths or tag congested arcs
    consumer.run({
      eachMessage: async ({ topic, message }) => {
        if (!message.value) return;
        const event = JSON.parse(message.value.toString());
        console.log(`Routing Service received event to recalculate or log blockages:`, event);
      }
    });

    app.listen(port, () => {
      console.log(`Routing Service listening on port ${port}`);
    });
  } catch (error) {
    console.error('Failed to start Routing Service:', error);
  }
}

// Graceful Shutdown
const shutdown = async () => {
  console.log('Shutting down Routing Service...');
  await driver.close();
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startServer();
