import express, { Request, Response } from 'express';
import { Kafka, Consumer } from 'kafkajs';
import cors from 'cors';
import dotenv from 'dotenv';
import crypto from 'crypto';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const port = process.env.PORT || 3004;

// Kafka Configuration
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:29092';
const kafka = new Kafka({
  clientId: 'ntcip-adapter',
  brokers: [kafkaBroker],
});
const consumer: Consumer = kafka.consumer({ groupId: 'ntcip-adapter-group' });

// Simulated legacy device endpoint (UDP or 4G TCP Socket emulation)
interface NtcipPacket {
  rawHex: string;
  sizeBytes: number;
  data: {
    messageType: number;
    intersectionId: number;
    timestamp: number;
    durationSeconds: number;
    verificationChecksum: number;
  };
}

/**
 * Packs high-level commands into a compact binary buffer of max 256 bytes.
 * Layout (Total: 21 bytes):
 * - Message Type (1 byte): 0x01 for priority green, 0x02 for schedule override
 * - Intersection ID (4 bytes): Unsigned 32-bit Integer
 * - Timestamp (4 bytes): Unsigned 32-bit Unix Epoch Timestamp
 * - Duration (4 bytes): Unsigned 32-bit Integer (seconds to hold green)
 * - HMAC/Checksum (8 bytes): SHA-256 slice for packet verification
 */
export function packNtcipMessage(
  messageType: number,
  intersectionId: number,
  durationSeconds: number
): Buffer {
  const buffer = Buffer.alloc(21); // Rigid fixed length to prevent buffer overruns
  
  const timestamp = Math.floor(Date.now() / 1000);

  // Write variables into buffer
  buffer.writeUInt8(messageType, 0);                 // Offset 0 (1 byte)
  buffer.writeUInt32BE(intersectionId, 1);           // Offset 1 (4 bytes)
  buffer.writeUInt32BE(timestamp, 5);                // Offset 5 (4 bytes)
  buffer.writeUInt32BE(durationSeconds, 9);          // Offset 9 (4 bytes)

  // Compute a simple verification checksum / security token (first 8 bytes of HMAC)
  const privateSecret = 'ntcip_secret_key_9999';
  const hmac = crypto.createHmac('sha256', privateSecret);
  hmac.update(buffer.subarray(0, 13)); // Sign the payload data
  const hashResult = hmac.digest();
  
  hashResult.copy(buffer, 13, 0, 8);                 // Offset 13 (8 bytes)

  return buffer;
}

// Endpoint to simulate manually triggering a traffic light override
app.post('/api/traffic/override', (req: Request, res: Response) => {
  const { intersectionId, durationSeconds, messageType } = req.body;

  if (intersectionId === undefined || durationSeconds === undefined) {
    res.status(400).json({ error: 'Missing parameters: intersectionId, durationSeconds' });
    return;
  }

  const msgType = messageType || 0x01; // Default to Priority Green (0x01)
  const binaryPacket = packNtcipMessage(msgType, Number(intersectionId), Number(durationSeconds));

  const packetInfo: NtcipPacket = {
    rawHex: binaryPacket.toString('hex').toUpperCase(),
    sizeBytes: binaryPacket.length,
    data: {
      messageType: msgType,
      intersectionId,
      timestamp: Math.floor(Date.now() / 1000),
      durationSeconds,
      verificationChecksum: binaryPacket.readUInt32BE(13), // Read partial hash signature
    }
  };

  console.log(`[NTCIP Adaptor] Generated Binary Packet (${packetInfo.sizeBytes} bytes) for network dispatch:`, packetInfo.rawHex);

  if (binaryPacket.length > 256) {
    res.status(500).json({ error: 'Packet size exceeds NTCIP payload limit of 256 bytes' });
    return;
  }

  res.json({
    success: true,
    message: 'NTCIP packet generated and queued for transmission over legacy 4G private network',
    packet: packetInfo
  });
});

async function startServer() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'ntcip-commands', fromBeginning: false });
    console.log('NTCIP Adaptor successfully connected to Kafka');

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          if (!message.value) return;
          const event = JSON.parse(message.value.toString());
          console.log(`[NTCIP Adaptor] Consumed Kafka Command:`, event);

          if (event.command === 'TRIGGER_PRIORITY_GREEN') {
            const { intersectionId, durationSeconds } = event.data;
            const packet = packNtcipMessage(0x01, intersectionId, durationSeconds);
            
            console.log(`[NTCIP Adaptor] DISPATCHING OVER 4G NTCIP -> Hex: ${packet.toString('hex').toUpperCase()} (${packet.length} bytes)`);
          }
        } catch (err) {
          console.error('[NTCIP Adaptor] Error processing Kafka traffic event:', err);
        }
      }
    });

    app.listen(port, () => {
      console.log(`NTCIP Adaptor Service listening on port ${port}`);
    });
  } catch (error) {
    console.error('Failed to start NTCIP Adaptor:', error);
  }
}

// Graceful Shutdown
const shutdown = async () => {
  console.log('Shutting down NTCIP Adaptor...');
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startServer();
