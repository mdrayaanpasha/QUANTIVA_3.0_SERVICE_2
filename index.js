import express from "express";
import cors from "cors";
import amqplib from "amqplib";
import { createClient } from 'redis';
import dotenv from "dotenv";

dotenv.config();

const client = createClient({
  url: process.env.REDIS_KEY
});

const rabbitmqUrl = process.env.RABITMQ_KEY;

async function connectRabbitMQ() {
  try {
    const connection = await amqplib.connect(rabbitmqUrl);
    channel = await connection.createChannel();
    console.log("Connected to RabbitMQ");
  } catch (error) {
    console.error("Failed to connect to RabbitMQ:", error);
  }
}

let Queue = "fin_queue";

let conn = await amqplib.connect(rabbitmqUrl);
const channel = await conn.createChannel();

await channel.assertQueue(Queue, { durable: true });
await channel.assertExchange("amq.direct", "direct", { durable: true });

await channel.consume(Queue, (msg) => {
  if (msg !== null) {
    console.log("Received message:", msg.content.toString());
    channel.ack(msg);
  }
}, { noAck: false }); 

client.on('error', err => console.log('Redis Client Error', err));

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get("/", (req, res) => {
  res.json({ message: "Server is running" });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", uptime: process.uptime() });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

// Error handler
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));