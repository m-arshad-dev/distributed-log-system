const { Worker } = require("bullmq");
const Redis = require("ioredis");
const { MongoClient } = require("mongodb");

const redis = new Redis({
  host: process.env.REDIS_HOST || "localhost",
  port: 6379,
});

const mongo = new MongoClient(process.env.MONGO_URL);
let collection;

async function initDB() {
  await mongo.connect();
  collection = mongo.db().collection("chunk_metrics");
}
initDB();

new Worker(
  "log-processing",
  async (job) => {
    const start = Date.now();
    const { fileId, chunkId, lines } = job.data;

    let errorCount = 0,
      infoCount = 0,
      warnCount = 0;

    for (const line of lines) {
      if (line.includes("ERROR")) errorCount++;
      else if (line.includes("INFO")) infoCount++;
      else if (line.includes("WARN")) warnCount++;
    }

    await collection.insertOne({
      fileId,
      chunkId,
      workerId: process.pid,
      errorCount,
      infoCount,
      warnCount,
      totalLines: lines.length,
      processingTimeMs: Date.now() - start,
      createdAt: new Date(),
    });
  },
  { connection: redis }
);

console.log("Worker started");
