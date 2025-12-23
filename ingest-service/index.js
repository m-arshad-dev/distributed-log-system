const express = require("express");
const fs = require("fs");
const multer = require("multer");
const { Queue } = require("bullmq");
const Redis = require("ioredis");
const { v4: uuidv4 } = require("uuid");

const app = express();
const upload = multer({ dest: "uploads/" });

const redis = new Redis({
  host: process.env.REDIS_HOST || "localhost",
  port: 6379,
});

const queue = new Queue("log-processing", { connection: redis });

app.post("/upload", upload.single("logfile"), async (req, res) => {
  const fileId = uuidv4();
  const lines = fs.readFileSync(req.file.path, "utf-8").split("\n");
  const CHUNK_SIZE = 1000;

  for (let i = 0; i < lines.length; i += CHUNK_SIZE) {
    await queue.add("process", {
      fileId,
      chunkId: `chunk_${i / CHUNK_SIZE}`,
      lines: lines.slice(i, i + CHUNK_SIZE),
    });
  }

  res.json({
    message: "Log file accepted",
    fileId,
    totalChunks: Math.ceil(lines.length / CHUNK_SIZE),
  });
});

app.listen(3000, () =>
  console.log("Ingest service running on port 3000")
);
