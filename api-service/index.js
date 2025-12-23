const express = require("express");
const { MongoClient } = require("mongodb");

const app = express();
const mongo = new MongoClient("mongodb://mongodb:27017/logs");
let collection;

async function init() {
  await mongo.connect();
  collection = mongo.db().collection("chunk_metrics");
}
init();

app.get("/files/:fileId/summary", async (req, res) => {
  const fileId = req.params.fileId;

  const result = await collection.aggregate([
    { $match: { fileId } },
    {
      $group: {
        _id: "$fileId",
        totalErrors: { $sum: "$errorCount" },
        totalInfo: { $sum: "$infoCount" },
        totalWarn: { $sum: "$warnCount" },
        totalLines: { $sum: "$totalLines" },
        avgProcessingTime: { $avg: "$processingTimeMs" },
        chunksProcessed: { $sum: 1 },
      },
    },
  ]).toArray();

  res.json(result[0] || {});
});

app.listen(4000, () =>
  console.log("Analytics API running on port 4000")
);
