const { DynamoDBClient, UpdateItemCommand } = require("@aws-sdk/client-dynamodb");

// We point the SDK to Gopherstack
const ddbClient = new DynamoDBClient({
  endpoint: process.env.AWS_ENDPOINT_URL || "http://host.docker.internal:8000",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" }
});

const TABLE_NAME = process.env.TABLE_NAME || "event-metrics";

exports.handler = async (event) => {
  console.log("Received events:", JSON.stringify(event, null, 2));

  const counts = {};

  // Aggregate event types
  for (const record of event.Records) {
    // Kinesis data is base64 encoded
    const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf8');
    try {
      const data = JSON.parse(payload);
      const type = data.type || "unknown";
      counts[type] = (counts[type] || 0) + 1;
    } catch (err) {
      console.error("Failed to parse record:", payload);
    }
  }

  console.log("Aggregated counts:", counts);

  // Update DynamoDB
  for (const [type, count] of Object.entries(counts)) {
    const params = {
      TableName: TABLE_NAME,
      Key: {
        eventType: { S: type }
      },
      UpdateExpression: "ADD #cnt :val",
      ExpressionAttributeNames: {
        "#cnt": "eventCount"
      },
      ExpressionAttributeValues: {
        ":val": { N: count.toString() }
      }
    };

    await ddbClient.send(new UpdateItemCommand(params));
    console.log(`Updated count for ${type} by ${count}`);
  }

  return { success: true, processed: event.Records.length };
};
