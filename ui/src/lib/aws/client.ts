import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { S3Client } from "@aws-sdk/client-s3";

const defaultRegion = "us-east-1";

function endpointURL(): string {
  if (typeof window === "undefined") {
    return "http://localhost:8000";
  }

  return window.location.origin;
}

function clientConfig(region = defaultRegion) {
  return {
    endpoint: endpointURL(),
    region,
    credentials: {
      accessKeyId: "test",
      secretAccessKey: "test",
    },
  };
}

export function newS3Client(region?: string): S3Client {
  return new S3Client(clientConfig(region));
}

export function newDynamoDBClient(region?: string): DynamoDBClient {
  return new DynamoDBClient(clientConfig(region));
}

export function newLambdaClient(region?: string): LambdaClient {
  return new LambdaClient(clientConfig(region));
}
