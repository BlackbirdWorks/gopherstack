import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { S3Client } from "@aws-sdk/client-s3";

const defaultRegion = "us-east-1";
const REGION_STORAGE_KEY = "gopherstack_region";

export function getStoredRegion(): string {
  if (typeof window === "undefined" || !window.localStorage) return defaultRegion;
  try {
    return window.localStorage.getItem(REGION_STORAGE_KEY) ?? defaultRegion;
  } catch {
    return defaultRegion;
  }
}

export function setStoredRegion(region: string): void {
  if (typeof window === "undefined" || !window.localStorage) return;
  try {
    window.localStorage.setItem(REGION_STORAGE_KEY, region);
  } catch {
    // ignore
  }
  window.dispatchEvent(new CustomEvent("gopherstack:region-change", { detail: region }));
}

function endpointURL(): string {
  if (typeof window === "undefined" || !window.location) {
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
  return new S3Client({ ...clientConfig(region ?? getStoredRegion()), forcePathStyle: true });
}

export function newDynamoDBClient(region?: string): DynamoDBClient {
  return new DynamoDBClient(clientConfig(region ?? getStoredRegion()));
}

export function newLambdaClient(region?: string): LambdaClient {
  return new LambdaClient(clientConfig(region ?? getStoredRegion()));
}
