import { describe, expect, it } from "vitest";

import { newDynamoDBClient, newLambdaClient, newS3Client } from "./client";

describe("aws client factories", () => {
  it("creates clients with explicit regions", async () => {
    const s3 = newS3Client("us-west-2");
    const ddb = newDynamoDBClient("eu-west-1");
    const lambda = newLambdaClient("ap-southeast-1");

    await expect(s3.config.region()).resolves.toBe("us-west-2");
    await expect(ddb.config.region()).resolves.toBe("eu-west-1");
    await expect(lambda.config.region()).resolves.toBe("ap-southeast-1");
  });

  it("uses default region when omitted", async () => {
    const s3 = newS3Client();

    await expect(s3.config.region()).resolves.toBe("us-east-1");
  });

  it("sets static test credentials", async () => {
    const lambda = newLambdaClient();
    const credentials = await lambda.config.credentials();

    expect(credentials.accessKeyId).toBe("test");
    expect(credentials.secretAccessKey).toBe("test");
  });

  it("supports server-side endpoint resolution path", async () => {
    const originalWindow = globalThis.window;
    Object.defineProperty(globalThis, "window", {
      value: undefined,
      configurable: true,
    });

    const s3 = newS3Client();
    const endpointProvider = s3.config.endpoint;
    const endpoint = await endpointProvider();

    expect(endpoint).toBeDefined();

    Object.defineProperty(globalThis, "window", {
      value: originalWindow,
      configurable: true,
    });
  });
});
