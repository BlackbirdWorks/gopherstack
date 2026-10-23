import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { CreateBucketCommand, GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { CreateQueueCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { GopherstackContainer } from "../index.js";

let gs;

before(async () => {
  gs = await new GopherstackContainer(process.env.GOPHERSTACK_TEST_IMAGE).withServices("s3", "sqs").start();
});

after(async () => {
  await gs?.stop();
});

test("s3 roundtrip", async () => {
  const s3 = new S3Client(gs.getAwsClientConfig());
  await s3.send(new CreateBucketCommand({ Bucket: "tc-node" }));
  await s3.send(new PutObjectCommand({ Bucket: "tc-node", Key: "k", Body: "hello" }));
  const out = await s3.send(new GetObjectCommand({ Bucket: "tc-node", Key: "k" }));
  assert.equal(await out.Body.transformToString(), "hello");
});

test("sqs roundtrip", async () => {
  const sqs = new SQSClient(gs.getAwsClientConfig());
  const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "tc-node" }));
  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "ping" }));
  const { Messages } = await sqs.send(new ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 2 }));
  assert.equal(Messages[0].Body, "ping");
});

test("helpers", () => {
  assert.match(gs.getAzureConnectionString(), /AccountName=devstoreaccount1/);
  assert.throws(() => gs.getGcpEndpoint("nope"));
});
