// AWS SDK for JavaScript v3 cross-SDK compatibility suite for gopherstack.
// Env: AWS_ENDPOINT_URL (default http://localhost:8000), GS_SKIP_LAMBDA=1 to skip Lambda.
// Prints PASS/FAIL per case and exits non-zero on any failure.
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { execFileSync } from "node:child_process";
import { deflateRawSync } from "node:zlib";

import { S3Client, CreateBucketCommand, ListBucketsCommand, PutObjectCommand, GetObjectCommand, HeadObjectCommand, ListObjectsV2Command, DeleteObjectCommand, DeleteBucketCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { DynamoDBClient, CreateTableCommand, DescribeTableCommand, DeleteTableCommand, TransactWriteItemsCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBStreamsClient, ListStreamsCommand, DescribeStreamCommand } from "@aws-sdk/client-dynamodb-streams";
import { SQSClient, CreateQueueCommand, GetQueueAttributesCommand, SetQueueAttributesCommand, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, DeleteQueueCommand } from "@aws-sdk/client-sqs";
import { SNSClient, CreateTopicCommand, SubscribeCommand, PublishCommand, DeleteTopicCommand } from "@aws-sdk/client-sns";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import { IAMClient, CreateRoleCommand, GetRoleCommand, DeleteRoleCommand } from "@aws-sdk/client-iam";
import { SecretsManagerClient, CreateSecretCommand, GetSecretValueCommand, DeleteSecretCommand } from "@aws-sdk/client-secrets-manager";
import { KMSClient, CreateKeyCommand, EncryptCommand, DecryptCommand } from "@aws-sdk/client-kms";
import { CloudWatchLogsClient, CreateLogGroupCommand, CreateLogStreamCommand, PutLogEventsCommand, GetLogEventsCommand, DeleteLogGroupCommand } from "@aws-sdk/client-cloudwatch-logs";
import { EventBridgeClient, PutRuleCommand, PutTargetsCommand, PutEventsCommand, RemoveTargetsCommand, DeleteRuleCommand } from "@aws-sdk/client-eventbridge";
import { SFNClient, CreateStateMachineCommand, StartExecutionCommand, DescribeExecutionCommand, DeleteStateMachineCommand } from "@aws-sdk/client-sfn";
import { LambdaClient, CreateFunctionCommand, GetFunctionCommand, InvokeCommand, DeleteFunctionCommand } from "@aws-sdk/client-lambda";

const endpoint = process.env.AWS_ENDPOINT_URL || "http://localhost:8000";
const RUN = randomBytes(4).toString("hex");
const base = {
  endpoint,
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  maxAttempts: 1,
};
const s3 = new S3Client({ ...base, forcePathStyle: true });
const ddb = new DynamoDBClient(base);
const doc = DynamoDBDocumentClient.from(ddb);
const streams = new DynamoDBStreamsClient(base);
const sqs = new SQSClient(base);
const sns = new SNSClient(base);
const sts = new STSClient(base);
const iam = new IAMClient(base);
const sm = new SecretsManagerClient(base);
const kms = new KMSClient(base);
const logs = new CloudWatchLogsClient(base);
const eb = new EventBridgeClient(base);
const sfn = new SFNClient(base);
const lambda = new LambdaClient(base);

class Skip extends Error {}
const cases = [];
const test = (name, fn) => cases.push([name, fn]);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const errName = async (p) => {
  try {
    await p;
  } catch (e) {
    return e.name;
  }
  return "";
};
async function waitFor(fn, timeoutMs = 30000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    const v = await fn();
    if (v) return v;
    await sleep(250);
  }
  throw new Error("timed out waiting for condition");
}
async function receiveN(url, n) {
  const msgs = [];
  await waitFor(async () => {
    const r = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    msgs.push(...(r.Messages ?? []));
    return msgs.length >= n;
  }, 20000);
  return msgs;
}
async function makeQueue(name, attrs) {
  const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attrs }));
  const a = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ["QueueArn"] }));
  return [QueueUrl, a.Attributes.QueueArn];
}
const allowAll = JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: "*", Action: "sqs:SendMessage", Resource: "*" }] });
const trust = (svc) => JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: { Service: svc }, Action: "sts:AssumeRole" }] });
const bodyText = (b) => b.transformToString();
const waitActive = (t) => waitFor(async () => (await ddb.send(new DescribeTableCommand({ TableName: t }))).Table.TableStatus === "ACTIVE");
const hashTable = (t, extra = {}) =>
  ddb.send(new CreateTableCommand({ TableName: t, AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }], KeySchema: [{ AttributeName: "id", KeyType: "HASH" }], BillingMode: "PAY_PER_REQUEST", ...extra }));

test("s3_bucket_and_object_roundtrip", async () => {
  const Bucket = `sdk-js-${RUN}-rt`;
  await s3.send(new CreateBucketCommand({ Bucket }));
  assert.ok((await s3.send(new ListBucketsCommand({}))).Buckets.some((b) => b.Name === Bucket));
  await s3.send(new PutObjectCommand({ Bucket, Key: "a/hello.txt", Body: "hello", ContentType: "text/plain", Metadata: { k: "v" } }));
  const got = await s3.send(new GetObjectCommand({ Bucket, Key: "a/hello.txt" }));
  assert.equal(await bodyText(got.Body), "hello");
  assert.equal(got.ContentType, "text/plain");
  assert.deepEqual(got.Metadata, { k: "v" });
  assert.equal((await s3.send(new HeadObjectCommand({ Bucket, Key: "a/hello.txt" }))).ContentLength, 5);
  assert.equal(await errName(s3.send(new GetObjectCommand({ Bucket, Key: "missing" }))), "NoSuchKey");
  await s3.send(new DeleteObjectCommand({ Bucket, Key: "a/hello.txt" }));
  await s3.send(new DeleteBucketCommand({ Bucket }));
});

test("s3_list_objects_v2_pagination", async () => {
  const Bucket = `sdk-js-${RUN}-page`;
  await s3.send(new CreateBucketCommand({ Bucket }));
  const keys = [0, 1, 2, 3, 4].map((i) => `p/${i}`);
  for (const Key of keys) await s3.send(new PutObjectCommand({ Bucket, Key, Body: Key }));
  const seen = [];
  let token;
  let pages = 0;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket, Prefix: "p/", MaxKeys: 2, ContinuationToken: token }));
    pages++;
    seen.push(...(r.Contents ?? []).map((o) => o.Key));
    token = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (token);
  assert.deepEqual(seen, keys);
  assert.equal(pages, 3);
  for (const Key of keys) await s3.send(new DeleteObjectCommand({ Bucket, Key }));
  await s3.send(new DeleteBucketCommand({ Bucket }));
});

test("s3_multipart_upload_lib_storage", async () => {
  const Bucket = `sdk-js-${RUN}-mpu`;
  await s3.send(new CreateBucketCommand({ Bucket }));
  const payload = Buffer.concat([Buffer.alloc(5 * 1024 * 1024, 97), Buffer.from("tail")]);
  const parts = [];
  const up = new Upload({ client: s3, params: { Bucket, Key: "big.bin", Body: payload }, partSize: 5 * 1024 * 1024, queueSize: 2 });
  up.on("httpUploadProgress", (p) => parts.push(p.part));
  await up.done();
  assert.ok(new Set(parts).size >= 2, "expected a multipart upload with >= 2 parts");
  const got = Buffer.from(await (await s3.send(new GetObjectCommand({ Bucket, Key: "big.bin" }))).Body.transformToByteArray());
  assert.ok(got.equals(payload));
  await s3.send(new DeleteObjectCommand({ Bucket, Key: "big.bin" }));
  await s3.send(new DeleteBucketCommand({ Bucket }));
});

test("s3_presigned_url_get", async () => {
  const Bucket = `sdk-js-${RUN}-presign`;
  await s3.send(new CreateBucketCommand({ Bucket }));
  await s3.send(new PutObjectCommand({ Bucket, Key: "k.txt", Body: "presigned-body" }));
  const url = await getSignedUrl(s3, new GetObjectCommand({ Bucket, Key: "k.txt" }), { expiresIn: 300 });
  const res = await fetch(url);
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "presigned-body");
  await s3.send(new DeleteObjectCommand({ Bucket, Key: "k.txt" }));
  await s3.send(new DeleteBucketCommand({ Bucket }));
});

test("dynamodb_documentclient_put_get_query_scan_pagination", async () => {
  const TableName = `sdk-js-${RUN}-items`;
  await ddb.send(new CreateTableCommand({
    TableName,
    AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }, { AttributeName: "sk", AttributeType: "N" }],
    KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
    BillingMode: "PAY_PER_REQUEST",
  }));
  await waitActive(TableName);
  for (let i = 0; i < 5; i++) await doc.send(new PutCommand({ TableName, Item: { pk: "u1", sk: i, v: `v${i}`, tags: ["a", { n: i }] } }));
  await doc.send(new PutCommand({ TableName, Item: { pk: "u2", sk: 0 } }));
  const got = (await doc.send(new GetCommand({ TableName, Key: { pk: "u1", sk: 3 } }))).Item;
  assert.deepEqual(got, { pk: "u1", sk: 3, v: "v3", tags: ["a", { n: 3 }] });
  const items = [];
  let start;
  let pages = 0;
  do {
    const r = await doc.send(new QueryCommand({ TableName, KeyConditionExpression: "pk = :p", ExpressionAttributeValues: { ":p": "u1" }, Limit: 2, ExclusiveStartKey: start }));
    pages++;
    items.push(...r.Items);
    start = r.LastEvaluatedKey;
  } while (start);
  assert.deepEqual(items.map((i) => i.sk), [0, 1, 2, 3, 4]);
  assert.ok(pages >= 3);
  let total = 0;
  start = undefined;
  do {
    const r = await doc.send(new ScanCommand({ TableName, Limit: 2, ExclusiveStartKey: start }));
    total += r.Items.length;
    start = r.LastEvaluatedKey;
  } while (start);
  assert.equal(total, 6);
  await ddb.send(new DeleteTableCommand({ TableName }));
});

test("dynamodb_conditional_put_failure_code", async () => {
  const TableName = `sdk-js-${RUN}-cond`;
  await hashTable(TableName);
  await waitActive(TableName);
  const put = () => doc.send(new PutCommand({ TableName, Item: { id: "1" }, ConditionExpression: "attribute_not_exists(id)" }));
  await put();
  assert.equal(await errName(put()), "ConditionalCheckFailedException");
  await ddb.send(new DeleteTableCommand({ TableName }));
});

test("dynamodb_transact_write_items", async () => {
  const TableName = `sdk-js-${RUN}-txn`;
  await hashTable(TableName);
  await waitActive(TableName);
  await ddb.send(new TransactWriteItemsCommand({ TransactItems: [
    { Put: { TableName, Item: { id: { S: "a" }, n: { N: "1" } } } },
    { Put: { TableName, Item: { id: { S: "b" }, n: { N: "2" } } } },
  ] }));
  assert.equal((await doc.send(new GetCommand({ TableName, Key: { id: "b" } }))).Item.n, 2);
  const name = await errName(ddb.send(new TransactWriteItemsCommand({ TransactItems: [
    { Put: { TableName, Item: { id: { S: "c" } } } },
    { ConditionCheck: { TableName, Key: { id: { S: "a" } }, ConditionExpression: "n = :x", ExpressionAttributeValues: { ":x": { N: "99" } } } },
  ] })));
  assert.equal(name, "TransactionCanceledException");
  assert.equal((await doc.send(new GetCommand({ TableName, Key: { id: "c" } }))).Item, undefined);
  await ddb.send(new DeleteTableCommand({ TableName }));
});

test("dynamodb_streams_describe", async () => {
  const TableName = `sdk-js-${RUN}-stream`;
  const r = await hashTable(TableName, { StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" } });
  const arn = r.TableDescription.LatestStreamArn;
  assert.ok(arn, "LatestStreamArn missing from CreateTable");
  await waitActive(TableName);
  const list = await streams.send(new ListStreamsCommand({ TableName }));
  assert.ok(list.Streams.some((s) => s.StreamArn === arn));
  const d = (await streams.send(new DescribeStreamCommand({ StreamArn: arn }))).StreamDescription;
  assert.equal(d.StreamViewType, "NEW_AND_OLD_IMAGES");
  assert.ok(d.Shards.length > 0);
  await ddb.send(new DeleteTableCommand({ TableName }));
});

test("sqs_send_receive_delete", async () => {
  const [QueueUrl] = await makeQueue(`sdk-js-${RUN}-std`);
  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "hello", MessageAttributes: { a: { DataType: "String", StringValue: "b" } } }));
  const [m] = await receiveN(QueueUrl, 1);
  assert.equal(m.Body, "hello");
  await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle: m.ReceiptHandle }));
  const a = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ["ApproximateNumberOfMessages"] }));
  assert.equal(a.Attributes.ApproximateNumberOfMessages, "0");
  await sqs.send(new DeleteQueueCommand({ QueueUrl }));
});

test("sqs_fifo_deduplication", async () => {
  const [QueueUrl] = await makeQueue(`sdk-js-${RUN}.fifo`, { FifoQueue: "true" });
  for (const d of ["dup-1", "dup-1", "dup-2"]) {
    await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: d === "dup-1" ? "once" : "twice", MessageGroupId: "g", MessageDeduplicationId: d }));
  }
  const msgs = await receiveN(QueueUrl, 2);
  await sleep(500);
  msgs.push(...((await sqs.send(new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 10 }))).Messages ?? []));
  assert.deepEqual(msgs.map((m) => m.Body), ["once", "twice"]);
  await sqs.send(new DeleteQueueCommand({ QueueUrl }));
});

test("sns_to_sqs_fanout", async () => {
  const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: `sdk-js-${RUN}-topic` }));
  const urls = [];
  for (let i = 0; i < 2; i++) {
    const [QueueUrl, arn] = await makeQueue(`sdk-js-${RUN}-fan${i}`);
    await sqs.send(new SetQueueAttributesCommand({ QueueUrl, Attributes: { Policy: allowAll } }));
    await sns.send(new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: arn }));
    urls.push(QueueUrl);
  }
  await sns.send(new PublishCommand({ TopicArn, Message: "fanout-msg", Subject: "subj" }));
  for (const u of urls) {
    const body = JSON.parse((await receiveN(u, 1))[0].Body);
    assert.equal(body.Type, "Notification");
    assert.equal(body.Message, "fanout-msg");
    assert.equal(body.TopicArn, TopicArn);
  }
  for (const QueueUrl of urls) await sqs.send(new DeleteQueueCommand({ QueueUrl }));
  await sns.send(new DeleteTopicCommand({ TopicArn }));
});

test("lambda_create_and_invoke", async () => {
  if (process.env.GS_SKIP_LAMBDA === "1") throw new Skip("GS_SKIP_LAMBDA=1");
  try {
    execFileSync("docker", ["info"], { stdio: "ignore" });
  } catch {
    throw new Skip("docker unavailable");
  }
  const zip = nodeZip("index.js", "exports.handler = async (event) => ({ echo: event, ok: true });\n");
  const { Role } = await iam.send(new CreateRoleCommand({ RoleName: `sdk-js-${RUN}-lamrole`, AssumeRolePolicyDocument: trust("lambda.amazonaws.com") }));
  const FunctionName = `sdk-js-${RUN}-fn`;
  await lambda.send(new CreateFunctionCommand({ FunctionName, Runtime: "nodejs22.x", Role: Role.Arn, Handler: "index.handler", Code: { ZipFile: zip } }));
  await waitFor(async () => ((await lambda.send(new GetFunctionCommand({ FunctionName }))).Configuration.State ?? "Active") === "Active", 60000);
  const r = await lambda.send(new InvokeCommand({ FunctionName, Payload: JSON.stringify({ x: 1 }) }));
  assert.equal(r.StatusCode, 200);
  assert.equal(r.FunctionError, undefined);
  assert.deepEqual(JSON.parse(Buffer.from(r.Payload).toString()), { echo: { x: 1 }, ok: true });
  await lambda.send(new DeleteFunctionCommand({ FunctionName }));
});

test("sts_get_caller_identity", async () => {
  const r = await sts.send(new GetCallerIdentityCommand({}));
  assert.match(r.Account, /^\d{12}$/);
  assert.ok(r.Arn.startsWith("arn:aws:"));
});

test("iam_create_role", async () => {
  const RoleName = `sdk-js-${RUN}-role`;
  const policy = trust("ec2.amazonaws.com");
  const { Role } = await iam.send(new CreateRoleCommand({ RoleName, AssumeRolePolicyDocument: policy, Path: "/sdk/" }));
  assert.ok(Role.Arn.endsWith(`:role/sdk/${RoleName}`));
  const got = (await iam.send(new GetRoleCommand({ RoleName }))).Role;
  assert.equal(got.Arn, Role.Arn);
  assert.deepEqual(JSON.parse(decodeURIComponent(got.AssumeRolePolicyDocument)), JSON.parse(policy));
  await iam.send(new DeleteRoleCommand({ RoleName }));
  assert.equal(await errName(iam.send(new GetRoleCommand({ RoleName }))), "NoSuchEntityException");
});

test("secretsmanager_roundtrip", async () => {
  const Name = `sdk-js-${RUN}-secret`;
  await sm.send(new CreateSecretCommand({ Name, SecretString: "s3cr3t" }));
  assert.equal((await sm.send(new GetSecretValueCommand({ SecretId: Name }))).SecretString, "s3cr3t");
  await sm.send(new DeleteSecretCommand({ SecretId: Name, ForceDeleteWithoutRecovery: true }));
  assert.equal(await errName(sm.send(new GetSecretValueCommand({ SecretId: Name }))), "ResourceNotFoundException");
});

test("kms_encrypt_decrypt", async () => {
  const { KeyMetadata } = await kms.send(new CreateKeyCommand({ Description: `sdk-js-${RUN}` }));
  const { CiphertextBlob } = await kms.send(new EncryptCommand({ KeyId: KeyMetadata.KeyId, Plaintext: Buffer.from("plain-data") }));
  const r = await kms.send(new DecryptCommand({ CiphertextBlob }));
  assert.equal(Buffer.from(r.Plaintext).toString(), "plain-data");
});

test("cloudwatch_logs_put_get", async () => {
  const logGroupName = `/sdk-js/${RUN}`;
  const logStreamName = "stream-1";
  await logs.send(new CreateLogGroupCommand({ logGroupName }));
  await logs.send(new CreateLogStreamCommand({ logGroupName, logStreamName }));
  const now = Date.now();
  await logs.send(new PutLogEventsCommand({ logGroupName, logStreamName, logEvents: [{ timestamp: now, message: "one" }, { timestamp: now + 1, message: "two" }] }));
  const r = await logs.send(new GetLogEventsCommand({ logGroupName, logStreamName, startFromHead: true }));
  assert.deepEqual(r.events.map((e) => e.message), ["one", "two"]);
  await logs.send(new DeleteLogGroupCommand({ logGroupName }));
});

test("eventbridge_put_events_to_sqs", async () => {
  const [QueueUrl, arn] = await makeQueue(`sdk-js-${RUN}-ebq`);
  await sqs.send(new SetQueueAttributesCommand({ QueueUrl, Attributes: { Policy: allowAll } }));
  const Name = `sdk-js-${RUN}-rule`;
  await eb.send(new PutRuleCommand({ Name, EventPattern: JSON.stringify({ source: ["sdk.test"] }), State: "ENABLED" }));
  assert.equal((await eb.send(new PutTargetsCommand({ Rule: Name, Targets: [{ Id: "q", Arn: arn }] }))).FailedEntryCount, 0);
  const r = await eb.send(new PutEventsCommand({ Entries: [{ Source: "sdk.test", DetailType: "t", Detail: JSON.stringify({ n: 1 }) }] }));
  assert.equal(r.FailedEntryCount, 0);
  const body = JSON.parse((await receiveN(QueueUrl, 1))[0].Body);
  assert.equal(body.source, "sdk.test");
  assert.deepEqual(body.detail, { n: 1 });
  await eb.send(new RemoveTargetsCommand({ Rule: Name, Ids: ["q"] }));
  await eb.send(new DeleteRuleCommand({ Name }));
  await sqs.send(new DeleteQueueCommand({ QueueUrl }));
});

test("stepfunctions_start_and_describe", async () => {
  const { Role } = await iam.send(new CreateRoleCommand({ RoleName: `sdk-js-${RUN}-sfnrole`, AssumeRolePolicyDocument: trust("states.amazonaws.com") }));
  const definition = JSON.stringify({ StartAt: "P", States: { P: { Type: "Pass", Result: { done: true }, End: true } } });
  const { stateMachineArn } = await sfn.send(new CreateStateMachineCommand({ name: `sdk-js-${RUN}-sm`, definition, roleArn: Role.Arn }));
  const { executionArn } = await sfn.send(new StartExecutionCommand({ stateMachineArn, input: JSON.stringify({ in: 1 }) }));
  const d = await waitFor(async () => {
    const x = await sfn.send(new DescribeExecutionCommand({ executionArn }));
    return x.status === "RUNNING" ? null : x;
  });
  assert.equal(d.status, "SUCCEEDED");
  assert.deepEqual(JSON.parse(d.output), { done: true });
  await sfn.send(new DeleteStateMachineCommand({ stateMachineArn }));
});

// Minimal single-entry zip writer (deflate) so no extra dependency is needed.
function nodeZip(name, content) {
  const data = Buffer.from(content);
  const comp = deflateRawSync(data);
  const crc = crc32(data);
  const n = Buffer.from(name);
  const lh = Buffer.alloc(30);
  lh.writeUInt32LE(0x04034b50, 0);
  lh.writeUInt16LE(20, 4);
  lh.writeUInt16LE(8, 8);
  lh.writeUInt32LE(crc, 14);
  lh.writeUInt32LE(comp.length, 18);
  lh.writeUInt32LE(data.length, 22);
  lh.writeUInt16LE(n.length, 26);
  const local = Buffer.concat([lh, n, comp]);
  const ch = Buffer.alloc(46);
  ch.writeUInt32LE(0x02014b50, 0);
  ch.writeUInt16LE(20, 4);
  ch.writeUInt16LE(20, 6);
  ch.writeUInt16LE(8, 10);
  ch.writeUInt32LE(crc, 16);
  ch.writeUInt32LE(comp.length, 20);
  ch.writeUInt32LE(data.length, 24);
  ch.writeUInt16LE(n.length, 28);
  const central = Buffer.concat([ch, n]);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(1, 8);
  end.writeUInt16LE(1, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(local.length, 16);
  return Buffer.concat([local, central, end]);
}
function crc32(buf) {
  let c = ~0;
  for (const b of buf) {
    c ^= b;
    for (let k = 0; k < 8; k++) c = (c >>> 1) ^ (0xedb88320 & -(c & 1));
  }
  return ~c >>> 0;
}

const only = process.argv.slice(2);
let failed = 0;
let ran = 0;
for (const [name, fn] of cases) {
  if (only.length && !only.includes(name)) continue;
  ran++;
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (e) {
    if (e instanceof Skip) {
      ran--;
      console.log(`SKIP ${name}: ${e.message}`);
      continue;
    }
    failed++;
    console.log(`FAIL ${name}: ${e.name}: ${e.message}`);
  }
}
console.log(`node: ${ran - failed} ok, ${failed} failed`);
process.exit(failed ? 1 : 0);
