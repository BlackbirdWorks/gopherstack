"""boto3 cross-SDK compatibility suite for gopherstack's AWS services.

Env: AWS_ENDPOINT_URL (default http://localhost:8000), GS_SKIP_LAMBDA=1 to skip Lambda.
Prints PASS/FAIL per case and exits non-zero on any failure.
"""

import io
import json
import os
import subprocess
import sys
import time
import traceback
import urllib.request
import uuid
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:8000")
REGION = "us-east-1"
RUN = uuid.uuid4().hex[:8]
CFG = Config(retries={"max_attempts": 1}, s3={"addressing_style": "path"})


def client(name):
    return boto3.client(
        name,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=CFG,
    )


def err_code(exc):
    return exc.response["Error"]["Code"]


def wait_for(fn, timeout=30.0, interval=0.25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(interval)
    raise AssertionError("timed out waiting for condition")


def receive_all(sqs, url, want, timeout=20.0):
    msgs = []

    def poll():
        r = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        msgs.extend(r.get("Messages", []))
        return len(msgs) >= want

    wait_for(poll, timeout, 0.0)
    return msgs


CASES = []


def case(fn):
    CASES.append(fn)
    return fn


def make_queue(sqs, name, **attrs):
    kw = {"QueueName": name}
    if attrs:
        kw["Attributes"] = attrs
    url = sqs.create_queue(**kw)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


ALLOW_ALL_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "sqs:SendMessage", "Resource": "*"}],
    }
)


@case
def s3_bucket_and_object_roundtrip():
    s3 = client("s3")
    b = f"sdk-py-{RUN}-rt"
    s3.create_bucket(Bucket=b)
    assert b in [x["Name"] for x in s3.list_buckets()["Buckets"]]
    s3.put_object(Bucket=b, Key="a/hello.txt", Body=b"hello", ContentType="text/plain", Metadata={"k": "v"})
    got = s3.get_object(Bucket=b, Key="a/hello.txt")
    assert got["Body"].read() == b"hello"
    assert got["ContentType"] == "text/plain" and got["Metadata"] == {"k": "v"}
    assert s3.head_object(Bucket=b, Key="a/hello.txt")["ContentLength"] == 5
    try:
        s3.get_object(Bucket=b, Key="missing")
        raise AssertionError("expected NoSuchKey")
    except ClientError as e:
        assert err_code(e) == "NoSuchKey", err_code(e)
    s3.delete_object(Bucket=b, Key="a/hello.txt")
    s3.delete_bucket(Bucket=b)
    try:
        s3.head_bucket(Bucket=b)
        raise AssertionError("bucket should be gone")
    except ClientError as e:
        assert err_code(e) in ("404", "NoSuchBucket"), err_code(e)


@case
def s3_list_objects_v2_pagination():
    s3 = client("s3")
    b = f"sdk-py-{RUN}-page"
    s3.create_bucket(Bucket=b)
    keys = [f"p/{i:02d}" for i in range(5)]
    for k in keys:
        s3.put_object(Bucket=b, Key=k, Body=k.encode())
    seen, token, pages = [], None, 0
    while True:
        kw = {"Bucket": b, "Prefix": "p/", "MaxKeys": 2}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        pages += 1
        seen += [o["Key"] for o in r.get("Contents", [])]
        if not r["IsTruncated"]:
            break
        token = r["NextContinuationToken"]
    assert seen == keys, seen
    assert pages == 3, pages
    r = s3.list_objects_v2(Bucket=b, Delimiter="/")
    assert [p["Prefix"] for p in r["CommonPrefixes"]] == ["p/"]
    for k in keys:
        s3.delete_object(Bucket=b, Key=k)
    s3.delete_bucket(Bucket=b)


@case
def s3_multipart_upload():
    s3 = client("s3")
    b = f"sdk-py-{RUN}-mpu"
    s3.create_bucket(Bucket=b)
    part1 = b"a" * (5 * 1024 * 1024)
    part2 = b"tail"
    up = s3.create_multipart_upload(Bucket=b, Key="big.bin")
    parts = []
    for n, body in ((1, part1), (2, part2)):
        r = s3.upload_part(Bucket=b, Key="big.bin", UploadId=up["UploadId"], PartNumber=n, Body=body)
        parts.append({"ETag": r["ETag"], "PartNumber": n})
    s3.complete_multipart_upload(
        Bucket=b, Key="big.bin", UploadId=up["UploadId"], MultipartUpload={"Parts": parts}
    )
    got = s3.get_object(Bucket=b, Key="big.bin")["Body"].read()
    assert got == part1 + part2 and len(got) == len(part1) + 4
    s3.delete_object(Bucket=b, Key="big.bin")
    s3.delete_bucket(Bucket=b)


@case
def s3_presigned_url_get():
    s3 = client("s3")
    b = f"sdk-py-{RUN}-presign"
    s3.create_bucket(Bucket=b)
    s3.put_object(Bucket=b, Key="k.txt", Body=b"presigned-body")
    url = s3.generate_presigned_url("get_object", Params={"Bucket": b, "Key": "k.txt"}, ExpiresIn=300)
    with urllib.request.urlopen(url, timeout=10) as resp:
        assert resp.status == 200
        assert resp.read() == b"presigned-body"
    s3.delete_object(Bucket=b, Key="k.txt")
    s3.delete_bucket(Bucket=b)


def ddb_wait_active(ddb, name):
    wait_for(lambda: ddb.describe_table(TableName=name)["Table"]["TableStatus"] == "ACTIVE")


@case
def dynamodb_put_get_query_scan_pagination():
    ddb = client("dynamodb")
    t = f"sdk-py-{RUN}-items"
    ddb.create_table(
        TableName=t,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb_wait_active(ddb, t)
    for i in range(5):
        ddb.put_item(TableName=t, Item={"pk": {"S": "u1"}, "sk": {"N": str(i)}, "v": {"S": f"v{i}"}})
    ddb.put_item(TableName=t, Item={"pk": {"S": "u2"}, "sk": {"N": "0"}})
    got = ddb.get_item(TableName=t, Key={"pk": {"S": "u1"}, "sk": {"N": "3"}})["Item"]
    assert got["v"]["S"] == "v3"
    q = {
        "TableName": t,
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "u1"}},
        "Limit": 2,
    }
    items, pages, start = [], 0, None
    while True:
        r = ddb.query(**q, **({"ExclusiveStartKey": start} if start else {}))
        pages += 1
        items += r["Items"]
        start = r.get("LastEvaluatedKey")
        if not start:
            break
    assert [i["sk"]["N"] for i in items] == ["0", "1", "2", "3", "4"], items
    assert pages >= 3, pages
    total, start = 0, None
    while True:
        r = ddb.scan(TableName=t, Limit=2, **({"ExclusiveStartKey": start} if start else {}))
        total += len(r["Items"])
        start = r.get("LastEvaluatedKey")
        if not start:
            break
    assert total == 6, total
    ddb.delete_table(TableName=t)


@case
def dynamodb_conditional_put_failure_code():
    ddb = client("dynamodb")
    t = f"sdk-py-{RUN}-cond"
    ddb.create_table(
        TableName=t,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb_wait_active(ddb, t)
    ddb.put_item(TableName=t, Item={"id": {"S": "1"}}, ConditionExpression="attribute_not_exists(id)")
    try:
        ddb.put_item(TableName=t, Item={"id": {"S": "1"}}, ConditionExpression="attribute_not_exists(id)")
        raise AssertionError("expected conditional failure")
    except ClientError as e:
        assert err_code(e) == "ConditionalCheckFailedException", err_code(e)
    ddb.delete_table(TableName=t)


@case
def dynamodb_transact_write_items():
    ddb = client("dynamodb")
    t = f"sdk-py-{RUN}-txn"
    ddb.create_table(
        TableName=t,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb_wait_active(ddb, t)
    ddb.transact_write_items(
        TransactItems=[
            {"Put": {"TableName": t, "Item": {"id": {"S": "a"}, "n": {"N": "1"}}}},
            {"Put": {"TableName": t, "Item": {"id": {"S": "b"}, "n": {"N": "2"}}}},
        ]
    )
    assert ddb.get_item(TableName=t, Key={"id": {"S": "b"}})["Item"]["n"]["N"] == "2"
    try:
        ddb.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": t, "Item": {"id": {"S": "c"}}}},
                {
                    "ConditionCheck": {
                        "TableName": t,
                        "Key": {"id": {"S": "a"}},
                        "ConditionExpression": "n = :x",
                        "ExpressionAttributeValues": {":x": {"N": "99"}},
                    }
                },
            ]
        )
        raise AssertionError("expected TransactionCanceledException")
    except ClientError as e:
        assert err_code(e) == "TransactionCanceledException", err_code(e)
    assert "Item" not in ddb.get_item(TableName=t, Key={"id": {"S": "c"}})
    ddb.delete_table(TableName=t)


@case
def dynamodb_streams_describe():
    ddb = client("dynamodb")
    streams = client("dynamodbstreams")
    t = f"sdk-py-{RUN}-stream"
    r = ddb.create_table(
        TableName=t,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    arn = r["TableDescription"]["LatestStreamArn"]
    ddb_wait_active(ddb, t)
    assert arn in [s["StreamArn"] for s in streams.list_streams(TableName=t)["Streams"]]
    d = streams.describe_stream(StreamArn=arn)["StreamDescription"]
    assert d["StreamViewType"] == "NEW_AND_OLD_IMAGES" and d["TableName"] == t
    assert d["Shards"], "stream should have at least one shard"
    ddb.delete_table(TableName=t)


@case
def sqs_send_receive_delete():
    sqs = client("sqs")
    url, _ = make_queue(sqs, f"sdk-py-{RUN}-std")
    sqs.send_message(QueueUrl=url, MessageBody="hello", MessageAttributes={"a": {"DataType": "String", "StringValue": "b"}})
    msgs = receive_all(sqs, url, 1)
    assert msgs[0]["Body"] == "hello"
    sqs.delete_message(QueueUrl=url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
    assert attrs["ApproximateNumberOfMessages"] == "0"
    sqs.delete_queue(QueueUrl=url)


@case
def sqs_fifo_deduplication():
    sqs = client("sqs")
    url, _ = make_queue(sqs, f"sdk-py-{RUN}.fifo", FifoQueue="true")
    for _ in range(2):
        sqs.send_message(QueueUrl=url, MessageBody="once", MessageGroupId="g", MessageDeduplicationId="dup-1")
    sqs.send_message(QueueUrl=url, MessageBody="twice", MessageGroupId="g", MessageDeduplicationId="dup-2")
    msgs = receive_all(sqs, url, 2)
    time.sleep(0.5)
    r = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    msgs += r.get("Messages", [])
    assert [m["Body"] for m in msgs] == ["once", "twice"], [m["Body"] for m in msgs]
    sqs.delete_queue(QueueUrl=url)


@case
def sns_to_sqs_fanout():
    sns, sqs = client("sns"), client("sqs")
    topic = sns.create_topic(Name=f"sdk-py-{RUN}-topic")["TopicArn"]
    urls = []
    for i in range(2):
        url, arn = make_queue(sqs, f"sdk-py-{RUN}-fan{i}")
        sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": ALLOW_ALL_POLICY})
        sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn)
        urls.append(url)
    sns.publish(TopicArn=topic, Message="fanout-msg", Subject="subj")
    for url in urls:
        body = json.loads(receive_all(sqs, url, 1)[0]["Body"])
        assert body["Type"] == "Notification" and body["Message"] == "fanout-msg", body
        assert body["TopicArn"] == topic
    for url in urls:
        sqs.delete_queue(QueueUrl=url)
    sns.delete_topic(TopicArn=topic)


@case
def lambda_create_and_invoke():
    if os.environ.get("GS_SKIP_LAMBDA") == "1" or subprocess.run(
        ["docker", "info"], capture_output=True
    ).returncode != 0:
        raise Skip("docker unavailable")
    lam, iam = client("lambda"), client("iam")
    role = iam.create_role(
        RoleName=f"sdk-py-{RUN}-lamrole",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}
                ],
            }
        ),
    )["Role"]["Arn"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.py", "def handler(event, context):\n    return {'echo': event, 'ok': True}\n")
    name = f"sdk-py-{RUN}-fn"
    lam.create_function(
        FunctionName=name, Runtime="python3.12", Role=role, Handler="index.handler", Code={"ZipFile": buf.getvalue()}
    )
    wait_for(lambda: lam.get_function(FunctionName=name)["Configuration"].get("State", "Active") == "Active", 60)
    r = lam.invoke(FunctionName=name, Payload=json.dumps({"x": 1}).encode())
    out = json.loads(r["Payload"].read())
    assert r["StatusCode"] == 200 and "FunctionError" not in r, r
    assert out == {"echo": {"x": 1}, "ok": True}, out
    lam.delete_function(FunctionName=name)


@case
def sts_get_caller_identity():
    r = client("sts").get_caller_identity()
    assert len(r["Account"]) == 12 and r["Account"].isdigit(), r
    assert r["Arn"].startswith("arn:aws:"), r["Arn"]


@case
def iam_create_role():
    iam = client("iam")
    name = f"sdk-py-{RUN}-role"
    doc = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
    arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(doc), Path="/sdk/")["Role"]["Arn"]
    assert arn.endswith(f":role/sdk/{name}"), arn
    got = iam.get_role(RoleName=name)["Role"]
    assert got["Arn"] == arn and got["AssumeRolePolicyDocument"] == doc
    assert name in [r["RoleName"] for r in iam.list_roles(PathPrefix="/sdk/")["Roles"]]
    iam.delete_role(RoleName=name)
    try:
        iam.get_role(RoleName=name)
        raise AssertionError("role should be gone")
    except ClientError as e:
        assert err_code(e) == "NoSuchEntity", err_code(e)


@case
def secretsmanager_roundtrip():
    sm = client("secretsmanager")
    name = f"sdk-py-{RUN}-secret"
    arn = sm.create_secret(Name=name, SecretString="s3cr3t")["ARN"]
    assert sm.get_secret_value(SecretId=name)["SecretString"] == "s3cr3t"
    sm.put_secret_value(SecretId=arn, SecretString="rotated")
    assert sm.get_secret_value(SecretId=name)["SecretString"] == "rotated"
    sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
    try:
        sm.get_secret_value(SecretId=name)
        raise AssertionError("secret should be gone")
    except ClientError as e:
        assert err_code(e) == "ResourceNotFoundException", err_code(e)


@case
def kms_encrypt_decrypt():
    kms = client("kms")
    key = kms.create_key(Description=f"sdk-py-{RUN}")["KeyMetadata"]["KeyId"]
    ct = kms.encrypt(KeyId=key, Plaintext=b"plain-data")["CiphertextBlob"]
    assert ct != b"plain-data"
    r = kms.decrypt(CiphertextBlob=ct)
    assert r["Plaintext"] == b"plain-data" and key in r["KeyId"]


@case
def cloudwatch_logs_put_get():
    logs = client("logs")
    g, s = f"/sdk-py/{RUN}", "stream-1"
    logs.create_log_group(logGroupName=g)
    logs.create_log_stream(logGroupName=g, logStreamName=s)
    now = int(time.time() * 1000)
    logs.put_log_events(
        logGroupName=g,
        logStreamName=s,
        logEvents=[{"timestamp": now, "message": "one"}, {"timestamp": now + 1, "message": "two"}],
    )
    ev = logs.get_log_events(logGroupName=g, logStreamName=s, startFromHead=True)["events"]
    assert [e["message"] for e in ev] == ["one", "two"], ev
    logs.delete_log_group(logGroupName=g)


@case
def eventbridge_put_events_to_sqs():
    eb, sqs = client("events"), client("sqs")
    url, arn = make_queue(sqs, f"sdk-py-{RUN}-ebq")
    sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": ALLOW_ALL_POLICY})
    rule = f"sdk-py-{RUN}-rule"
    eb.put_rule(Name=rule, EventPattern=json.dumps({"source": ["sdk.test"]}), State="ENABLED")
    r = eb.put_targets(Rule=rule, Targets=[{"Id": "q", "Arn": arn}])
    assert r["FailedEntryCount"] == 0, r
    r = eb.put_events(
        Entries=[{"Source": "sdk.test", "DetailType": "t", "Detail": json.dumps({"n": 1})}]
    )
    assert r["FailedEntryCount"] == 0, r
    body = json.loads(receive_all(sqs, url, 1)[0]["Body"])
    assert body["source"] == "sdk.test" and body["detail"] == {"n": 1}, body
    eb.remove_targets(Rule=rule, Ids=["q"])
    eb.delete_rule(Name=rule)
    sqs.delete_queue(QueueUrl=url)


@case
def stepfunctions_start_and_describe():
    sfn, iam = client("stepfunctions"), client("iam")
    role = iam.create_role(
        RoleName=f"sdk-py-{RUN}-sfnrole",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
                ],
            }
        ),
    )["Role"]["Arn"]
    definition = {"StartAt": "P", "States": {"P": {"Type": "Pass", "Result": {"done": True}, "End": True}}}
    sm = sfn.create_state_machine(name=f"sdk-py-{RUN}-sm", definition=json.dumps(definition), roleArn=role)[
        "stateMachineArn"
    ]
    ex = sfn.start_execution(stateMachineArn=sm, input=json.dumps({"in": 1}))["executionArn"]
    d = wait_for(
        lambda: (lambda x: x if x["status"] != "RUNNING" else None)(sfn.describe_execution(executionArn=ex)), 30
    )
    assert d["status"] == "SUCCEEDED", d
    assert json.loads(d["output"]) == {"done": True}, d["output"]
    sfn.delete_state_machine(stateMachineArn=sm)


class Skip(Exception):
    pass


def main():
    only = sys.argv[1:]
    failed = 0
    for fn in CASES:
        if only and fn.__name__ not in only:
            continue
        try:
            fn()
            print(f"PASS {fn.__name__}", flush=True)
        except Skip as s:
            print(f"SKIP {fn.__name__}: {s}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(limit=3)
    print(f"python: {len(CASES) - failed} ok, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
