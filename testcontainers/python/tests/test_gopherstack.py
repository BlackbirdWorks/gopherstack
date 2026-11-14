import os

import pytest

from testcontainers_gopherstack import GopherstackContainer

IMAGE = os.environ.get("GOPHERSTACK_TEST_IMAGE", "ghcr.io/blackbirdworks/gopherstack:latest")


@pytest.fixture(scope="module")
def gs():
    try:
        c = GopherstackContainer(IMAGE).with_services("s3", "sqs", "azureblob")
        c.start()
    except Exception as exc:  # noqa: BLE001
        if "docker" in str(exc).lower() and "not" in str(exc).lower():
            pytest.skip("docker unavailable")
        raise
    yield c
    c.stop()


def test_s3_roundtrip(gs):
    s3 = gs.get_aws_client("s3")
    s3.create_bucket(Bucket="tc-py")
    s3.put_object(Bucket="tc-py", Key="k", Body=b"hello")
    assert s3.get_object(Bucket="tc-py", Key="k")["Body"].read() == b"hello"


def test_sqs_roundtrip(gs):
    sqs = gs.get_aws_client("sqs")
    url = sqs.create_queue(QueueName="tc-py")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="ping")
    msgs = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=2)["Messages"]
    assert msgs[0]["Body"] == "ping"


def test_helpers(gs):
    assert "AccountName=devstoreaccount1" in gs.get_azure_connection_string()
    assert gs.get_gcp_endpoint("storage").count(":") == 1
    with pytest.raises(ValueError):
        gs.get_gcp_endpoint("nope")


@pytest.mark.parametrize("method", ["with_persistence", "with_docker_socket"])
def test_root_user_keeps_earlier_kwargs(method, tmp_path):
    c = GopherstackContainer(IMAGE, mem_limit="256m")
    c.with_user("1000:1000")
    getattr(c, method)(*([str(tmp_path)] if method == "with_persistence" else []))
    assert c._kwargs == {"mem_limit": "256m", "user": "0:0"}


def test_with_user_overrides_and_merges(tmp_path):
    c = GopherstackContainer(IMAGE, mem_limit="256m").with_persistence(str(tmp_path)).with_user("1000:1000")
    assert c._kwargs == {"mem_limit": "256m", "user": "1000:1000"}
