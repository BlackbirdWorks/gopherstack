# Gopherstack testcontainers module (Python)

```python
from testcontainers_gopherstack import GopherstackContainer

with GopherstackContainer().with_services("s3", "sqs") as gs:
    s3 = gs.get_aws_client("s3")          # boto3, static creds
    gs.get_endpoint()                      # http://host:port
    gs.get_azure_connection_string()
    gs.get_gcp_endpoint("storage")         # host:port
```

Also `with_persistence(dir)`, `with_docker_socket()` (see the
[socket warning](../README.md#docker-socket-warning)), `with_env(k, v)`.

```bash
pip install testcontainers boto3 pytest
GOPHERSTACK_TEST_IMAGE=gopherstack:tc-local python -m pytest tests
```
