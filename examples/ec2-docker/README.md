# ec2-docker

`RunInstances` starts a real Docker container with `sshd`. You SSH into it with the key pair you created.

## What it shows

1. `aws ec2 create-key-pair` and save the PEM.
2. `aws ec2 run-instances` launches a container through the `docker` EC2 provider. The public key goes into `/home/ec2-user/.ssh/authorized_keys`.
3. Poll `describe-instances` until the instance is `running` and has a `PublicDnsName` and the `gopherstack:ssh-host` and `gopherstack:ssh-port` tags.
4. `dig @127.0.0.1 -p 10053 +short <PublicDnsName>` resolves the synthetic hostname through the embedded DNS server.
5. `ssh -p <ssh-port> ec2-user@<ip>` runs a command.
6. `terminate-instances` and `delete-key-pair`.

| Port | Use |
|------|-----|
| `8000` | AWS API |
| `10053/udp` | Embedded DNS (`ec2-<id>.compute-1.amazonaws.com`) |
| `22000`-`22019` | SSH ports mapped from instance containers |

`demo.sh` runs on the host, not in the compose stack.

## Prerequisites

- Docker, AWS CLI v2, `jq`, `dig`, `ssh`.

> [!WARNING]
> The compose file mounts `/var/run/docker.sock`, runs Gopherstack as root, and binds instance SSH ports to `0.0.0.0` (`EC2_DOCKER_SSH_HOST_IP`). Anything that can reach the host can reach those instances. The key pair is stored at `/tmp/ec2-docker-demo.pem` while the demo runs.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

| Variable | Default |
|----------|---------|
| `ENDPOINT` | `http://localhost:8000` |
| `DNS_ADDR`, `DNS_PORT` | `127.0.0.1`, `10053` |
| `KEY_NAME` | `ec2-docker-demo` |
| `KEY_FILE` | `/tmp/<KEY_NAME>.pem` |
| `AMI_ID` | `ami-0c55b159cbfafe1f0` |

## Expected output

```
ec2-<id>.compute-1.amazonaws.com -> 127.0.0.1
sshd is up after 8s
hello world from i-<id>
=== SUCCESS: round-tripped through the docker-backed EC2 instance ===
```

The script terminates the instance and deletes the key pair on exit.

## Get the SSH endpoint from the API

`DescribeInstances` returns the hostname and two Gopherstack tags:

```bash
aws --endpoint-url http://localhost:8000 ec2 describe-instances \
  --instance-ids i-<id> \
  --query 'Reservations[0].Instances[0].{
    Dns:  PublicDnsName,
    Host: Tags[?Key==`gopherstack:ssh-host`]|[0].Value,
    Port: Tags[?Key==`gopherstack:ssh-port`]|[0].Value
  }'
```

## Images

With the `docker` provider, an unknown AMI id fails with `InvalidAMIID.NotFound`.

- Seeded public AMIs map to images by name: Amazon Linux 2023 to `amazonlinux:2023`, Amazon Linux 2 to `amazonlinux:2`, Amazon Linux 1 to `amazonlinux:1`, Ubuntu 20.04, 22.04 and 24.04 to `ubuntu:<version>`. Windows and Bottlerocket use `--ec2-docker-image`.
- `CreateImage` on a running instance runs `docker commit` and tags the result `gopherstack-ec2/<name>:<ami-id>`.
- A local image tagged `gopherstack-ec2/<name>:ami-<8+ hex digits>` is registered as a self-owned AMI on the next `DescribeImages` or `RunInstances`. Images in that namespace are never pulled.
- `RegisterImage` with `ImageLocation=docker://<reference>` binds the AMI to that image. `CopyImage` copies the binding.

Provider flags (`--ec2-docker-*`, EBS volumes) are in the [EC2 guide](../../docs/services/ec2.md).

## Clean up

```bash
docker compose down -v
```
