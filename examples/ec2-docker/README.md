# EC2 docker provider example

This example shows the EC2 service running with the `docker` compute provider,
which spawns real Docker containers (one per `RunInstances` call) instead of
keeping a pure in-memory simulation. The launched container ships an OpenSSH
daemon and the key pair the caller created is wired into
`/home/ec2-user/.ssh/authorized_keys`, so callers can `ssh ec2-user@<ip>` and
get a real shell.

## What the demo does

1. Boots gopherstack with `EC2_PROVIDER=docker` and a shared compose network so
   the demo container and EC2-backed containers can talk by private IP.
2. `aws ec2 create-key-pair --key-name ec2-docker-demo` and saves the returned
   PEM private key.
3. `aws ec2 run-instances --image-id ami-... --instance-type t3.micro` to
   trigger a container launch through the docker compute provider.
4. Polls `aws ec2 describe-instances` until `State=running` and a
   `PrivateIpAddress` is reported.
5. SSHes in and runs `echo "hello world from $(hostname)"`.
6. `aws ec2 terminate-instances ...` and `delete-key-pair` to clean up.

## Run it

```sh
cd examples/ec2-docker
docker compose up --build --abort-on-container-exit
docker compose down --remove-orphans
```

You should see output similar to:

```
=== ssh ec2-user@172.18.0.4 'echo hello world from $(hostname)' ===
hello world from i-0123456789abcdef0
=== SUCCESS: round-tripped through the docker-backed EC2 instance ===
```

## Configuration knobs

The provider is configured via environment variables / CLI flags:

| Flag                         | Env var                    | Default        | Description                                                                          |
| ---------------------------- | -------------------------- | -------------- | ------------------------------------------------------------------------------------ |
| `--ec2-provider`             | `EC2_PROVIDER`             | `inmemory`     | Set to `docker` to enable the docker compute provider.                               |
| `--ec2-docker-image`         | `EC2_DOCKER_IMAGE`         | `amazonlinux:2`| Docker image used for new instances.                                                 |
| `--ec2-docker-network`       | `EC2_DOCKER_NETWORK`       | _(empty)_      | Docker network the launched containers attach to (empty = default bridge).           |
| `--ec2-docker-ssh-host-ip`   | `EC2_DOCKER_SSH_HOST_IP`   | `127.0.0.1`    | Host IP that mapped sshd ports bind to (use `0.0.0.0` to expose externally).         |
| `--ec2-docker-ssh-port-min`  | `EC2_DOCKER_SSH_PORT_MIN`  | `0`            | Lower bound of the host TCP port range used to map sshd (`0` = let Docker pick).     |
| `--ec2-docker-ssh-port-max`  | `EC2_DOCKER_SSH_PORT_MAX`  | `0`            | Upper bound of the host TCP port range used to map sshd.                             |

When `EC2_DOCKER_NETWORK` is set, you can reach the EC2-backed container
directly by its private IP from any other container on the same network — the
host port mapping is not strictly required.
