# Security group enforcement

By default Gopherstack stores EC2 security groups but does not act on them. Every container-backed endpoint is reachable from anywhere on the host.

`--enforce-security-groups` (`GOPHERSTACK_ENFORCE_SECURITY_GROUPS=true`) checks each new client connection against the live ingress rules of the resource's security groups. With the flag off, containers publish their ports directly as before.

Rules are read on every connection, so `AuthorizeSecurityGroupIngress`, `RevokeSecurityGroupIngress`, `ModifyInstanceAttribute` (groups), `ModifyDBInstance`, `UpdateCluster` and similar calls apply to the next connection. Open connections are not cut when a rule is revoked.

## What is enforced

| Service | Endpoint checked | Rule port matched |
|---------|------------------|-------------------|
| EC2 (`--ec2-provider docker`) | Each published instance port and the SSH port | Container port |
| RDS / Aurora | Instance, cluster and custom cluster endpoints | Engine port |
| DocumentDB | Cluster endpoint, with or without TLS | 27017 |
| MSK (`--kafka-engine docker`) | Plaintext, TLS, SCRAM and IAM listeners | 9092 / 9094 / 9096 / 9098 |
| ElastiCache and MemoryDB (embedded) | Cluster endpoint | 6379 |
| Amazon MQ (docker) | Every broker and console port | AWS port (5672, 61617, 15671, ...) |
| Neptune | Cluster port (Gremlin, openCypher, Bolt) | 8182 |
| ECS tasks | Host-port mappings of awsvpc tasks that carry security groups | Container port |

A rule matches on protocol, port range, and either the client address (IPv4/IPv6 CIDRs, managed prefix lists) or the client's security groups. The rule port is always the service's real port, not the host port Docker or the port allocator assigned.

Resources created without security groups get the VPC default group, as in AWS. That group has the implicit "allow all from members of itself" rule, so a client whose network interface or instance is in the default group reaches a default-group resource. Any other client needs an explicit rule. The implicit rule is applied at evaluation time: `DescribeSecurityGroupRules` does not list it and it cannot be revoked.

## How it works

Services that already front their endpoint (RDS proxies, the DocumentDB and Neptune TLS and openCypher fronts, the MSK IAM proxy) check the client before serving it. Elsewhere, the container is published on a loopback-only port and a TCP relay listens on the port clients are told about. The relay refuses a client without forwarding a byte, so a refused connection looks like one accepted and immediately closed.

Clients on the host appear as `127.0.0.1`. To test, authorize `127.0.0.1/32`.

## Not enforced

> [!WARNING]
> Only ingress is evaluated. Egress rules, network ACLs, route tables and subnet reachability are ignored.

- UDP. EC2 instances publish no UDP ports when the flag is on. All-traffic (`-1`) rules publish no ports on EC2 instances; give the rule an explicit TCP range.
- EC2 ports not named by any ingress rule at launch. Docker cannot publish a port on a running container, so a rule added later for a new port stays unreachable. Relaunch the instance.
- Lambda. AWS puts no security groups in front of invocation, so function URLs and invokes are not filtered.
- ECS tasks without an awsvpc security-group list, and bridge or host mode tasks.
- ElastiCache and MemoryDB clusters restored from a snapshot before the flag took effect, and `docker`-mode ElastiCache/MemoryDB (no server).
- Traffic between containers on a Docker network. Only host-published endpoints are guarded.
- The emulator's own connections to a resource (probes, Lambda event source mappings from MQ). They use the loopback port behind the relay.
