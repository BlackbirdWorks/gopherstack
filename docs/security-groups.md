# Security group enforcement

By default Gopherstack stores EC2 security groups but does not act on them: every container-backed endpoint is
reachable from anywhere on the host. `--enforce-security-groups` (`GOPHERSTACK_ENFORCE_SECURITY_GROUPS=true`) makes
each new client connection to those endpoints pass a check against the live ingress rules of the resource's
security groups. With the flag off nothing changes: containers keep publishing their ports directly.

Rules are read on every connection, so `AuthorizeSecurityGroupIngress`, `RevokeSecurityGroupIngress`,
`ModifyInstanceAttribute` (groups), `ModifyDBInstance`, `UpdateCluster` and friends apply to the next connection.
Connections already open are not cut when a rule is revoked.

## What is enforced

| Service | Endpoint checked | Rule port matched |
|---------|------------------|-------------------|
| EC2 (`--ec2-provider docker`) | each published instance port and the SSH port | the container port |
| RDS / Aurora | instance, cluster and custom cluster endpoints | the engine port |
| DocumentDB | cluster endpoint, with or without TLS | 27017 (the requested port) |
| MSK (`--kafka-engine docker`) | plaintext, TLS, SCRAM and IAM listeners | 9092 / 9094 / 9096 / 9098 |
| ElastiCache and MemoryDB (embedded) | cluster endpoint | 6379 (the requested port) |
| Amazon MQ (docker) | every broker and console port | the AWS port (5672, 61617, 15671, ...) |
| Neptune | cluster port (Gremlin, openCypher, Bolt) | 8182 (the requested port) |
| ECS tasks | host-port mappings of awsvpc tasks that carry security groups | the container port |

A rule matches on protocol, port range, and the client's address (IPv4 and IPv6 CIDRs, managed prefix lists) or its
security groups (rules that reference another group match clients whose network interface or instance carries that
group). The rule port is always the service's real port, not the random host port Docker or the port allocator
assigned.

Resources created without security groups get the VPC's default group, as in AWS. The default group carries the
implicit "allow all from members of itself" rule, so a client whose network interface or instance is in the default
group reaches a default-group resource; any other client needs an explicit rule. The implicit rule is modelled at
evaluation time: it is not listed by `DescribeSecurityGroupRules` and cannot be revoked.

## How it works

Services that already front their endpoint (RDS proxies, the DocumentDB and Neptune TLS/openCypher fronts, the MSK
IAM proxy) check the client before serving it. Everywhere else the container is published on a loopback-only
port and a small TCP relay listens on the port clients are told about. The relay refuses a client without forwarding
a byte, so a refused connection looks like a connection that was accepted and immediately closed.

Clients connecting from the host appear as `127.0.0.1`. To test, authorize `127.0.0.1/32`.

## What is not enforced

- Egress rules. Only ingress is evaluated.
- Network ACLs, route tables and subnet reachability.
- UDP. EC2 instances do not publish UDP ports when the flag is on. All-traffic (`-1`) rules publish no ports on EC2
  instances; give the rule an explicit TCP range.
- EC2 ports that were not named by any ingress rule when the instance launched. Docker cannot publish a port on a
  running container, so a rule added later for a new port is not reachable; change to a port already declared, or
  relaunch.
- Lambda. AWS does not put security groups in front of invocation, so function URLs and invokes are not filtered.
- ECS tasks without an awsvpc security-group list, and bridge or host mode tasks.
- ElastiCache and MemoryDB clusters restored from a snapshot before the flag takes effect, and `docker`-mode
  ElastiCache/MemoryDB, which have no server.
- Traffic between containers on a Docker network: only the host-published endpoints are guarded.
- Services of the emulator talking to a resource themselves (probes, Lambda event source mappings from MQ) use the
  loopback port behind the relay and are not filtered.
