# RDS — Relational Database Service

Stub RDS implementation for tracking DB instance metadata. Does not start actual database processes.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateDBInstance` | Register a new DB instance record |
| `DeleteDBInstance` | Remove a DB instance record |
| `DescribeDBInstances` | List DB instances with optional filters |
| `ModifyDBInstance` | Update DB instance configuration |
| `CreateDBSnapshot` | Create a snapshot record |
| `DescribeDBSnapshots` | List snapshots |
| `DeleteDBSnapshot` | Delete a snapshot record |
| `CreateDBSubnetGroup` | Create a subnet group record |
| `DescribeDBSubnetGroups` | List subnet groups |
| `DeleteDBSubnetGroup` | Delete a subnet group |
| `ListTagsForResource` | List tags on a resource |
| `AddTagsToResource` | Add tags |
| `RemoveTagsFromResource` | Remove tags |

## AWS CLI Examples

```bash
# Create a DB instance
aws --endpoint-url http://localhost:8000 rds create-db-instance \
    --db-instance-identifier my-db \
    --db-instance-class db.t3.micro \
    --engine mysql \
    --master-username admin \
    --master-user-password password \
    --allocated-storage 20

# Describe instances
aws --endpoint-url http://localhost:8000 rds describe-db-instances

# Create a snapshot
aws --endpoint-url http://localhost:8000 rds create-db-snapshot \
    --db-instance-identifier my-db \
    --db-snapshot-identifier my-db-snap-1

# Create a subnet group
aws --endpoint-url http://localhost:8000 rds create-db-subnet-group \
    --db-subnet-group-name my-subnet-group \
    --db-subnet-group-description "Test subnet group" \
    --subnet-ids '["subnet-12345678","subnet-87654321"]'
```

## Known Limitations

- RDS does **not** start a real database process. The endpoint returned is a synthetic hostname; no actual MySQL/PostgreSQL connection is possible unless DNS is configured to resolve it to a real database.
- Multi-AZ, Read Replicas, and Parameter Groups are accepted but not enforced.
- Automatic backups and maintenance windows are recorded but not acted upon.

## DNS Integration

With the DNS server enabled (`--dns-addr :10053`), RDS instance hostnames are automatically registered and resolve to `127.0.0.1` (or the configured `--dns-resolve-ip`). See [DNS setup](../architecture/dns.md).
