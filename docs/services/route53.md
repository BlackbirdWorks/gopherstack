# Route 53

In-memory Route 53 implementation supporting hosted zone and record set management.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateHostedZone` | Create a public or private hosted zone |
| `DeleteHostedZone` | Delete a hosted zone |
| `ListHostedZones` | List all hosted zones |
| `GetHostedZone` | Get hosted zone details |
| `ChangeResourceRecordSets` | Create, update, or delete DNS records |
| `ListResourceRecordSets` | List DNS records in a hosted zone |
| `ListTagsForResource` | List tags on a hosted zone |
| `ChangeTagsForResource` | Add or remove tags on a hosted zone |

## AWS CLI Examples

```bash
# Create a hosted zone
aws --endpoint-url http://localhost:8000 route53 create-hosted-zone \
    --name example.internal \
    --caller-reference $(date +%s)

# Create an A record
aws --endpoint-url http://localhost:8000 route53 change-resource-record-sets \
    --hosted-zone-id /hostedzone/Z0000000000000 \
    --change-batch '{
        "Changes": [{
            "Action": "CREATE",
            "ResourceRecordSet": {
                "Name": "api.example.internal",
                "Type": "A",
                "TTL": 60,
                "ResourceRecords": [{"Value": "127.0.0.1"}]
            }
        }]
    }'

# List record sets
aws --endpoint-url http://localhost:8000 route53 list-resource-record-sets \
    --hosted-zone-id /hostedzone/Z0000000000000

# List hosted zones
aws --endpoint-url http://localhost:8000 route53 list-hosted-zones
```

## Known Limitations

- DNS records are stored in memory but the embedded DNS server must be enabled (`--dns-addr :10053`) for them to resolve.
- DNSSEC, traffic policies, health checks, and routing policies (weighted, latency, geolocation) are not implemented.
- Private hosted zones and VPC associations are accepted but VPC filtering is not enforced.
- Alias records pointing to AWS resources (ELB, CloudFront, etc.) are stored but not resolved.
