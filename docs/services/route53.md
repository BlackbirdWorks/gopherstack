# Route 53

In-memory Route 53 with hosted zones, record sets and routing policies, health checks, DNSSEC, traffic policies, CIDR collections and query logging.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws route53 create-hosted-zone --name example.internal --caller-reference $(date +%s)

aws route53 change-resource-record-sets --hosted-zone-id /hostedzone/Z0000000000000 \
    --change-batch '{"Changes":[{"Action":"CREATE","ResourceRecordSet":{
        "Name":"api.example.internal","Type":"A","TTL":60,
        "ResourceRecords":[{"Value":"127.0.0.1"}]}}]}'

aws route53 list-resource-record-sets --hosted-zone-id /hostedzone/Z0000000000000
aws route53 list-hosted-zones
```

## Supported operations

| Area | Operations |
|------|------------|
| Hosted zones | `CreateHostedZone`, `GetHostedZone`, `ListHostedZones`, `ListHostedZonesByName`, `ListHostedZonesByVPC`, `UpdateHostedZoneComment`, `UpdateHostedZoneFeatures`, `DeleteHostedZone`, `GetHostedZoneCount`, `GetHostedZoneLimit` |
| Records | `ChangeResourceRecordSets`, `ListResourceRecordSets`, `GetChange`, `TestDNSAnswer` |
| Health checks | `CreateHealthCheck`, `GetHealthCheck`, `ListHealthChecks`, `UpdateHealthCheck`, `DeleteHealthCheck`, `GetHealthCheckStatus`, `GetHealthCheckCount`, `GetHealthCheckLastFailureReason` |
| DNSSEC | `CreateKeySigningKey`, `ActivateKeySigningKey`, `DeactivateKeySigningKey`, `DeleteKeySigningKey`, `EnableHostedZoneDNSSEC`, `DisableHostedZoneDNSSEC`, `GetDNSSEC` |
| VPC associations | `AssociateVPCWithHostedZone`, `DisassociateVPCFromHostedZone`, `Create/Delete/ListVPCAssociationAuthorization(s)` |
| Traffic policies | `CreateTrafficPolicy`, `GetTrafficPolicy`, `ListTrafficPolicies`, `DeleteTrafficPolicy`, `CreateTrafficPolicyVersion`, `ListTrafficPolicyVersions`, `UpdateTrafficPolicyComment`, traffic policy instance create, get, list, update, delete and count |
| CIDR collections | `CreateCidrCollection`, `ChangeCidrCollection`, `DeleteCidrCollection`, `ListCidrCollections`, `ListCidrBlocks`, `ListCidrLocations` |
| Delegation sets | `CreateReusableDelegationSet`, `GetReusableDelegationSet`, `ListReusableDelegationSets`, `DeleteReusableDelegationSet`, `GetReusableDelegationSetLimit` |
| Query logging | `CreateQueryLoggingConfig`, `GetQueryLoggingConfig`, `ListQueryLoggingConfigs`, `DeleteQueryLoggingConfig` |
| Tags | `ChangeTagsForResource`, `ListTagsForResource`, `ListTagsForResources` |
| Other | `GetAccountLimit`, `GetCheckerIpRanges`, `GetGeoLocation`, `ListGeoLocations` |

## DNS resolution

Records resolve only with the embedded DNS server enabled (`--dns-addr :10053`). A and CNAME records are registered with it automatically. See [DNS setup](../architecture/dns.md).

Routing policies (weighted, latency, geolocation, failover, multivalue) are evaluated by `TestDNSAnswer`. A record whose health check is `Unhealthy` is excluded from answers.

## Differences from AWS

> [!NOTE]
> Alias records that point at AWS resources (ELB, CloudFront and so on) are stored but not resolved to the resource.
