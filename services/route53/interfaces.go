package route53

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// StorageBackend defines the interface for Route 53 backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Hosted zone operations
	CreateHostedZone(name, callerRef, comment string, private bool, delegationSetID string) (*HostedZone, error)
	DeleteHostedZone(zoneID string) error
	GetHostedZone(zoneID string) (*HostedZone, error)
	ListHostedZones(marker string, maxItems int) (page.Page[HostedZone], error)
	ListHostedZonesByName(dnsName, zoneID string, maxItems int) ([]HostedZone, string, string, error)
	GetHostedZoneCount() int
	UpdateHostedZoneComment(zoneID, comment string) (*HostedZone, error)

	// Record set operations
	ChangeResourceRecordSets(zoneID string, changes []Change) (string, error)
	ListResourceRecordSets(zoneID, startName, startType, startIdentifier string, maxItems int) (RRSetPage, error)
	CountResourceRecordSets(zoneID string) (int, error)
	GetChange(changeID string) (*ChangeInfo, error)

	// Health check operations
	CreateHealthCheck(callerRef string, cfg HealthCheckConfig) (*HealthCheck, error)
	GetHealthCheck(id string) (*HealthCheck, error)
	ListHealthChecks(marker string, maxItems int) (page.Page[HealthCheck], error)
	GetHealthCheckCount() int
	DeleteHealthCheck(id string) error
	UpdateHealthCheck(id string, cfg HealthCheckConfig, expectedVersion *int64) (*HealthCheck, error)
	GetHealthCheckStatus(id string) (string, error)
	SetHealthCheckStatus(id, status string) error

	// Key signing key operations
	CreateKeySigningKey(hostedZoneID, callerRef, name, kmsArn, status string) (*KeySigningKey, error)
	ActivateKeySigningKey(hostedZoneID, name string) (*KeySigningKey, error)
	DeactivateKeySigningKey(hostedZoneID, name string) (*KeySigningKey, error)
	DeleteKeySigningKey(hostedZoneID, name string) error

	// DNSSEC operations
	EnableHostedZoneDNSSEC(zoneID string) error
	DisableHostedZoneDNSSEC(zoneID string) error
	GetDNSSEC(zoneID string) (bool, []KeySigningKey, error)

	// VPC association operations
	AssociateVPCWithHostedZone(zoneID, vpcID, vpcRegion string) error
	DisassociateVPCFromHostedZone(zoneID, vpcID string) error
	ListVPCAssociations(zoneID string) ([]vpcAssociation, error)
	ListHostedZonesByVPC(vpcID, vpcRegion string) ([]HostedZone, error)
	CreateVPCAssociationAuthorization(zoneID, vpcID, vpcRegion string) (*VPCAssociationAuthorization, error)
	DeleteVPCAssociationAuthorization(zoneID, vpcID string) error
	ListVPCAssociationAuthorizations(zoneID string) ([]VPCAssociationAuthorization, error)
	CountAssociatedVPCs(zoneID string) (int, error)

	// CIDR collection operations
	CreateCidrCollection(name, callerRef string) (*CidrCollection, error)
	ChangeCidrCollection(
		collectionID string,
		changes []CidrCollectionChange,
		expectedVersion *int64,
	) (*CidrCollection, error)
	DeleteCidrCollection(id string) error
	ListCidrCollections() ([]*CidrCollection, error)
	ListCidrLocations(collectionID string) ([]string, error)
	ListCidrBlocks(collectionID, locationName string) ([]string, error)

	// Query logging operations
	CreateQueryLoggingConfig(hostedZoneID, logGroupArn string) (*QueryLoggingConfig, error)
	GetQueryLoggingConfig(id string) (*QueryLoggingConfig, error)
	DeleteQueryLoggingConfig(id string) error
	ListQueryLoggingConfigs(hostedZoneID string) ([]*QueryLoggingConfig, error)

	// Delegation set operations
	CreateReusableDelegationSet(callerRef, hostedZoneID string) (*ReusableDelegationSet, error)
	GetReusableDelegationSet(id string) (*ReusableDelegationSet, error)
	DeleteReusableDelegationSet(id string) error
	ListReusableDelegationSets() ([]*ReusableDelegationSet, error)
	CountZonesByReusableDelegationSet(id string) (int, error)

	// DNS query simulation
	TestDNSAnswer(zoneID, recordName, recordType string, qctx DNSQueryContext) ([]string, error)

	// Traffic policy operations
	CreateTrafficPolicy(name, document, comment string) (*TrafficPolicy, error)
	CreateTrafficPolicyVersion(id, document, comment string) (*TrafficPolicy, error)
	CreateTrafficPolicyInstance(
		hostedZoneID, name, tpID string,
		tpVersion int32,
		ttl int64,
	) (*TrafficPolicyInstance, error)
	UpdateTrafficPolicyInstance(id, tpID string, tpVersion int32, ttl int64) (*TrafficPolicyInstance, error)
	UpdateTrafficPolicyComment(id string, version int32, comment string) (*TrafficPolicy, error)
	DeleteTrafficPolicy(id string, version int32) error
	GetTrafficPolicy(id string, version int32) (*TrafficPolicy, error)
	DeleteTrafficPolicyInstance(id string) error
	GetTrafficPolicyInstance(id string) (*TrafficPolicyInstance, error)
	ListTrafficPolicies() ([]*TrafficPolicySummary, error)
	ListTrafficPolicyVersions(id string) ([]*TrafficPolicy, error)
	ListTrafficPolicyInstances() ([]*TrafficPolicyInstance, error)
	ListTrafficPolicyInstancesByHostedZone(hostedZoneID string) ([]*TrafficPolicyInstance, error)
	ListTrafficPolicyInstancesByPolicy(tpID string, tpVersion int32) ([]*TrafficPolicyInstance, error)

	// Tags operations. resourceType is the AWS TagResourceType wire value
	// ("hostedzone" or "healthcheck"); it is used to validate that the
	// resource actually exists (and to pick the right NoSuch* error) before
	// reading or mutating tags.
	ListTagsForResource(resourceType, resourceID string) (map[string]string, error)
	ListTagsForResources(resourceType string, resourceIDs []string) (map[string]map[string]string, error)
	ChangeTagsForResource(resourceType, resourceID string, addTags map[string]string, removeKeys []string) error

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
