package route53

import "github.com/blackbirdworks/gopherstack/pkgs/page"

// StorageBackend defines the interface for Route 53 backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Hosted zone operations
	CreateHostedZone(name, callerRef, comment string, private bool) (*HostedZone, error)
	DeleteHostedZone(zoneID string) error
	GetHostedZone(zoneID string) (*HostedZone, error)
	ListHostedZones(marker string, maxItems int) (page.Page[HostedZone], error)

	// Record set operations
	ChangeResourceRecordSets(zoneID string, changes []Change) error
	ListResourceRecordSets(zoneID string) ([]ResourceRecordSet, error)

	// Health check operations
	CreateHealthCheck(callerRef string, cfg HealthCheckConfig) (*HealthCheck, error)
	GetHealthCheck(id string) (*HealthCheck, error)
	ListHealthChecks(marker string, maxItems int) (page.Page[HealthCheck], error)
	DeleteHealthCheck(id string) error
	UpdateHealthCheck(id string, cfg HealthCheckConfig) (*HealthCheck, error)
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

	// CIDR collection operations
	CreateCidrCollection(name, callerRef string) (*CidrCollection, error)
	ChangeCidrCollection(collectionID string, changes []CidrCollectionChange) (*CidrCollection, error)
	DeleteCidrCollection(id string) error
	ListCidrCollections() ([]*CidrCollection, error)

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

	// VPC disassociation
	DisassociateVPCFromHostedZone(zoneID, vpcID string) error
	GetVPCAssociations(zoneID string) ([]vpcAssociation, error)

	// DNS query simulation
	TestDNSAnswer(zoneID, recordName, recordType string) ([]string, error)

	// Traffic policy operations
	CreateTrafficPolicy(name, document, comment string) (*TrafficPolicy, error)
	CreateTrafficPolicyVersion(id, document, comment string) (*TrafficPolicy, error)
	CreateTrafficPolicyInstance(
		hostedZoneID, name, tpID string,
		tpVersion int32,
		ttl int64,
	) (*TrafficPolicyInstance, error)
	DeleteTrafficPolicy(id string, version int32) error
	GetTrafficPolicy(id string, version int32) (*TrafficPolicy, error)
	DeleteTrafficPolicyInstance(id string) error
	GetTrafficPolicyInstance(id string) (*TrafficPolicyInstance, error)
	ListTrafficPolicies() ([]*TrafficPolicy, error)
	ListTrafficPolicyVersions(id string) ([]*TrafficPolicy, error)
	ListTrafficPolicyInstances() ([]*TrafficPolicyInstance, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
