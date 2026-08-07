package vpclattice

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	arnService                     = "vpc-lattice"
	resourceService                = "service"
	resourceServiceNetwork         = "servicenetwork"
	resourceServiceNetworkSvcAssoc = "servicenetworkserviceassociation"
	resourceServiceNetworkVpcAssoc = "servicenetworkvpcassociation"
	resourceListener               = "listener"
	resourceRule                   = "rule"
	resourceTargetGroup            = "targetgroup"
	resourceAccessLogSubscription  = "accesslogsubscription"
	resourceResourceGateway        = "resourcegateway"
	resourceResourceConfiguration  = "resourceconfiguration"
	resourceServiceNetworkResAssoc = "servicenetworkresourceassociation"
	resourceDomainVerification     = "domainverification"

	idPrefixService                = "svc-"
	idPrefixNetwork                = "sn-"
	idPrefixSNSA                   = "snsa-"
	idPrefixSNVA                   = "snva-"
	idPrefixListener               = "listener-"
	idPrefixRule                   = "rule-"
	idPrefixTargetGroup            = "tg-"
	idPrefixALS                    = "als-"
	idPrefixResourceGateway        = "rgw-"
	idPrefixResourceConfiguration  = "rcfg-"
	idPrefixServiceNetworkResAssoc = "snra-"
	idPrefixDomainVerification     = "dv-"

	statusActive           = "ACTIVE"
	statusInactive         = "INACTIVE"
	statusCreateInProgress = "CREATE_IN_PROGRESS"
	statusDeleteInProgress = "DELETE_IN_PROGRESS"
	statusDeleted          = "DELETED"
	statusCreateFailed     = "CREATE_FAILED"

	// verificationStatusPending is the only VerificationStatus this backend
	// ever produces: real domain verification requires AWS to observe a
	// caller-provisioned public DNS TXT record, which this mock has no way
	// to do -- see DomainVerification's doc comment in domain_verifications.go.
	verificationStatusPending = "PENDING"

	authTypeNone  = "NONE"
	protocolHTTP  = "HTTP"
	protocolHTTPS = "HTTPS"

	tgStatusActive = "ACTIVE"

	targetStatusHealthy = "HEALTHY"

	authPolicyStateActive = "Active"

	defaultRulePriority = 100

	defaultMaxResults = 100
)

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	services        *store.Table[storedService]
	servicesByName  *store.Index[storedService]
	serviceNetworks *store.Table[storedServiceNetwork]
	networksByName  *store.Index[storedServiceNetwork]
	snsas           *store.Table[storedSNSA]
	snvas           *store.Table[storedSNVA]

	listeners          *store.Table[storedListener]
	listenersByService *store.Index[storedListener]

	rules           *store.Table[storedRule]
	rulesByListener *store.Index[storedRule]

	targetGroups *store.Table[storedTargetGroup]
	tgsByName    *store.Index[storedTargetGroup]
	targets      map[string][]*storedTarget

	alss *store.Table[storedALS]

	resourceGateways       *store.Table[storedResourceGateway]
	resourceGatewaysByName *store.Index[storedResourceGateway]

	resourceConfigurations       *store.Table[storedResourceConfiguration]
	resourceConfigurationsByName *store.Index[storedResourceConfiguration]

	snras *store.Table[storedSNRA]

	domainVerifications         *store.Table[storedDomainVerification]
	domainVerificationsByDomain *store.Index[storedDomainVerification]

	authPolicies     map[string]string
	resourcePolicies map[string]string
	tags             map[string]map[string]string
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:               lockmetrics.New("vpclattice"),
		registry:         store.NewRegistry(),
		targets:          make(map[string][]*storedTarget),
		authPolicies:     make(map[string]string),
		resourcePolicies: make(map[string]string),
		tags:             make(map[string]map[string]string),
		accountID:        accountID,
		region:           region,
	}
	registerAllTables(b)

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) regionFor(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.targets = make(map[string][]*storedTarget)
	b.authPolicies = make(map[string]string)
	b.resourcePolicies = make(map[string]string)
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) buildListenerARN(serviceID, listenerID string) string {
	return arn.Build(arnService, b.region, b.accountID,
		fmt.Sprintf("%s/%s/%s/%s", resourceService, serviceID, resourceListener, listenerID))
}

func (b *InMemoryBackend) buildRuleARN(serviceID, listenerID, ruleID string) string {
	return arn.Build(
		arnService,
		b.region,
		b.accountID,
		fmt.Sprintf(
			"%s/%s/%s/%s/%s/%s",
			resourceService,
			serviceID,
			resourceListener,
			listenerID,
			resourceRule,
			ruleID,
		),
	)
}

func newID(prefix string) string {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")[:17]

	return prefix + id
}

// newHostedZoneID generates a Route 53-style hosted zone ID ("Z" followed by
// uppercase alphanumerics) for a service's dnsEntry.hostedZoneId, matching
// real AWS's DnsEntry shape. VPC Lattice provisions a real private hosted
// zone per service; this backend has no Route53 integration to source one
// from, so it synthesizes a plausible, stable-per-resource ID instead of
// leaving the field empty.
func newHostedZoneID() string {
	id := strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")[:20])

	return "Z" + id
}

func copyTags(src map[string]string) map[string]string {
	if src == nil {
		return make(map[string]string)
	}

	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}
