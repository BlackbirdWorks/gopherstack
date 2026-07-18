package servicediscovery

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	typeNamespace = "NAMESPACE"
	typeService   = "SERVICE"
	typeInstance  = "INSTANCE"
)

const (
	namespaceTypeHTTP       = "HTTP"
	namespaceTypeDNSPrivate = "DNS_PRIVATE"
	namespaceTypeDNSPublic  = "DNS_PUBLIC"

	operationStatusSuccess = "SUCCESS"

	operationTypeCreateNamespace    = "CREATE_NAMESPACE"
	operationTypeDeleteNamespace    = "DELETE_NAMESPACE"
	operationTypeUpdateNamespace    = "UPDATE_NAMESPACE"
	operationTypeUpdateService      = "UPDATE_SERVICE"
	operationTypeRegisterInstance   = "REGISTER_INSTANCE"
	operationTypeDeregisterInstance = "DEREGISTER_INSTANCE"

	instanceHealthStatusHealthy   = "HEALTHY"
	instanceHealthStatusUnhealthy = "UNHEALTHY"

	healthStatusFilterAll              = "ALL"
	healthStatusFilterHealthyOrElseAll = "HEALTHY_OR_ELSE_ALL"

	serviceTypeHTTP    = "HTTP"
	serviceTypeDNS     = "DNS"
	serviceTypeDNSHTTP = "DNS_HTTP"

	defaultSOATTL int64 = 15

	maxResultsDefault = 100
	maxResultsCap     = 100
)

// InMemoryBackend is the in-memory Cloud Map backend.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	namespaces       *store.Table[Namespace]
	namespacesByARN  *store.Index[Namespace]
	namespacesByName *store.Index[Namespace]

	services         *store.Table[Service]
	servicesByARN    *store.Index[Service]
	servicesByNsName *store.Index[Service]

	instances          *store.Table[Instance]
	instancesByService *store.Index[Instance]

	operations *store.Table[Operation]

	serviceAttributes      map[string]map[string]string
	instanceHealthStatuses map[string]string

	accountID string
	region    string

	instanceRevision int64
	nsCounter        int
	svcCounter       int
	opCounter        int

	deterministicIDs bool
}

// NewInMemoryBackend creates a new in-memory Cloud Map backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:               store.NewRegistry(),
		serviceAttributes:      make(map[string]map[string]string),
		instanceHealthStatuses: make(map[string]string),
		mu:                     lockmetrics.New("servicediscovery"),
		accountID:              accountID,
		region:                 region,
	}

	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) namespaceARN(id string) string {
	return arn.Build("servicediscovery", b.region, b.accountID, fmt.Sprintf("namespace/%s", id))
}

func (b *InMemoryBackend) serviceARN(id string) string {
	return arn.Build("servicediscovery", b.region, b.accountID, fmt.Sprintf("service/%s", id))
}

const (
	idChars              = "abcdefghijklmnopqrstuvwxyz0123456789"
	idAlnumLen           = 26
	idOperationSuffixLen = 8
)

func (b *InMemoryBackend) nextNsID() string {
	if b.deterministicIDs {
		b.nsCounter++

		return fmt.Sprintf("ns-%026d", b.nsCounter)
	}

	return "ns-" + randAlnum(idAlnumLen)
}

func (b *InMemoryBackend) nextSvcID() string {
	if b.deterministicIDs {
		b.svcCounter++

		return fmt.Sprintf("srv-%025d", b.svcCounter)
	}

	return "srv-" + randAlnum(idAlnumLen)
}

func (b *InMemoryBackend) nextOpID() string {
	b.opCounter++

	return fmt.Sprintf("op-%08d", b.opCounter)
}

func randAlnum(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = idChars[rand.IntN(len(idChars))] //nolint:gosec // non-cryptographic, math/rand/v2 sufficient
	}

	return string(buf)
}

// syntheticHostedZoneID generates a synthetic Route53 hosted zone ID for DNS namespaces.
func syntheticHostedZoneID() string {
	return "Z" + strings.ToUpper(randAlnum(idOperationSuffixLen))
}

// instanceKey creates a unique key for storing instances.
func instanceKey(serviceID, instanceID string) string {
	return serviceID + "/" + instanceID
}

// copyTags returns a shallow copy of a tag map, or nil when input is nil/empty.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// copyAttrs returns a shallow copy of an attributes map, or nil when input is nil/empty.
func copyAttrs(attrs map[string]string) map[string]string {
	return copyTags(attrs)
}

func copyDNSConfig(dc *DNSConfig) *DNSConfig {
	if dc == nil {
		return nil
	}

	cp := *dc

	if len(dc.DNSRecords) > 0 {
		cp.DNSRecords = make([]DNSRecord, len(dc.DNSRecords))
		copy(cp.DNSRecords, dc.DNSRecords)
	}

	return &cp
}

func copyHealthCheckConfig(hcc *HealthCheckConfig) *HealthCheckConfig {
	if hcc == nil {
		return nil
	}

	cp := *hcc

	return &cp
}

func copyHealthCheckCustomConfig(hccc *HealthCheckCustomConfig) *HealthCheckCustomConfig {
	if hccc == nil {
		return nil
	}

	cp := *hccc

	return &cp
}

func copyNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.Tags = copyTags(ns.Tags)

	if ns.Properties != nil {
		props := *ns.Properties

		if ns.Properties.DNSProperties != nil {
			dp := *ns.Properties.DNSProperties

			if ns.Properties.DNSProperties.SOA != nil {
				soa := *ns.Properties.DNSProperties.SOA
				dp.SOA = &soa
			}

			props.DNSProperties = &dp
		}

		if ns.Properties.HTTPProperties != nil {
			hp := *ns.Properties.HTTPProperties
			props.HTTPProperties = &hp
		}

		cp.Properties = &props
	}

	return &cp
}

func copyService(svc *Service) *Service {
	cp := *svc
	cp.Tags = copyTags(svc.Tags)
	cp.DNSConfig = copyDNSConfig(svc.DNSConfig)
	cp.HealthCheckConfig = copyHealthCheckConfig(svc.HealthCheckConfig)
	cp.HealthCheckCustomConfig = copyHealthCheckCustomConfig(svc.HealthCheckCustomConfig)

	return &cp
}

func copyOperation(op *Operation) Operation {
	cp := *op

	if len(op.Targets) > 0 {
		cp.Targets = make(map[string]string, len(op.Targets))
		maps.Copy(cp.Targets, op.Targets)
	}

	return cp
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.serviceAttributes = make(map[string]map[string]string)
	b.instanceHealthStatuses = make(map[string]string)
	b.instanceRevision = 0
	b.nsCounter = 0
	b.svcCounter = 0
	b.opCounter = 0
}
