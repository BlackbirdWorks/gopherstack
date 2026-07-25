package apprunner

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Every map[string]*T resource field is a *store.Table[T] registered on
// registry (see store_setup.go for the registration and the rationale for
// which fields became Table/Index-backed vs. stayed raw maps). asgByName and
// obsByName are left as raw, order-sensitive slice-maps: they track ASG /
// observability-config revisions in ascending-revision order (the last
// element is "latest"), and [store.Index] does not preserve insertion order
// across removals (it swap-removes), so converting them to an Index would
// silently break the "latest == last element" invariant ListAutoScaling/
// ListObservabilityConfigurations relies on. customDomains is left raw for
// the same reason (order-preserving append/splice, no sort). tags is left
// raw because its values are map[string]string, not *T.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	services *store.Table[storedService]
	byName   *store.Index[storedService]

	autoScalingConfigs *store.Table[storedAutoScalingConfiguration]
	// asgByName maps name to revisions (ascending); left raw, see type doc.
	asgByName map[string][]*storedAutoScalingConfiguration

	connections *store.Table[storedConnection]
	connByName  *store.Index[storedConnection]

	observabilityConfigs *store.Table[storedObservabilityConfiguration]
	// obsByName maps name to revisions (ascending); left raw, see type doc.
	obsByName map[string][]*storedObservabilityConfiguration

	vpcConnectors *store.Table[storedVpcConnector]

	vpcIngressConnections *store.Table[storedVpcIngressConnection]
	vicByName             *store.Index[storedVpcIngressConnection]

	customDomains map[string][]*storedCustomDomain // serviceArn → domains; left raw, see type doc.
	tags          map[string]map[string]string     // resourceArn → tags; left raw, see type doc.

	accountID string
	region    string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:            lockmetrics.New("apprunner"),
		registry:      store.NewRegistry(),
		accountID:     accountID,
		region:        region,
		asgByName:     make(map[string][]*storedAutoScalingConfiguration),
		obsByName:     make(map[string][]*storedObservabilityConfiguration),
		customDomains: make(map[string][]*storedCustomDomain),
		tags:          make(map[string]map[string]string),
	}
	registerAllTables(b)
	b.ensureDefaultAutoScalingConfiguration()

	return b
}

func (b *InMemoryBackend) serviceARN(id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "service/"+id)
}

func (b *InMemoryBackend) asgARN(name string, revision int32, id string) string {
	return arn.Build(
		"apprunner", b.region, b.accountID,
		fmt.Sprintf("autoscalingconfiguration/%s/%d/%s", name, revision, id),
	)
}

func (b *InMemoryBackend) connectionARN(name, id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "connection/"+name+"/"+id)
}

func (b *InMemoryBackend) obsARN(name string, revision int32, id string) string {
	return arn.Build(
		"apprunner", b.region, b.accountID,
		fmt.Sprintf("observabilityconfiguration/%s/%d/%s", name, revision, id),
	)
}

func (b *InMemoryBackend) vpcConnectorARN(name string, revision int32, id string) string {
	return arn.Build(
		"apprunner", b.region, b.accountID,
		fmt.Sprintf("vpcconnector/%s/%d/%s", name, revision, id),
	)
}

func (b *InMemoryBackend) vicARN(name, id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "vpcingressconnection/"+name+"/"+id)
}

func newID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

func newOpID() string {
	return uuid.NewString()
}

func buildServiceURL(name, region string) string {
	return fmt.Sprintf("%s.%s.awsapprunner.com", name, region)
}

func (b *InMemoryBackend) addOperation(svc *storedService, opType string) {
	now := time.Now().UTC()
	op := &storedOperation{
		ID:        newOpID(),
		Type:      opType,
		Status:    opStatusSucceeded,
		TargetArn: svc.ServiceArn,
		StartedAt: now,
		EndedAt:   now,
		UpdatedAt: now,
	}
	svc.Operations = append(svc.Operations, op)

	// Retain only the most recent maxOperationsPerService entries. Copy into a
	// fresh slice so the dropped prefix is released for GC rather than pinned by
	// the original backing array.
	if len(svc.Operations) > maxOperationsPerService {
		trimmed := make([]*storedOperation, maxOperationsPerService)
		copy(trimmed, svc.Operations[len(svc.Operations)-maxOperationsPerService:])
		svc.Operations = trimmed
	}
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.asgByName = make(map[string][]*storedAutoScalingConfiguration)
	b.obsByName = make(map[string][]*storedObservabilityConfiguration)
	b.customDomains = make(map[string][]*storedCustomDomain)
	b.tags = make(map[string]map[string]string)
	b.ensureDefaultAutoScalingConfiguration()
}
