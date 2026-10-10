package awsconfig

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	recorderStatusActive  = "ACTIVE"
	recorderStatusPending = "PENDING"

	recorderLastStatusPending = "Pending"
	recorderLastStatusSuccess = "Success"
)

// Real DeliveryStatus enum values (configservice@v1.68.4 types/enums.go:278-280).
const (
	deliveryStatusSuccess       = "Success"
	deliveryStatusFailure       = "Failure"
	deliveryStatusNotApplicable = "Not_Applicable"
)

// InMemoryBackend is the in-memory store for AWS Config resources.
//
// Phase 3.3 datalayer conversion: every map[string]*T resource collection is
// now a *store.Table[T] registered on b.registry (see store_setup.go's
// registerAllTables), which collapses Reset/Snapshot/Restore to one
// b.registry call each instead of one hand-written block per map. Fields
// whose value is not a *T (a scalar, a slice, or a nested map) have no
// natural store.Table key and are left as plain maps -- see each field's own
// comment for why, and persistence.go's doc comment for the persistence
// audit of each.
type InMemoryBackend struct {
	s3Writer                     S3Writer
	snsPublisher                 SNSPublisher
	templates                    TemplateSource
	storedQueries                *store.Table[StoredQuery]
	ruleResourceEvals            *store.Table[StoredEvaluation]
	deliveryStatus               *store.Table[deliveryChannelStatusState]
	connectors                   *store.Table[Connector]
	aggregationAuths             *store.Table[AggregationAuthorization]
	configRules                  *store.Table[ConfigRule]
	ruleEvaluations              map[string]string
	ruleActivity                 map[string]*ruleActivity
	packTransitions              map[string]packTransition
	remediationConfigs           *store.Table[RemediationConfiguration]
	retentionConfigs             *store.Table[RetentionConfiguration]
	ruleResourceEvalsByRule      *store.Index[StoredEvaluation]
	ruleResourceEvalsByResource  *store.Index[StoredEvaluation]
	resourceHistory              map[string][]ResourceConfigItem
	resourceEvaluations          *store.Table[ResourceEvaluation]
	aggregators                  *store.Table[ConfigurationAggregator]
	conformancePacks             *store.Table[ConformancePack]
	conformancePackRules         *store.Table[ConformancePackRuleLink]
	conformancePackRulesByPack   *store.Index[ConformancePackRuleLink]
	orgConfigRules               *store.Table[OrganizationConfigRule]
	orgConformancePacks          *store.Table[OrganizationConformancePack]
	registry                     *store.Registry
	channels                     *store.Table[DeliveryChannel]
	resourceTags                 map[string][]Tag
	mu                           *lockmetrics.RWMutex
	remediationExecutions        *store.Table[RemediationExecutionStatusEntry]
	remediationExecutionsByRule  *store.Index[RemediationExecutionStatusEntry]
	remediationExceptions        map[string][]RemediationException
	resourceConfigs              *store.Table[ResourceConfigItem]
	resourceConfigsByType        *store.Index[ResourceConfigItem]
	deletedResourceConfigs       *store.Table[ResourceConfigItem]
	deletedResourceConfigsByType *store.Index[ResourceConfigItem]
	customRulePolicies           map[string]string
	orgCustomRulePolicies        map[string]string
	serviceLinkedRecorders       *store.Table[ServiceLinkedRecorderLink]
	recordersByServicePrincipal  *store.Index[ConfigurationRecorder]
	recorders                    *store.Table[ConfigurationRecorder]
	clock                        func() time.Time
	accountID                    string
	region                       string
	lifecycleDelay               time.Duration
	ruleCounter                  int
	orgRuleCounter               int
	orgPackCounter               int
	conformancePackCounter       int
	aggregatorCounter            int
	resourceEvalCounter          int
	captureCounter               int
}

// now returns the backend's current time, honoring an injected clock.
func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// SetClock overrides the backend's clock. For deterministic tests only.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = clock
}

// SetS3Writer registers the S3 writer used to deliver configuration
// snapshots (DeliverConfigSnapshot). Unwired backends record a FAILURE
// delivery outcome instead of pretending success.
func (b *InMemoryBackend) SetS3Writer(w S3Writer) {
	b.mu.Lock("SetS3Writer")
	defer b.mu.Unlock()

	b.s3Writer = w
}

// SetSNSPublisher registers the SNS publisher used to deliver configuration
// stream notifications after a successful snapshot delivery.
func (b *InMemoryBackend) SetSNSPublisher(pub SNSPublisher) {
	b.mu.Lock("SetSNSPublisher")
	defer b.mu.Unlock()

	b.snsPublisher = pub
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithMeta("123456789012", "us-east-1")
}

// NewInMemoryBackendWithMeta creates a new InMemoryBackend with account and region context.
func NewInMemoryBackendWithMeta(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:              store.NewRegistry(),
		ruleEvaluations:       make(map[string]string),
		ruleActivity:          make(map[string]*ruleActivity),
		packTransitions:       make(map[string]packTransition),
		resourceHistory:       make(map[string][]ResourceConfigItem),
		resourceTags:          make(map[string][]Tag),
		remediationExceptions: make(map[string][]RemediationException),
		customRulePolicies:    make(map[string]string),
		orgCustomRulePolicies: make(map[string]string),
		mu:                    lockmetrics.New("awsconfig"),
		accountID:             accountID,
		region:                region,
	}

	registerAllTables(b)

	return b
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.remediationExecutions.Reset()
	b.ruleEvaluations = make(map[string]string)
	b.ruleActivity = make(map[string]*ruleActivity)
	b.packTransitions = make(map[string]packTransition)
	b.resourceHistory = make(map[string][]ResourceConfigItem)
	b.resourceEvalCounter = 0
	b.captureCounter = 0
	b.ruleCounter = 0
	b.orgRuleCounter = 0
	b.orgPackCounter = 0
	b.conformancePackCounter = 0
	b.aggregatorCounter = 0
	b.resourceTags = make(map[string][]Tag)
	b.remediationExceptions = make(map[string][]RemediationException)
	b.customRulePolicies = make(map[string]string)
	b.orgCustomRulePolicies = make(map[string]string)
}
