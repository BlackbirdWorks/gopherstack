package securityhub

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// securityhubSnapshotVersion identifies the shape of [snapshot], in
// particular the "tables" field produced by b.registry.SnapshotAll(). It must
// be bumped whenever a change to a converted resource type or snapshot itself
// would make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (registry.ResetAll,
// never a partial decode) any mismatch -- see Restore. Snapshots taken before
// Phase 3.3 (the pkgs/store conversion) had no version field at all, so they
// decode with Version == 0, which is guaranteed to mismatch
// securityhubSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const securityhubSnapshotVersion = 1

const (
	keyErrorCode    = "ErrorCode"
	keyErrorMessage = "ErrorMessage"

	errCodeInvalidInput = "InvalidInput"

	keyStandardsArn      = "StandardsArn"
	keySecurityControlID = "SecurityControlId"
	keyRuleArn           = "RuleArn"

	statusEnabled       = "ENABLED"
	statusAvailable     = "AVAILABLE"
	statusReady         = "READY"
	companyAWS          = "AWS"
	intTypeSendFindings = "SEND_FINDINGS_TO_SECURITY_HUB"

	msgRuleNotFound = "Rule not found"

	maxStandardsResults = 25
	maxDefaultResults   = 100

	// Shared map-literal keys reused across multiple op families (members,
	// invitations, organizations, resources V2) -- kept here so every family
	// file references one constant instead of re-declaring the same literal.
	keyAccountID         = "AccountId"
	keyProcessingResult  = "ProcessingResult"
	keyInvitedAt         = "InvitedAt"
	keyMemberStatus      = "MemberStatus"
	keyGroupByAttribute  = "GroupByAttribute"
	keyCount             = "Count"
	keySortOrder         = "SortOrder"
	keyFindingIdentifier = "FindingIdentifier"
	keyProductArn        = "ProductArn"
	keyAwsAccountID      = "AwsAccountId"
	keyMetadataUID       = "MetadataUid"

	// Finding filter/group-by field names that name a value nested under the
	// finding (Severity.Label/Workflow.Status/Compliance.Status), not a flat
	// top-level key -- see findingFieldString.
	keyFilterSeverityLabel    = "SeverityLabel"
	keyFilterWorkflowStatus   = "WorkflowStatus"
	keyFilterComplianceStatus = "ComplianceStatus"

	msgFindingNotFound = "Finding not found"

	errCodeResourceNotFound = "ResourceNotFoundException"
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	// registry lets Reset/Snapshot/Restore collapse the lifecycle of every
	// converted resource field below to one call each (registry.ResetAll(),
	// registry.SnapshotAll(), registry.RestoreAll()) instead of hand-rolled
	// per-map boilerplate. See store_setup.go for the full registration list
	// and the fields deliberately left as plain maps.
	registry               *store.Registry
	configPolicyAssocs     *store.Table[ConfigurationPolicyAssociation]
	orgConfig              *OrgConfig
	tags                   map[string]map[string]string
	automationRules        *store.Table[AutomationRule]
	hub                    *Hub
	findings               map[string]map[string]any
	findingHistory         map[string][]map[string]any
	insights               *store.Table[Insight]
	controlParams          map[string]map[string]any
	productSubscriptions   map[string]string
	mu                     *lockmetrics.RWMutex
	actionTargets          *store.Table[ActionTarget]
	members                *store.Table[Member]
	invitations            *store.Table[Invitation]
	adminAccount           *AdminAccount
	configPolicies         *store.Table[ConfigurationPolicy]
	orgAdminAccounts       map[string]string
	recommendedPoliciesV2  *store.Table[RecommendedPolicyV2]
	findingAggregators     *store.Table[FindingAggregator]
	controlOverrides       *store.Table[StandardsControl]
	controlAssocOverrides  *store.Table[StandardsControlAssociation] // composite key: standardsArn|securityControlID
	ticketsV2              *store.Table[TicketV2]
	connectorsV2           *store.Table[ConnectorV2]
	standardsSubscriptions *store.Table[StandardsSubscription]
	hubV2                  *HubV2
	aggregatorsV2          *store.Table[AggregatorV2]
	automationRulesV2      *store.Table[AutomationRuleV2]
	cspmConnectors         *store.Table[CspmConnector]
	region                 string
	accountID              string
	aggregatorV2Seq        int
	connectorV2Seq         int
	automationRuleV2Seq    int
	configPolicySeq        int
	memberSeq              int
	findingAggregatorSeq   int
	ticketV2Seq            int
	standardsSeq           int
	actionTargetSeq        int
	insightSeq             int
	automationRuleSeq      int
	cspmConnectorSeq       int
	hubEnabled             bool
	hubV2Enabled           bool
}

// NewInMemoryBackend creates a new in-memory backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:             store.NewRegistry(),
		mu:                   lockmetrics.New("securityhub"),
		accountID:            accountID,
		region:               region,
		findings:             make(map[string]map[string]any),
		findingHistory:       make(map[string][]map[string]any),
		productSubscriptions: make(map[string]string),
		controlParams:        make(map[string]map[string]any),
		tags:                 make(map[string]map[string]string),
		orgAdminAccounts:     make(map[string]string),
	}

	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetLocked()
}

// resetLocked returns every field to the state NewInMemoryBackend leaves it
// in: every store.Table-backed resource field via one b.registry.ResetAll()
// call, every raw map reallocated, every scalar/pointer/sequence field zeroed.
// It backs both Reset() and Restore's incompatible-snapshot-version branch
// (see Restore), which must discard to the exact same empty state Reset()
// produces rather than leave stale pre-restore data behind. The caller MUST
// hold b.mu for writing.
func (b *InMemoryBackend) resetLocked() {
	b.hubEnabled = false
	b.hub = nil
	b.findings = make(map[string]map[string]any)
	b.findingHistory = make(map[string][]map[string]any)
	b.insightSeq = 0
	b.standardsSeq = 0
	b.actionTargetSeq = 0
	b.productSubscriptions = make(map[string]string)
	b.controlParams = make(map[string]map[string]any)
	b.automationRuleSeq = 0
	b.tags = make(map[string]map[string]string)
	// Members / Invitations / Admin
	b.adminAccount = nil
	b.orgConfig = nil
	b.orgAdminAccounts = make(map[string]string)
	b.memberSeq = 0
	// Finding Aggregator
	b.findingAggregatorSeq = 0
	// Configuration Policy
	b.configPolicySeq = 0
	// V2
	b.hubV2Enabled = false
	b.hubV2 = nil
	b.aggregatorV2Seq = 0
	b.automationRuleV2Seq = 0
	b.connectorV2Seq = 0
	b.ticketV2Seq = 0
	b.cspmConnectorSeq = 0

	// Every store.Table-backed resource field collapses to one call,
	// replacing what was previously ~16 individual make(map[...]) resets.
	b.registry.ResetAll()
}

// snapshot is the top-level on-disk shape for the SecurityHub backend.
//
// Tables holds one JSON-encoded array per registered [store.Table] name,
// produced by b.registry.SnapshotAll() -- see store_setup.go for the full
// registration list. Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type snapshot struct {
	ProductSubscriptions map[string]string            `json:"productSubscriptions"`
	Hub                  *Hub                         `json:"hub"`
	Findings             map[string]map[string]any    `json:"findings"`
	FindingHistory       map[string][]map[string]any  `json:"findingHistory"`
	Tags                 map[string]map[string]string `json:"tags"`
	ControlParams        map[string]map[string]any    `json:"controlParams"`
	// Members / Invitations / Admin
	AdminAccount     *AdminAccount     `json:"adminAccount"`
	OrgConfig        *OrgConfig        `json:"orgConfig"`
	OrgAdminAccounts map[string]string `json:"orgAdminAccounts"`
	// V2
	HubV2   *HubV2                     `json:"hubV2"`
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`

	StandardsSeq         int  `json:"standardsSeq"`
	ActionTargetSeq      int  `json:"actionTargetSeq"`
	AutomationRuleSeq    int  `json:"automationRuleSeq"`
	InsightSeq           int  `json:"insightSeq"`
	MemberSeq            int  `json:"memberSeq"`
	FindingAggregatorSeq int  `json:"findingAggregatorSeq"`
	ConfigPolicySeq      int  `json:"configPolicySeq"`
	AggregatorV2Seq      int  `json:"aggregatorV2Seq"`
	AutomationRuleV2Seq  int  `json:"automationRuleV2Seq"`
	ConnectorV2Seq       int  `json:"connectorV2Seq"`
	TicketV2Seq          int  `json:"ticketV2Seq"`
	CspmConnectorSeq     int  `json:"cspmConnectorSeq"`
	HubEnabled           bool `json:"hubEnabled"`
	HubV2Enabled         bool `json:"hubV2Enabled"`
}

func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables hold plain JSON-friendly structs, so a
		// marshal failure here indicates a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "securityhub: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := snapshot{
		Version:              securityhubSnapshotVersion,
		Tables:               tables,
		HubEnabled:           b.hubEnabled,
		Hub:                  b.hub,
		Findings:             b.findings,
		FindingHistory:       b.findingHistory,
		InsightSeq:           b.insightSeq,
		StandardsSeq:         b.standardsSeq,
		ActionTargetSeq:      b.actionTargetSeq,
		ProductSubscriptions: b.productSubscriptions,
		ControlParams:        b.controlParams,
		AutomationRuleSeq:    b.automationRuleSeq,
		Tags:                 b.tags,
		// Members / Invitations / Admin
		AdminAccount:     b.adminAccount,
		OrgConfig:        b.orgConfig,
		OrgAdminAccounts: b.orgAdminAccounts,
		MemberSeq:        b.memberSeq,
		// Finding Aggregator
		FindingAggregatorSeq: b.findingAggregatorSeq,
		// Configuration Policy
		ConfigPolicySeq: b.configPolicySeq,
		// V2
		HubV2Enabled:        b.hubV2Enabled,
		HubV2:               b.hubV2,
		AggregatorV2Seq:     b.aggregatorV2Seq,
		AutomationRuleV2Seq: b.automationRuleV2Seq,
		ConnectorV2Seq:      b.connectorV2Seq,
		TicketV2Seq:         b.ticketV2Seq,
		CspmConnectorSeq:    b.cspmConnectorSeq,
	}

	return persistence.MarshalSnapshot(ctx, "securityhub", snap)
}

func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap snapshot
	if err := persistence.UnmarshalSnapshot(ctx, "securityhub", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != securityhubSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"securityhub: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", securityhubSnapshotVersion)

		b.resetLocked()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("securityhub: restore snapshot tables: %w", err)
	}

	b.hubEnabled = snap.HubEnabled
	b.hub = snap.Hub

	b.findings = snap.Findings
	if b.findings == nil {
		b.findings = make(map[string]map[string]any)
	}

	b.findingHistory = snap.FindingHistory
	if b.findingHistory == nil {
		b.findingHistory = make(map[string][]map[string]any)
	}

	b.insightSeq = snap.InsightSeq
	b.standardsSeq = snap.StandardsSeq
	b.actionTargetSeq = snap.ActionTargetSeq

	b.productSubscriptions = snap.ProductSubscriptions
	if b.productSubscriptions == nil {
		b.productSubscriptions = make(map[string]string)
	}

	b.controlParams = snap.ControlParams
	if b.controlParams == nil {
		b.controlParams = make(map[string]map[string]any)
	}

	b.automationRuleSeq = snap.AutomationRuleSeq

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	// Members / Invitations / Admin
	b.adminAccount = snap.AdminAccount
	b.orgConfig = snap.OrgConfig

	b.orgAdminAccounts = snap.OrgAdminAccounts
	if b.orgAdminAccounts == nil {
		b.orgAdminAccounts = make(map[string]string)
	}

	b.memberSeq = snap.MemberSeq
	// Finding Aggregator
	b.findingAggregatorSeq = snap.FindingAggregatorSeq
	// Configuration Policy
	b.configPolicySeq = snap.ConfigPolicySeq
	// V2
	b.hubV2Enabled = snap.HubV2Enabled
	b.hubV2 = snap.HubV2
	b.aggregatorV2Seq = snap.AggregatorV2Seq
	b.automationRuleV2Seq = snap.AutomationRuleV2Seq
	b.connectorV2Seq = snap.ConnectorV2Seq
	b.ticketV2Seq = snap.TicketV2Seq
	b.cspmConnectorSeq = snap.CspmConnectorSeq

	return nil
}

// filterOrAll returns values from m for the given arns, or all values if arns is empty.
func filterOrAll[V any](arns []string, t *store.Table[V]) []*V {
	if len(arns) == 0 {
		return t.All()
	}

	var results []*V

	for _, arn := range arns {
		if v, ok := t.Get(arn); ok {
			results = append(results, v)
		}
	}

	return results
}

// paginateSlice applies token-based pagination, capping maxResults at maxCap.
func paginateSlice[T any](results []T, nextToken string, maxResults, maxCap int) ([]T, string) {
	if maxResults <= 0 || maxResults > maxCap {
		maxResults = maxCap
	}
	start := decodeToken(nextToken)
	if start >= len(results) {
		return []T{}, ""
	}
	end := start + maxResults
	end = min(end, len(results))
	nextOut := ""
	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return results[start:end], nextOut
}

func encodeToken(offset int) string {
	return strconv.Itoa(offset)
}

func decodeToken(token string) int {
	if token == "" {
		return 0
	}

	var offset int

	if _, err := fmt.Sscanf(token, "%d", &offset); err != nil {
		return 0
	}

	return offset
}
