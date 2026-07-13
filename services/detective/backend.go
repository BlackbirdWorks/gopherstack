package detective

import (
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errValidation        = "ValidationException"

	memberStatusInvited          = "INVITED"
	memberStatusEnabled          = "ENABLED"
	memberStatusAcceptedDisabled = "ACCEPTED_BUT_DISABLED"

	investigationStateActive     = "ACTIVE"
	investigationStateArchived   = "ARCHIVED"
	investigationStatusRunning   = "RUNNING"
	investigationStatusFailed    = "FAILED"
	investigationStatusSucceeded = "SUCCESSFUL"

	datasourceIngestStateStarted  = "STARTED"
	datasourceIngestStateStopped  = "STOPPED"
	datasourceIngestStateDisabled = "DISABLED"

	entityTypeIAMRole = "IAM_ROLE"
	entityTypeIAMUser = "IAM_USER"

	severityInformational = "INFORMATIONAL"
	severityLow           = "LOW"
	severityMedium        = "MEDIUM"
	severityHigh          = "HIGH"
	severityCritical      = "CRITICAL"

	maxGraphsPerPage         = 200
	maxMembersPerPage        = 200
	maxInvestigationsPerPage = 200
	maxIndicatorsPerPage     = 100
	maxInvitationsPerPage    = 200
	maxOrgAdminsPerPage      = 200
	maxDatasourcesPerPage    = 200

	maxTagCount              = 50
	maxTagKeyLen             = 128
	maxTagValueLen           = 256
	maxCreateMembersPerBatch = 50
	accountIDLen             = 12

	// iamARNPartsLen is the number of ":"-separated segments in a well-formed
	// IAM entity ARN: "arn", partition, "iam", region (empty), account, resource.
	iamARNPartsLen = 6
)

var (
	// ErrGraphNotFound is returned when a behavior graph does not exist.
	ErrGraphNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyHasGraph is returned when account already has a graph in region.
	ErrAlreadyHasGraph = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)

// storedGraph holds a behavior graph with all fields.
// CreatedTime is first: time.Time's non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedGraph struct {
	CreatedTime time.Time         `json:"createdTime"`
	Tags        map[string]string `json:"tags"`
	Arn         string            `json:"arn"`
}

func (g *storedGraph) toGraph() Graph {
	return Graph{
		Arn:         g.Arn,
		CreatedTime: g.CreatedTime,
		Tags:        g.Tags,
	}
}

// storedMember holds a member with all fields.
// time.Time fields are first: their non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedMember struct {
	InvitedTime     time.Time `json:"invitedTime"`
	UpdatedTime     time.Time `json:"updatedTime"`
	AccountID       string    `json:"accountId"`
	AdministratorID string    `json:"administratorId"`
	EmailAddress    string    `json:"emailAddress"`
	GraphARN        string    `json:"graphArn"`
	Status          string    `json:"status"`
}

func (m *storedMember) toMemberDetail() MemberDetail {
	return MemberDetail{
		AccountID:       m.AccountID,
		AdministratorID: m.AdministratorID,
		EmailAddress:    m.EmailAddress,
		GraphARN:        m.GraphARN,
		InvitedTime:     m.InvitedTime,
		Status:          m.Status,
		UpdatedTime:     m.UpdatedTime,
	}
}

// storedInvestigation holds investigation state.
// time.Time fields are first so their non-pointer prefix reduces GC pointer bytes.
type storedInvestigation struct {
	CreatedTime     time.Time `json:"createdTime"`
	ScopeStartTime  time.Time `json:"scopeStartTime"`
	ScopeEndTime    time.Time `json:"scopeEndTime"`
	GraphARN        string    `json:"graphArn"`
	InvestigationID string    `json:"investigationId"`
	EntityARN       string    `json:"entityArn"`
	EntityType      string    `json:"entityType"`
	Severity        string    `json:"severity"`
	State           string    `json:"state"`
	Status          string    `json:"status"`
}

func (i *storedInvestigation) toInvestigation() Investigation {
	return Investigation{
		CreatedTime:     i.CreatedTime,
		ScopeStartTime:  i.ScopeStartTime,
		ScopeEndTime:    i.ScopeEndTime,
		GraphARN:        i.GraphARN,
		InvestigationID: i.InvestigationID,
		EntityARN:       i.EntityARN,
		EntityType:      i.EntityType,
		Severity:        i.Severity,
		State:           i.State,
		Status:          i.Status,
	}
}

func (i *storedInvestigation) toDetail() InvestigationDetail {
	return InvestigationDetail{
		CreatedTime:     i.CreatedTime,
		EntityARN:       i.EntityARN,
		EntityType:      i.EntityType,
		InvestigationID: i.InvestigationID,
		Severity:        i.Severity,
		State:           i.State,
		Status:          i.Status,
	}
}

// storedOrgAdmin holds an organization administrator record.
// DelegationTime is first so its non-pointer prefix reduces GC pointer bytes.
type storedOrgAdmin struct {
	DelegationTime time.Time `json:"delegationTime"`
	AccountID      string    `json:"accountId"`
	GraphARN       string    `json:"graphArn"`
}

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 50 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

// validateAccountID returns true if id is exactly 12 ASCII digits.
func validateAccountID(id string) bool {
	if len(id) != accountIDLen {
		return false
	}

	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// validateEmail returns true if email contains an @ with chars on both sides.
func validateEmail(email string) bool {
	idx := strings.Index(email, "@")

	return idx > 0 && idx < len(email)-1
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// graphs, members, and investigations are store.Table-backed (see
// store_setup.go); tags, datasources, and orgConfigs remain plain maps since
// their values are not *T; orgAdmins remains a plain order-sensitive slice.
type InMemoryBackend struct {
	members               *store.Table[storedMember]
	mu                    *lockmetrics.RWMutex
	registry              *store.Registry
	graphs                *store.Table[storedGraph]
	membersByGraph        *store.Index[storedMember]
	investigations        *store.Table[storedInvestigation]
	investigationsByGraph *store.Index[storedInvestigation]
	tags                  map[string]map[string]string
	datasources           map[string]map[string]string
	orgConfigs            map[string]bool
	accountID             string
	region                string
	orgAdmins             []*storedOrgAdmin
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:          lockmetrics.New("detective"),
		registry:    store.NewRegistry(),
		accountID:   accountID,
		region:      region,
		tags:        make(map[string]map[string]string),
		datasources: make(map[string]map[string]string),
		orgAdmins:   nil,
		orgConfigs:  make(map[string]bool),
	}

	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) graphARN(id string) string {
	return arn.Build("detective", b.region, b.accountID, fmt.Sprintf("graph:%s", id))
}

// createGraphLocked creates and stores a new behavior graph. Callers must
// hold b.mu for writing. Shared by CreateGraph and EnableOrganizationAdminAccount,
// which both need to materialize a graph the first time an account uses Detective.
func (b *InMemoryBackend) createGraphLocked(tags map[string]string) *storedGraph {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	arn := b.graphARN(id)
	now := time.Now().UTC()

	graphTags := make(map[string]string)
	maps.Copy(graphTags, tags)

	g := &storedGraph{
		Arn:         arn,
		CreatedTime: now,
		Tags:        graphTags,
	}
	b.graphs.Put(g)

	if len(graphTags) > 0 {
		b.tags[arn] = make(map[string]string)
		maps.Copy(b.tags[arn], graphTags)
	}

	return g
}

// CreateGraph creates a new behavior graph. Returns existing one if already created (idempotent).
func (b *InMemoryBackend) CreateGraph(tags map[string]string) (*Graph, error) {
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateGraph")
	defer b.mu.Unlock()

	if existing := b.graphs.All(); len(existing) > 0 {
		cp := existing[0].toGraph()

		return &cp, nil
	}

	cp := b.createGraphLocked(tags).toGraph()

	return &cp, nil
}

// DeleteGraph deletes a behavior graph.
func (b *InMemoryBackend) DeleteGraph(graphARN string) error {
	b.mu.Lock("DeleteGraph")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	b.graphs.Delete(graphARN)

	for _, m := range slices.Clone(b.membersByGraph.Get(graphARN)) {
		b.members.Delete(memberKey(m.GraphARN, m.AccountID))
	}

	for _, inv := range slices.Clone(b.investigationsByGraph.Get(graphARN)) {
		b.investigations.Delete(investigationKey(inv.GraphARN, inv.InvestigationID))
	}

	delete(b.tags, graphARN)
	delete(b.datasources, graphARN)
	delete(b.orgConfigs, graphARN)

	return nil
}

// encodePageToken encodes a pagination offset as an opaque base64 token.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken decodes an opaque base64 pagination token back to an offset.
// Returns 0 and no error when the token is empty.
func decodePageToken(tok string) (int, error) {
	if tok == "" {
		return 0, nil
	}

	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid pagination token", ErrValidation)
	}

	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, fmt.Errorf("%w: invalid pagination token", ErrValidation)
	}

	return n, nil
}

// Indicator type constants mirroring Amazon Detective's FindingType enum.
const (
	indicatorImpossibleTravel    = "IMPOSSIBLE_TRAVEL"
	indicatorFlaggedIPAddress    = "FLAGGED_IP_ADDRESS"
	indicatorNewGeolocation      = "NEW_GEOLOCATION"
	indicatorTTPObserved         = "TTP_OBSERVED"
	indicatorRelatedFinding      = "RELATED_FINDING"
	indicatorRelatedFindingGroup = "RELATED_FINDING_GROUP"
)

// builtInIndicators returns a deterministic set of indicators for an investigation.
// Real Detective derives indicators from VPC Flow Logs, CloudTrail, and GuardDuty.
// The emulator generates a fixed representative set seeded by the investigation ID
// so repeated calls for the same investigation return consistent results.
func builtInIndicators(inv *storedInvestigation) []*Indicator {
	indicators := []*Indicator{
		{
			IndicatorType: indicatorTTPObserved,
			Title:         "Observed tactics, techniques and procedures for entity " + inv.EntityARN,
		},
		{
			IndicatorType: indicatorNewGeolocation,
			Title:         "API calls from a previously unseen geographic location",
		},
	}

	// Higher-severity investigations include richer indicator sets.
	if inv.Severity == severityMedium || inv.Severity == severityHigh || inv.Severity == severityCritical {
		indicators = append(indicators,
			&Indicator{
				IndicatorType: indicatorFlaggedIPAddress,
				Title:         "Activity from a threat-intelligence flagged IP address",
			},
			&Indicator{
				IndicatorType: indicatorImpossibleTravel,
				Title:         "Geographically impossible API activity within a short time window",
			},
		)
	}

	if inv.Severity == severityHigh || inv.Severity == severityCritical {
		indicators = append(indicators,
			&Indicator{
				IndicatorType: indicatorRelatedFinding,
				Title:         "Related GuardDuty finding associated with the entity",
			},
			&Indicator{
				IndicatorType: indicatorRelatedFindingGroup,
				Title:         "Cluster of related findings involving the same entity",
			},
		)
	}

	return indicators
}

// ListGraphs returns behavior graphs for the admin account.
func (b *InMemoryBackend) ListGraphs(maxResults int32, nextToken string) ([]*Graph, string, error) {
	b.mu.RLock("ListGraphs")
	defer b.mu.RUnlock()

	graphs := b.graphs.Snapshot()

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(graphs) {
		start = len(graphs)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxGraphsPerPage {
		limit = maxGraphsPerPage
	}

	end := min(start+limit, len(graphs))

	result := make([]*Graph, 0, end-start)
	for _, g := range graphs[start:end] {
		cp := g.toGraph()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(graphs) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// CreateMembers creates or invites member accounts to a behavior graph.
func (b *InMemoryBackend) CreateMembers(
	graphARN string,
	accounts []Account,
	_ string,
) ([]*MemberDetail, []UnprocessedAccount, error) {
	if len(accounts) > maxCreateMembersPerBatch {
		return nil, nil, fmt.Errorf("%w: cannot specify more than %d accounts", ErrValidation, maxCreateMembersPerBatch)
	}

	for _, acc := range accounts {
		if !validateAccountID(acc.AccountID) {
			return nil, nil, fmt.Errorf("%w: account ID must be a 12-digit number", ErrValidation)
		}

		if acc.EmailAddress != "" && !validateEmail(acc.EmailAddress) {
			return nil, nil, fmt.Errorf("%w: invalid email address format", ErrValidation)
		}
	}

	b.mu.Lock("CreateMembers")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	now := time.Now().UTC()

	var members []*MemberDetail
	var unprocessed []UnprocessedAccount

	for _, acc := range accounts {
		if acc.AccountID == b.accountID {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: acc.AccountID,
				Reason:    "Cannot invite the administrator account",
			})

			continue
		}

		// AWS documents that CreateMembers cannot re-process an account that
		// was already invited: it is reported back via UnprocessedAccounts,
		// not silently returned as a freshly-processed member.
		if b.members.Has(memberKey(graphARN, acc.AccountID)) {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: acc.AccountID,
				Reason:    "Account is already a member of the behavior graph",
			})

			continue
		}

		m := &storedMember{
			AccountID:       acc.AccountID,
			AdministratorID: b.accountID,
			EmailAddress:    acc.EmailAddress,
			GraphARN:        graphARN,
			InvitedTime:     now,
			Status:          memberStatusInvited,
			UpdatedTime:     now,
		}
		b.members.Put(m)
		cp := m.toMemberDetail()
		members = append(members, &cp)
	}

	return members, unprocessed, nil
}

// DeleteMembers removes member accounts from a behavior graph.
func (b *InMemoryBackend) DeleteMembers(
	graphARN string,
	accountIDs []string,
) ([]string, []UnprocessedAccount, error) {
	b.mu.Lock("DeleteMembers")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	deleted := make([]string, 0, len(accountIDs))
	unprocessed := make([]UnprocessedAccount, 0)

	for _, id := range accountIDs {
		key := memberKey(graphARN, id)
		if !b.members.Has(key) {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    "Member account not found in behavior graph", //nolint:goconst // existing issue.
			})

			continue
		}
		b.members.Delete(key)
		deleted = append(deleted, id)
	}

	return deleted, unprocessed, nil
}

// GetMembers returns member details for the given account IDs.
func (b *InMemoryBackend) GetMembers(
	graphARN string,
	accountIDs []string,
) ([]*MemberDetail, []UnprocessedAccount, error) {
	b.mu.RLock("GetMembers")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	var members []*MemberDetail
	var unprocessed []UnprocessedAccount

	for _, id := range accountIDs {
		if m, ok := b.members.Get(memberKey(graphARN, id)); ok {
			cp := m.toMemberDetail()
			members = append(members, &cp)
		} else {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    "Member account not found in behavior graph",
			})
		}
	}

	return members, unprocessed, nil
}

// ListMembers returns member accounts for a behavior graph.
func (b *InMemoryBackend) ListMembers(
	graphARN string,
	maxResults int32,
	nextToken string,
) ([]*MemberDetail, string, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, "", ErrGraphNotFound
	}

	items := slices.Clone(b.membersByGraph.Get(graphARN))
	sort.Slice(items, func(i, j int) bool { return items[i].AccountID < items[j].AccountID })

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(items) {
		start = len(items)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxMembersPerPage {
		limit = maxMembersPerPage
	}

	end := min(start+limit, len(items))

	result := make([]*MemberDetail, 0, end-start)
	for _, m := range items[start:end] {
		cp := m.toMemberDetail()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(items) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceARN) {
		return ErrGraphNotFound
	}

	existing := b.tags[resourceARN]
	if existing == nil {
		existing = make(map[string]string)
		b.tags[resourceARN] = existing
	}

	newCount := len(existing)
	for k := range tags {
		if _, alreadyExists := existing[k]; !alreadyExists {
			newCount++
		}
	}

	if newCount > maxTagCount {
		return fmt.Errorf("%w: resource would exceed the %d tag limit", ErrValidation, maxTagCount)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceARN) {
		return ErrGraphNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownResource(resourceARN) {
		return nil, ErrGraphNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}

// isKnownResource returns true if the ARN corresponds to a known graph.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownResource(arn string) bool {
	return b.graphs.Has(arn)
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// AcceptInvitation accepts a graph invitation on behalf of the member account.
func (b *InMemoryBackend) AcceptInvitation(graphARN string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, b.accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusInvited {
		return fmt.Errorf("%w: member status must be INVITED", ErrValidation)
	}

	now := time.Now().UTC()
	m.Status = memberStatusEnabled
	m.UpdatedTime = now

	return nil
}

// RejectInvitation rejects a graph invitation on behalf of the member account.
func (b *InMemoryBackend) RejectInvitation(graphARN string) error {
	b.mu.Lock("RejectInvitation")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, b.accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusInvited {
		return fmt.Errorf("%w: member status must be INVITED", ErrValidation)
	}

	b.members.Delete(memberKey(graphARN, b.accountID))

	return nil
}

// DisassociateMembership removes the calling account from a graph it belongs to.
func (b *InMemoryBackend) DisassociateMembership(graphARN string) error {
	b.mu.Lock("DisassociateMembership")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, b.accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusEnabled {
		return fmt.Errorf("%w: member status must be ENABLED", ErrValidation)
	}

	b.members.Delete(memberKey(graphARN, b.accountID))

	return nil
}

// ListInvitations returns graphs where this account has an open or accepted invitation.
func (b *InMemoryBackend) ListInvitations(maxResults int32, nextToken string) ([]*MemberDetail, string, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	var invitations []*MemberDetail
	for _, m := range b.members.All() {
		if m.AccountID == b.accountID && (m.Status == memberStatusInvited || m.Status == memberStatusEnabled) {
			cp := m.toMemberDetail()
			invitations = append(invitations, &cp)
		}
	}

	sort.Slice(invitations, func(i, j int) bool {
		return invitations[i].GraphARN < invitations[j].GraphARN
	})

	start := 0
	if nextToken != "" {
		for i, inv := range invitations {
			if inv.GraphARN == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxInvitationsPerPage {
		limit = maxInvitationsPerPage
	}

	end := min(start+limit, len(invitations))
	result := invitations[start:end]

	var outToken string
	if end < len(invitations) {
		outToken = invitations[end].GraphARN
	}

	return result, outToken, nil
}

// deriveEntityType returns the IAM_ROLE or IAM_USER entity type for entityARN.
// The real StartInvestigation request has no EntityType input member -- Detective
// derives it server-side from the resource segment of the IAM entity ARN -- so
// the emulator must do the same instead of trusting a client-supplied value.
func deriveEntityType(entityARN string) (string, error) {
	parts := strings.SplitN(entityARN, ":", iamARNPartsLen)
	if len(parts) != iamARNPartsLen || parts[0] != "arn" || parts[2] != "iam" {
		return "", fmt.Errorf("%w: EntityArn must be an IAM user or role ARN", ErrValidation)
	}

	resource := parts[iamARNPartsLen-1]

	switch {
	case strings.HasPrefix(resource, "role/"):
		return entityTypeIAMRole, nil
	case strings.HasPrefix(resource, "user/"):
		return entityTypeIAMUser, nil
	default:
		return "", fmt.Errorf("%w: EntityArn must be an IAM user or role ARN", ErrValidation)
	}
}

// StartInvestigation creates a new investigation for an entity within a graph.
func (b *InMemoryBackend) StartInvestigation(
	graphARN, entityARN string,
	scopeStart, scopeEnd time.Time,
) (string, error) {
	entityType, typeErr := deriveEntityType(entityARN)
	if typeErr != nil {
		return "", typeErr
	}

	b.mu.Lock("StartInvestigation")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return "", ErrGraphNotFound
	}

	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	now := time.Now().UTC()

	inv := &storedInvestigation{
		CreatedTime:     now,
		ScopeStartTime:  scopeStart,
		ScopeEndTime:    scopeEnd,
		GraphARN:        graphARN,
		InvestigationID: id,
		EntityARN:       entityARN,
		EntityType:      entityType,
		Severity:        severityInformational,
		State:           investigationStateActive,
		Status:          investigationStatusRunning,
	}

	b.investigations.Put(inv)

	return id, nil
}

// GetInvestigation returns an investigation by graph ARN and investigation ID.
func (b *InMemoryBackend) GetInvestigation(graphARN, investigationID string) (*Investigation, error) {
	b.mu.RLock("GetInvestigation")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, ErrGraphNotFound
	}

	inv, ok := b.investigations.Get(investigationKey(graphARN, investigationID))
	if !ok {
		return nil, ErrMemberNotFound
	}

	cp := inv.toInvestigation()

	return &cp, nil
}

// ListInvestigations returns investigations for a graph.
func (b *InMemoryBackend) ListInvestigations(
	graphARN string,
	maxResults int32,
	nextToken string,
) ([]*InvestigationDetail, string, error) {
	b.mu.RLock("ListInvestigations")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, "", ErrGraphNotFound
	}

	items := slices.Clone(b.investigationsByGraph.Get(graphARN))
	sort.Slice(items, func(i, j int) bool { return items[i].InvestigationID < items[j].InvestigationID })

	start := 0
	if nextToken != "" {
		for i, inv := range items {
			if inv.InvestigationID == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxInvestigationsPerPage {
		limit = maxInvestigationsPerPage
	}

	end := min(start+limit, len(items))

	result := make([]*InvestigationDetail, 0, end-start)
	for _, inv := range items[start:end] {
		d := inv.toDetail()
		result = append(result, &d)
	}

	var outToken string
	if end < len(items) {
		outToken = items[end].InvestigationID
	}

	return result, outToken, nil
}

// UpdateInvestigationState updates the state of an investigation.
func (b *InMemoryBackend) UpdateInvestigationState(graphARN, investigationID, state string) error {
	if state != investigationStateActive && state != investigationStateArchived {
		return fmt.Errorf("%w: invalid State %q", ErrValidation, state)
	}

	b.mu.Lock("UpdateInvestigationState")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	inv, ok := b.investigations.Get(investigationKey(graphARN, investigationID))
	if !ok {
		return ErrMemberNotFound
	}

	inv.State = state

	return nil
}

// ListIndicators returns indicators for an investigation.
func (b *InMemoryBackend) ListIndicators(
	graphARN, investigationID, indicatorType string,
	maxResults int32,
	nextToken string,
) ([]*Indicator, string, error) {
	b.mu.RLock("ListIndicators")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, "", ErrGraphNotFound
	}

	inv, ok := b.investigations.Get(investigationKey(graphARN, investigationID))
	if !ok {
		return nil, "", ErrMemberNotFound
	}

	all := builtInIndicators(inv)

	if indicatorType != "" {
		filtered := make([]*Indicator, 0, len(all))
		for _, ind := range all {
			if ind.IndicatorType == indicatorType {
				filtered = append(filtered, ind)
			}
		}
		all = filtered
	}

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(all) {
		start = len(all)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxIndicatorsPerPage {
		limit = maxIndicatorsPerPage
	}

	end := min(start+limit, len(all))

	var outToken string
	if end < len(all) {
		outToken = encodePageToken(end)
	}

	return all[start:end], outToken, nil
}

// ListDatasourcePackages returns datasource package ingest details for a graph.
func (b *InMemoryBackend) ListDatasourcePackages(
	graphARN string,
	maxResults int32,
	nextToken string,
) (map[string]DatasourcePackageIngestDetail, string, error) {
	b.mu.RLock("ListDatasourcePackages")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, "", ErrGraphNotFound
	}

	pkgMap := b.datasources[graphARN]
	keys := collections.SortedKeys(pkgMap)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxDatasourcesPerPage {
		limit = maxDatasourcesPerPage
	}

	end := min(start+limit, len(keys))

	result := make(map[string]DatasourcePackageIngestDetail, end-start)
	for _, k := range keys[start:end] {
		result[k] = DatasourcePackageIngestDetail{IngestState: pkgMap[k]}
	}

	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	}

	return result, outToken, nil
}

// UpdateDatasourcePackages enables datasource packages on a graph.
func (b *InMemoryBackend) UpdateDatasourcePackages(graphARN string, packages []string) error {
	b.mu.Lock("UpdateDatasourcePackages")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	if b.datasources[graphARN] == nil {
		b.datasources[graphARN] = make(map[string]string)
	}

	for _, pkg := range packages {
		b.datasources[graphARN][pkg] = datasourceIngestStateStarted
	}

	return nil
}

// BatchGetGraphMemberDatasources returns datasource package info for member accounts of a graph.
func (b *InMemoryBackend) BatchGetGraphMemberDatasources(
	graphARN string,
	accountIDs []string,
) ([]MembershipDatasources, []UnprocessedAccount, error) {
	b.mu.RLock("BatchGetGraphMemberDatasources")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	var results []MembershipDatasources
	var unprocessed []UnprocessedAccount

	for _, id := range accountIDs {
		if !b.members.Has(memberKey(graphARN, id)) {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    "Member account not found in behavior graph",
			})

			continue
		}

		pkgStates := make(map[string]string)
		if pkgMap := b.datasources[graphARN]; pkgMap != nil {
			maps.Copy(pkgStates, pkgMap)
		}

		results = append(results, MembershipDatasources{
			AccountID:                     id,
			GraphARN:                      graphARN,
			DatasourcePackageIngestStates: pkgStates,
		})
	}

	return results, unprocessed, nil
}

// BatchGetMembershipDatasources returns datasource history for the account across graphs.
func (b *InMemoryBackend) BatchGetMembershipDatasources(
	graphARNs []string,
) ([]MembershipDatasources, []UnprocessedGraph, error) {
	b.mu.RLock("BatchGetMembershipDatasources")
	defer b.mu.RUnlock()

	var results []MembershipDatasources
	var unprocessed []UnprocessedGraph

	for _, graphARN := range graphARNs {
		if !b.graphs.Has(graphARN) {
			unprocessed = append(unprocessed, UnprocessedGraph{
				GraphArn: graphARN,
				Reason:   "Graph not found",
			})

			continue
		}

		pkgStates := make(map[string]string)
		if pkgMap := b.datasources[graphARN]; pkgMap != nil {
			maps.Copy(pkgStates, pkgMap)
		}

		results = append(results, MembershipDatasources{
			AccountID:                     b.accountID,
			GraphARN:                      graphARN,
			DatasourcePackageIngestStates: pkgStates,
		})
	}

	return results, unprocessed, nil
}

// EnableOrganizationAdminAccount designates a Detective administrator account.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(accountID string) error {
	if !validateAccountID(accountID) {
		return fmt.Errorf("%w: accountId must be a 12-digit number", ErrValidation)
	}

	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	var graphARN string
	if all := b.graphs.All(); len(all) > 0 {
		graphARN = all[0].Arn
	} else {
		// AWS: "If the account does not have Detective enabled, then enables
		// Detective for that account and creates a new behavior graph."
		graphARN = b.createGraphLocked(nil).Arn
	}

	now := time.Now().UTC()
	b.orgAdmins = append(b.orgAdmins, &storedOrgAdmin{
		DelegationTime: now,
		AccountID:      accountID,
		GraphARN:       graphARN,
	})

	return nil
}

// DisableOrganizationAdminAccount removes the Detective administrator account.
func (b *InMemoryBackend) DisableOrganizationAdminAccount() error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdmins = nil

	return nil
}

// ListOrganizationAdminAccounts returns Detective organization administrator accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts(
	maxResults int32,
	nextToken string,
) ([]*OrgAdmin, string, error) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	admins := b.orgAdmins
	start := 0
	if nextToken != "" {
		for i, a := range admins {
			if a.AccountID == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxOrgAdminsPerPage {
		limit = maxOrgAdminsPerPage
	}

	end := min(start+limit, len(admins))

	result := make([]*OrgAdmin, 0, end-start)
	for _, a := range admins[start:end] {
		cp := OrgAdmin{
			DelegationTime: a.DelegationTime,
			AccountID:      a.AccountID,
			GraphARN:       a.GraphARN,
		}
		result = append(result, &cp)
	}

	var outToken string
	if end < len(admins) {
		outToken = admins[end].AccountID
	}

	return result, outToken, nil
}

// DescribeOrganizationConfiguration returns AutoEnable setting for a graph.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(graphARN string) (bool, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return false, ErrGraphNotFound
	}

	return b.orgConfigs[graphARN], nil
}

// UpdateOrganizationConfiguration sets the AutoEnable flag for a graph.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(graphARN string, autoEnable bool) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	b.orgConfigs[graphARN] = autoEnable

	return nil
}

// StartMonitoringMember enables monitoring for a member in ACCEPTED_BUT_DISABLED state.
func (b *InMemoryBackend) StartMonitoringMember(graphARN, accountID string) error {
	b.mu.Lock("StartMonitoringMember")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusAcceptedDisabled {
		return fmt.Errorf("%w: member status must be ACCEPTED_BUT_DISABLED", ErrValidation)
	}

	now := time.Now().UTC()
	m.Status = memberStatusEnabled
	m.UpdatedTime = now

	return nil
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.datasources = make(map[string]map[string]string)
	b.orgAdmins = nil
	b.orgConfigs = make(map[string]bool)
}
