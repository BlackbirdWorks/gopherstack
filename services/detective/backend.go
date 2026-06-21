package detective

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// snapshot holds serializable backend state.
type snapshot struct {
	Graphs         map[string]*storedGraph                    `json:"graphs"`
	Members        map[string]map[string]*storedMember        `json:"members"`
	Tags           map[string]map[string]string               `json:"tags"`
	Investigations map[string]map[string]*storedInvestigation `json:"investigations"`
	Datasources    map[string]map[string]string               `json:"datasources"`
	OrgConfigs     map[string]bool                            `json:"orgConfigs"`
	OrgAdmins      []*storedOrgAdmin                          `json:"orgAdmins"`
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
type InMemoryBackend struct {
	mu             *lockmetrics.RWMutex
	graphs         map[string]*storedGraph
	members        map[string]map[string]*storedMember
	tags           map[string]map[string]string
	investigations map[string]map[string]*storedInvestigation
	datasources    map[string]map[string]string
	orgConfigs     map[string]bool
	accountID      string
	region         string
	orgAdmins      []*storedOrgAdmin
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:             lockmetrics.New("detective"),
		accountID:      accountID,
		region:         region,
		graphs:         make(map[string]*storedGraph),
		members:        make(map[string]map[string]*storedMember),
		tags:           make(map[string]map[string]string),
		investigations: make(map[string]map[string]*storedInvestigation),
		datasources:    make(map[string]map[string]string),
		orgAdmins:      nil,
		orgConfigs:     make(map[string]bool),
	}
}

func (b *InMemoryBackend) graphARN(id string) string {
	return fmt.Sprintf("arn:aws:detective:%s:%s:graph:%s", b.region, b.accountID, id)
}

// CreateGraph creates a new behavior graph. Returns existing one if already created (idempotent).
func (b *InMemoryBackend) CreateGraph(tags map[string]string) (*Graph, error) {
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateGraph")
	defer b.mu.Unlock()

	for _, g := range b.graphs {
		cp := g.toGraph()

		return &cp, nil
	}

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
	b.graphs[arn] = g
	b.members[arn] = make(map[string]*storedMember)

	if len(graphTags) > 0 {
		b.tags[arn] = make(map[string]string)
		maps.Copy(b.tags[arn], graphTags)
	}

	cp := g.toGraph()

	return &cp, nil
}

// DeleteGraph deletes a behavior graph.
func (b *InMemoryBackend) DeleteGraph(graphARN string) error {
	b.mu.Lock("DeleteGraph")
	defer b.mu.Unlock()

	if _, ok := b.graphs[graphARN]; !ok {
		return ErrGraphNotFound
	}

	delete(b.graphs, graphARN)
	delete(b.members, graphARN)
	delete(b.tags, graphARN)

	return nil
}

// ListGraphs returns behavior graphs for the admin account.
func (b *InMemoryBackend) ListGraphs(maxResults int32, nextToken string) ([]*Graph, string, error) {
	b.mu.RLock("ListGraphs")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.graphs))
	for arn := range b.graphs {
		arns = append(arns, arn)
	}
	sort.Strings(arns)

	start := 0
	if nextToken != "" {
		for i, arn := range arns {
			if arn == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxGraphsPerPage {
		limit = maxGraphsPerPage
	}

	end := min(start+limit, len(arns))

	result := make([]*Graph, 0, end-start)
	for _, arn := range arns[start:end] {
		g := b.graphs[arn]
		cp := g.toGraph()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(arns) {
		outToken = arns[end]
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

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, nil, ErrGraphNotFound
	}

	memberMap := b.members[graphARN]
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

		if existing, ok := memberMap[acc.AccountID]; ok {
			cp := existing.toMemberDetail()
			members = append(members, &cp)

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
		memberMap[acc.AccountID] = m
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

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, nil, ErrGraphNotFound
	}

	memberMap := b.members[graphARN]

	deleted := make([]string, 0, len(accountIDs))
	unprocessed := make([]UnprocessedAccount, 0)

	for _, id := range accountIDs {
		if _, ok := memberMap[id]; !ok {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    "Member account not found in behavior graph", //nolint:goconst // existing issue.
			})

			continue
		}
		delete(memberMap, id)
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

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, nil, ErrGraphNotFound
	}

	memberMap := b.members[graphARN]

	var members []*MemberDetail
	var unprocessed []UnprocessedAccount

	for _, id := range accountIDs {
		if m, ok := memberMap[id]; ok {
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

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, "", ErrGraphNotFound
	}

	memberMap := b.members[graphARN]
	ids := make([]string, 0, len(memberMap))
	for id := range memberMap {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxMembersPerPage {
		limit = maxMembersPerPage
	}

	end := min(start+limit, len(ids))

	result := make([]*MemberDetail, 0, end-start)
	for _, id := range ids[start:end] {
		m := memberMap[id]
		cp := m.toMemberDetail()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
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
	_, ok := b.graphs[arn]

	return ok
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// AcceptInvitation accepts a graph invitation on behalf of the member account.
func (b *InMemoryBackend) AcceptInvitation(graphARN string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	memberMap, ok := b.members[graphARN]
	if !ok {
		return ErrGraphNotFound
	}

	m, ok := memberMap[b.accountID]
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

	memberMap, ok := b.members[graphARN]
	if !ok {
		return ErrGraphNotFound
	}

	m, ok := memberMap[b.accountID]
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusInvited {
		return fmt.Errorf("%w: member status must be INVITED", ErrValidation)
	}

	delete(memberMap, b.accountID)

	return nil
}

// DisassociateMembership removes the calling account from a graph it belongs to.
func (b *InMemoryBackend) DisassociateMembership(graphARN string) error {
	b.mu.Lock("DisassociateMembership")
	defer b.mu.Unlock()

	memberMap, ok := b.members[graphARN]
	if !ok {
		return ErrGraphNotFound
	}

	m, ok := memberMap[b.accountID]
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusEnabled {
		return fmt.Errorf("%w: member status must be ENABLED", ErrValidation)
	}

	delete(memberMap, b.accountID)

	return nil
}

// ListInvitations returns graphs where this account has an open or accepted invitation.
func (b *InMemoryBackend) ListInvitations(maxResults int32, nextToken string) ([]*MemberDetail, string, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	var invitations []*MemberDetail
	for graphARN, memberMap := range b.members {
		if m, ok := memberMap[b.accountID]; ok && (m.Status == memberStatusInvited || m.Status == memberStatusEnabled) {
			cp := m.toMemberDetail()
			_ = graphARN
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

// StartInvestigation creates a new investigation for an entity within a graph.
func (b *InMemoryBackend) StartInvestigation(
	graphARN, entityARN, entityType string,
	scopeStart, scopeEnd time.Time,
) (string, error) {
	if _, ok := map[string]bool{entityTypeIAMRole: true, entityTypeIAMUser: true}[entityType]; entityType != "" && !ok {
		return "", fmt.Errorf("%w: invalid EntityType %q", ErrValidation, entityType)
	}

	b.mu.Lock("StartInvestigation")
	defer b.mu.Unlock()

	if _, ok := b.graphs[graphARN]; !ok {
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

	if b.investigations[graphARN] == nil {
		b.investigations[graphARN] = make(map[string]*storedInvestigation)
	}

	b.investigations[graphARN][id] = inv

	return id, nil
}

// GetInvestigation returns an investigation by graph ARN and investigation ID.
func (b *InMemoryBackend) GetInvestigation(graphARN, investigationID string) (*Investigation, error) {
	b.mu.RLock("GetInvestigation")
	defer b.mu.RUnlock()

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, ErrGraphNotFound
	}

	invMap := b.investigations[graphARN]
	inv, ok := invMap[investigationID]
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

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, "", ErrGraphNotFound
	}

	invMap := b.investigations[graphARN]
	ids := make([]string, 0, len(invMap))
	for id := range invMap {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxInvestigationsPerPage {
		limit = maxInvestigationsPerPage
	}

	end := min(start+limit, len(ids))

	result := make([]*InvestigationDetail, 0, end-start)
	for _, id := range ids[start:end] {
		d := invMap[id].toDetail()
		result = append(result, &d)
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
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

	if _, ok := b.graphs[graphARN]; !ok {
		return ErrGraphNotFound
	}

	invMap := b.investigations[graphARN]
	inv, ok := invMap[investigationID]
	if !ok {
		return ErrMemberNotFound
	}

	inv.State = state

	return nil
}

// ListIndicators returns indicators for an investigation.
func (b *InMemoryBackend) ListIndicators(
	graphARN, investigationID, indicatorType string, //nolint:revive // existing issue.
	maxResults int32, //nolint:revive // existing issue.
	nextToken string, //nolint:revive // existing issue.
) ([]*Indicator, string, error) {
	b.mu.RLock("ListIndicators")
	defer b.mu.RUnlock()

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, "", ErrGraphNotFound
	}

	invMap := b.investigations[graphARN]
	if _, ok := invMap[investigationID]; !ok {
		return nil, "", ErrMemberNotFound
	}

	return nil, "", nil
}

// ListDatasourcePackages returns datasource package ingest details for a graph.
func (b *InMemoryBackend) ListDatasourcePackages(
	graphARN string,
	maxResults int32,
	nextToken string,
) (map[string]DatasourcePackageIngestDetail, string, error) {
	b.mu.RLock("ListDatasourcePackages")
	defer b.mu.RUnlock()

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, "", ErrGraphNotFound
	}

	pkgMap := b.datasources[graphARN]
	keys := make([]string, 0, len(pkgMap))
	for k := range pkgMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

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

	if _, ok := b.graphs[graphARN]; !ok {
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

	if _, ok := b.graphs[graphARN]; !ok {
		return nil, nil, ErrGraphNotFound
	}

	memberMap := b.members[graphARN]
	var results []MembershipDatasources
	var unprocessed []UnprocessedAccount

	for _, id := range accountIDs {
		if _, ok := memberMap[id]; !ok {
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
		if _, ok := b.graphs[graphARN]; !ok {
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
	for arn := range b.graphs {
		graphARN = arn

		break
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

	if _, ok := b.graphs[graphARN]; !ok {
		return false, ErrGraphNotFound
	}

	return b.orgConfigs[graphARN], nil
}

// UpdateOrganizationConfiguration sets the AutoEnable flag for a graph.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(graphARN string, autoEnable bool) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.graphs[graphARN]; !ok {
		return ErrGraphNotFound
	}

	b.orgConfigs[graphARN] = autoEnable

	return nil
}

// StartMonitoringMember enables monitoring for a member in ACCEPTED_BUT_DISABLED state.
func (b *InMemoryBackend) StartMonitoringMember(graphARN, accountID string) error {
	b.mu.Lock("StartMonitoringMember")
	defer b.mu.Unlock()

	memberMap, ok := b.members[graphARN]
	if !ok {
		return ErrGraphNotFound
	}

	m, ok := memberMap[accountID]
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

	b.graphs = make(map[string]*storedGraph)
	b.members = make(map[string]map[string]*storedMember)
	b.tags = make(map[string]map[string]string)
	b.investigations = make(map[string]map[string]*storedInvestigation)
	b.datasources = make(map[string]map[string]string)
	b.orgAdmins = nil
	b.orgConfigs = make(map[string]bool)
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		Graphs:         b.graphs,
		Members:        b.members,
		Tags:           b.tags,
		Investigations: b.investigations,
		Datasources:    b.datasources,
		OrgAdmins:      b.orgAdmins,
		OrgConfigs:     b.orgConfigs,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Graphs != nil {
		b.graphs = snap.Graphs
	} else {
		b.graphs = make(map[string]*storedGraph)
	}

	if snap.Members != nil {
		b.members = snap.Members
	} else {
		b.members = make(map[string]map[string]*storedMember)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	if snap.Investigations != nil {
		b.investigations = snap.Investigations
	} else {
		b.investigations = make(map[string]map[string]*storedInvestigation)
	}

	if snap.Datasources != nil {
		b.datasources = snap.Datasources
	} else {
		b.datasources = make(map[string]map[string]string)
	}

	b.orgAdmins = snap.OrgAdmins

	if snap.OrgConfigs != nil {
		b.orgConfigs = snap.OrgConfigs
	} else {
		b.orgConfigs = make(map[string]bool)
	}

	return nil
}
