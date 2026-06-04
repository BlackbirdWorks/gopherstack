package detective

import (
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

	memberStatusInvited = "INVITED"

	maxGraphsPerPage  = 200
	maxMembersPerPage = 200

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

// snapshot holds serializable backend state.
type snapshot struct {
	Graphs  map[string]*storedGraph             `json:"graphs"`
	Members map[string]map[string]*storedMember `json:"members"` // graphARN → accountID → member
	Tags    map[string]map[string]string        `json:"tags"`
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
	mu        *lockmetrics.RWMutex
	graphs    map[string]*storedGraph             // graphARN → graph
	members   map[string]map[string]*storedMember // graphARN → accountID → member
	tags      map[string]map[string]string        // resourceARN → tags
	accountID string
	region    string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:        lockmetrics.New("detective"),
		accountID: accountID,
		region:    region,
		graphs:    make(map[string]*storedGraph),
		members:   make(map[string]map[string]*storedMember),
		tags:      make(map[string]map[string]string),
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
				Reason:    "Member account not found in behavior graph",
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

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.graphs = make(map[string]*storedGraph)
	b.members = make(map[string]map[string]*storedMember)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		Graphs:  b.graphs,
		Members: b.members,
		Tags:    b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
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

	return nil
}
