package quicksight

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// filterFlowAssetName is the SearchFlows filter attribute name for matching
// on a flow's display name (the "assetName" filter per the QuickSight API).
const filterFlowAssetName = "assetName"

// storedFlow is the persisted representation of a QuickSight flow.
// CreateFlow was added to the QuickSight API after the prior parity pass
// (see PARITY.md); seedFlow remains available for tests that want to
// populate a flow without exercising the full Create/Describe/Update
// lifecycle (e.g. fixtures for List/Search/GetFlowMetadata that don't care
// about FlowDefinition/StepAliases).
type storedFlow struct {
	CreatedTime     time.Time            `json:"createdTime"`
	LastUpdatedTime time.Time            `json:"lastUpdatedTime"`
	LastPublishedAt time.Time            `json:"lastPublishedAt,omitzero"`
	FlowDefinition  map[string]any       `json:"flowDefinition,omitempty"`
	Description     string               `json:"description,omitempty"`
	Arn             string               `json:"arn"`
	Name            string               `json:"name"`
	FlowID          string               `json:"flowId"`
	CreatedBy       string               `json:"createdBy,omitempty"`
	LastPublishedBy string               `json:"lastPublishedBy,omitempty"`
	LastUpdatedBy   string               `json:"lastUpdatedBy,omitempty"`
	PublishState    string               `json:"publishState"`
	Permissions     []ResourcePermission `json:"permissions,omitempty"`
	RunCount        int32                `json:"runCount"`
	UserCount       int32                `json:"userCount"`
}

func (f *storedFlow) toFlow() *Flow {
	return &Flow{
		CreatedTime:     f.CreatedTime,
		LastUpdatedTime: f.LastUpdatedTime,
		LastPublishedAt: f.LastPublishedAt,
		FlowID:          f.FlowID,
		Arn:             f.Arn,
		Name:            f.Name,
		Description:     f.Description,
		CreatedBy:       f.CreatedBy,
		LastPublishedBy: f.LastPublishedBy,
		LastUpdatedBy:   f.LastUpdatedBy,
		PublishState:    f.PublishState,
		RunCount:        f.RunCount,
		UserCount:       f.UserCount,
		Permissions:     clonePermissions(f.Permissions),
	}
}

// toFlowDetail returns the FlowDetail-shaped view used by DescribeFlow,
// which (unlike FlowSummary) carries FlowDefinition and StepAliases but
// omits RunCount/UserCount/LastPublishedAt/LastPublishedBy (real
// FlowDetail has no such fields). StepAliases is always empty: real AWS
// derives it from parsing the flow definition's steps, which this backend
// stores opaquely (see CreateFlow) rather than interpreting -- an honest
// omission, not a fabricated value.
func (f *storedFlow) toFlowDetail() *Flow {
	return &Flow{
		CreatedTime:     f.CreatedTime,
		LastUpdatedTime: f.LastUpdatedTime,
		FlowDefinition:  f.FlowDefinition,
		FlowID:          f.FlowID,
		Arn:             f.Arn,
		Name:            f.Name,
		Description:     f.Description,
		CreatedBy:       f.CreatedBy,
		LastUpdatedBy:   f.LastUpdatedBy,
		PublishState:    f.PublishState,
	}
}

// ---- Flow CRUD ----

const flowPublishStatePublished = "PUBLISHED"

// CreateFlow creates a new flow. Real AWS creates both a DRAFT and a
// PUBLISHED (auto-published) version; this backend has no versioning
// concept, so it stores a single definition and reports PublishState
// PUBLISHED, matching the real op's documented auto-publish behavior.
func (b *InMemoryBackend) CreateFlow(
	_, name, description string,
	flowDefinition map[string]any,
	permissions []ResourcePermission,
) (*Flow, error) {
	if name == "" || flowDefinition == nil {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateFlow")
	defer b.mu.Unlock()

	flowID := uuid.New().String()
	now := time.Now().UTC()
	f := &storedFlow{
		CreatedTime:     now,
		LastUpdatedTime: now,
		LastPublishedAt: now,
		FlowDefinition:  flowDefinition,
		Description:     description,
		Arn:             b.buildARN("flow", flowID),
		Name:            name,
		FlowID:          flowID,
		PublishState:    flowPublishStatePublished,
		Permissions:     clonePermissions(permissions),
	}
	b.flows.Put(f)

	return f.toFlow(), nil
}

// DescribeFlow returns the FlowDetail-shaped view of a flow. Real AWS scopes
// the response to a requested PublishState (DRAFT/PUBLISHED/
// PENDING_APPROVAL); this backend stores a single definition (no draft/
// published divergence), so publishState is accepted by the handler for
// wire fidelity but doesn't change which data is returned.
func (b *InMemoryBackend) DescribeFlow(accountID, flowID string) (*Flow, error) {
	b.mu.RLock("DescribeFlow")
	defer b.mu.RUnlock()

	f, ok := b.flows.Get(flowKey(accountID, flowID))
	if !ok {
		return nil, ErrFlowNotFound
	}

	return f.toFlowDetail(), nil
}

// UpdateFlow updates a flow's mutable fields. Per CreateFlow, replacing
// FlowDefinition replaces the entire definition (matching the real op's
// documented all-or-nothing step replacement).
func (b *InMemoryBackend) UpdateFlow(
	accountID, flowID, name, description string,
	flowDefinition map[string]any,
) (*Flow, error) {
	b.mu.Lock("UpdateFlow")
	defer b.mu.Unlock()

	key := flowKey(accountID, flowID)
	f, ok := b.flows.Get(key)
	if !ok {
		return nil, ErrFlowNotFound
	}

	if name != "" {
		f.Name = name
	}
	if description != "" {
		f.Description = description
	}
	if flowDefinition != nil {
		f.FlowDefinition = flowDefinition
	}
	f.LastUpdatedTime = time.Now().UTC()

	return f.toFlow(), nil
}

func (b *InMemoryBackend) DeleteFlow(accountID, flowID string) error {
	b.mu.Lock("DeleteFlow")
	defer b.mu.Unlock()

	key := flowKey(accountID, flowID)
	f, ok := b.flows.Get(key)
	if !ok {
		return ErrFlowNotFound
	}

	delete(b.tags, f.Arn)
	b.flows.Delete(key)

	return nil
}

func flowKey(accountID, flowID string) string {
	return accountID + "/" + flowID
}

// seedFlow inserts f directly into backend state. Exported for tests only
// (via export_test.go's SeedFlow) since QuickSight has no CreateFlow API.
func (b *InMemoryBackend) seedFlow(_ string, f *Flow) {
	b.mu.Lock("seedFlow")
	defer b.mu.Unlock()

	arn := f.Arn
	if arn == "" {
		arn = b.buildARN("flow", f.FlowID)
	}

	b.flows.Put(&storedFlow{
		CreatedTime:     f.CreatedTime,
		LastUpdatedTime: f.LastUpdatedTime,
		LastPublishedAt: f.LastPublishedAt,
		FlowID:          f.FlowID,
		Arn:             arn,
		Name:            f.Name,
		Description:     f.Description,
		CreatedBy:       f.CreatedBy,
		LastPublishedBy: f.LastPublishedBy,
		LastUpdatedBy:   f.LastUpdatedBy,
		PublishState:    f.PublishState,
		RunCount:        f.RunCount,
		UserCount:       f.UserCount,
		Permissions:     clonePermissions(f.Permissions),
	})
}

// ---- Flows ----

func (b *InMemoryBackend) ListFlows(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*Flow, string, error) {
	b.mu.RLock("ListFlows")
	defer b.mu.RUnlock()

	all := b.flows.All()
	sort.Slice(all, func(i, j int) bool { return all[i].FlowID < all[j].FlowID })

	result, next := paginateFlows(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchFlows(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Flow, string, error) {
	b.mu.RLock("SearchFlows")
	defer b.mu.RUnlock()

	var filtered []*storedFlow
	for _, f := range b.flows.All() {
		if matchesAllNameFilters(f.Name, filters, filterFlowAssetName) {
			filtered = append(filtered, f)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].FlowID < filtered[j].FlowID })

	result, next := paginateFlows(filtered, maxResults, nextToken)

	return result, next, nil
}

func paginateFlows(all []*storedFlow, maxResults int32, nextToken string) ([]*Flow, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, f := range all {
			if f.FlowID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].FlowID
	} else {
		end = len(all)
	}

	result := make([]*Flow, 0, end-start)
	for _, f := range all[start:end] {
		result = append(result, f.toFlow())
	}

	return result, next
}

func (b *InMemoryBackend) GetFlowMetadata(accountID, flowID string) (*Flow, error) {
	b.mu.RLock("GetFlowMetadata")
	defer b.mu.RUnlock()

	f, ok := b.flows.Get(flowKey(accountID, flowID))
	if !ok {
		// GetFlowMetadata's own deserializer models InvalidParameterValueException,
		// not ResourceNotFoundException, for an unresolvable FlowId -- unlike
		// CreateFlow/DescribeFlow/UpdateFlow/DeleteFlow, which do model it
		// (quicksight@v1.123.1 deserializers.go).
		return nil, fmt.Errorf("%w: flow %q not found", ErrValidation, flowID)
	}

	return f.toFlow(), nil
}

// ---- Flow permissions ----

func (b *InMemoryBackend) GetFlowPermissions(accountID, flowID string) (*Flow, []ResourcePermission, error) {
	b.mu.RLock("GetFlowPermissions")
	defer b.mu.RUnlock()

	f, ok := b.flows.Get(flowKey(accountID, flowID))
	if !ok {
		// GetFlowPermissions's own deserializer models InvalidParameterValueException,
		// not ResourceNotFoundException, for an unresolvable FlowId.
		return nil, nil, fmt.Errorf("%w: flow %q not found", ErrValidation, flowID)
	}

	return f.toFlow(), clonePermissions(f.Permissions), nil
}

func (b *InMemoryBackend) UpdateFlowPermissions(
	accountID, flowID string,
	grant, revoke []ResourcePermission,
) (*Flow, []ResourcePermission, error) {
	b.mu.Lock("UpdateFlowPermissions")
	defer b.mu.Unlock()

	key := flowKey(accountID, flowID)
	f, ok := b.flows.Get(key)
	if !ok {
		// UpdateFlowPermissions's own deserializer models
		// InvalidParameterValueException, not ResourceNotFoundException, for an
		// unresolvable FlowId.
		return nil, nil, fmt.Errorf("%w: flow %q not found", ErrValidation, flowID)
	}

	f.Permissions = applyGrantRevoke(f.Permissions, grant, revoke)
	f.LastUpdatedTime = time.Now().UTC()

	return f.toFlow(), clonePermissions(f.Permissions), nil
}
