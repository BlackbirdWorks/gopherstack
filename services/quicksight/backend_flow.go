package quicksight

import (
	"sort"
	"strings"
	"time"
)

// filterFlowAssetName is the SearchFlows filter attribute name for matching
// on a flow's display name (the "assetName" filter per the QuickSight API).
const filterFlowAssetName = "assetName"

// storedFlow is the persisted representation of a QuickSight flow. There is
// no CreateFlow operation in the QuickSight API (flows are authored via the
// console/Quick Suite); flows only ever enter backend state via seedFlow,
// used by tests to populate fixtures for List/Search/Get.
type storedFlow struct {
	CreatedTime     time.Time            `json:"createdTime"`
	LastUpdatedTime time.Time            `json:"lastUpdatedTime"`
	LastPublishedAt time.Time            `json:"lastPublishedAt,omitzero"`
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

func flowKey(accountID, flowID string) string {
	return accountID + "/" + flowID
}

// seedFlow inserts f directly into backend state. Exported for tests only
// (via export_test.go's SeedFlow) since QuickSight has no CreateFlow API.
func (b *InMemoryBackend) seedFlow(accountID string, f *Flow) {
	b.mu.Lock("seedFlow")
	defer b.mu.Unlock()

	arn := f.Arn
	if arn == "" {
		arn = b.buildARN("flow", f.FlowID)
	}

	b.flows[flowKey(accountID, f.FlowID)] = &storedFlow{
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
	}
}

// ---- Flows ----

func (b *InMemoryBackend) ListFlows(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Flow, string, error) {
	b.mu.RLock("ListFlows")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedFlow
	for k, f := range b.flows {
		if strings.HasPrefix(k, prefix) {
			all = append(all, f)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].FlowID < all[j].FlowID })

	result, next := paginateFlows(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchFlows(
	accountID string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Flow, string, error) {
	b.mu.RLock("SearchFlows")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var filtered []*storedFlow
	for k, f := range b.flows {
		if strings.HasPrefix(k, prefix) && matchesAllNameFilters(f.Name, filters, filterFlowAssetName) {
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

	f, ok := b.flows[flowKey(accountID, flowID)]
	if !ok {
		return nil, ErrFlowNotFound
	}

	return f.toFlow(), nil
}

// ---- Flow permissions ----

func (b *InMemoryBackend) GetFlowPermissions(accountID, flowID string) (*Flow, []ResourcePermission, error) {
	b.mu.RLock("GetFlowPermissions")
	defer b.mu.RUnlock()

	f, ok := b.flows[flowKey(accountID, flowID)]
	if !ok {
		return nil, nil, ErrFlowNotFound
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
	f, ok := b.flows[key]
	if !ok {
		return nil, nil, ErrFlowNotFound
	}

	f.Permissions = applyGrantRevoke(f.Permissions, grant, revoke)
	f.LastUpdatedTime = time.Now().UTC()

	return f.toFlow(), clonePermissions(f.Permissions), nil
}
