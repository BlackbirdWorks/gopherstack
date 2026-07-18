package quicksight

import (
	"maps"
	"sort"
	"time"
)

const (
	filterActionConnectorName = "ACTION_CONNECTOR_NAME"
)

// storedActionConnector is the persisted representation of a QuickSight
// action connector.
type storedActionConnector struct {
	CreatedTime          time.Time            `json:"createdTime"`
	LastUpdatedTime      time.Time            `json:"lastUpdatedTime"`
	AuthenticationConfig map[string]any       `json:"authenticationConfig,omitempty"`
	ActionConnectorID    string               `json:"actionConnectorId"`
	Arn                  string               `json:"arn"`
	Name                 string               `json:"name"`
	Type                 string               `json:"type"`
	Description          string               `json:"description,omitempty"`
	VPCConnectionArn     string               `json:"vpcConnectionArn,omitempty"`
	Status               string               `json:"status"`
	Permissions          []ResourcePermission `json:"permissions,omitempty"`
}

func (a *storedActionConnector) toActionConnector() *ActionConnector {
	return &ActionConnector{
		CreatedTime:          a.CreatedTime,
		LastUpdatedTime:      a.LastUpdatedTime,
		AuthenticationConfig: a.AuthenticationConfig,
		ActionConnectorID:    a.ActionConnectorID,
		Arn:                  a.Arn,
		Name:                 a.Name,
		Type:                 a.Type,
		Description:          a.Description,
		VPCConnectionArn:     a.VPCConnectionArn,
		Status:               a.Status,
		Permissions:          clonePermissions(a.Permissions),
	}
}

func actionConnectorKey(accountID, actionConnectorID string) string {
	return accountID + "/" + actionConnectorID
}

// ---- Action Connectors ----

func (b *InMemoryBackend) CreateActionConnector(
	accountID, actionConnectorID, name, connectorType, description, vpcConnectionArn string,
	authenticationConfig map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*ActionConnector, error) {
	if actionConnectorID == "" || name == "" || connectorType == "" || len(authenticationConfig) == 0 {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateActionConnector")
	defer b.mu.Unlock()

	key := actionConnectorKey(accountID, actionConnectorID)
	if b.actionConnectors.Has(key) {
		return nil, ErrActionConnectorAlreadyExists
	}

	now := time.Now().UTC()
	arn := b.buildARN("action-connector", actionConnectorID)
	a := &storedActionConnector{
		CreatedTime:          now,
		LastUpdatedTime:      now,
		AuthenticationConfig: authenticationConfig,
		ActionConnectorID:    actionConnectorID,
		Arn:                  arn,
		Name:                 name,
		Type:                 connectorType,
		Description:          description,
		VPCConnectionArn:     vpcConnectionArn,
		Status:               statusCreationSuccessful,
		Permissions:          clonePermissions(permissions),
	}
	b.actionConnectors.Put(a)

	if len(tags) > 0 {
		b.tags[arn] = maps.Clone(tags)
	}

	return a.toActionConnector(), nil
}

func (b *InMemoryBackend) DescribeActionConnector(accountID, actionConnectorID string) (*ActionConnector, error) {
	b.mu.RLock("DescribeActionConnector")
	defer b.mu.RUnlock()

	a, ok := b.actionConnectors.Get(actionConnectorKey(accountID, actionConnectorID))
	if !ok {
		return nil, ErrActionConnectorNotFound
	}

	return a.toActionConnector(), nil
}

func (b *InMemoryBackend) UpdateActionConnector(
	accountID, actionConnectorID, name, description, vpcConnectionArn string,
	authenticationConfig map[string]any,
) (*ActionConnector, error) {
	b.mu.Lock("UpdateActionConnector")
	defer b.mu.Unlock()

	key := actionConnectorKey(accountID, actionConnectorID)
	a, ok := b.actionConnectors.Get(key)
	if !ok {
		return nil, ErrActionConnectorNotFound
	}

	if name != "" {
		a.Name = name
	}
	if description != "" {
		a.Description = description
	}
	if vpcConnectionArn != "" {
		a.VPCConnectionArn = vpcConnectionArn
	}
	if len(authenticationConfig) > 0 {
		a.AuthenticationConfig = authenticationConfig
	}
	a.LastUpdatedTime = time.Now().UTC()
	a.Status = statusUpdateSuccessful

	return a.toActionConnector(), nil
}

func (b *InMemoryBackend) DeleteActionConnector(accountID, actionConnectorID string) (*ActionConnector, error) {
	b.mu.Lock("DeleteActionConnector")
	defer b.mu.Unlock()

	key := actionConnectorKey(accountID, actionConnectorID)
	a, ok := b.actionConnectors.Get(key)
	if !ok {
		return nil, ErrActionConnectorNotFound
	}

	delete(b.tags, a.Arn)
	b.actionConnectors.Delete(key)

	return a.toActionConnector(), nil
}

func (b *InMemoryBackend) ListActionConnectors(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*ActionConnector, string, error) {
	b.mu.RLock("ListActionConnectors")
	defer b.mu.RUnlock()

	all := b.actionConnectors.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ActionConnectorID < all[j].ActionConnectorID })

	result, next := paginateActionConnectors(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchActionConnectors(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*ActionConnector, string, error) {
	b.mu.RLock("SearchActionConnectors")
	defer b.mu.RUnlock()

	var filtered []*storedActionConnector
	for _, a := range b.actionConnectors.All() {
		if matchesAllNameFilters(a.Name, filters, filterActionConnectorName) {
			filtered = append(filtered, a)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ActionConnectorID < filtered[j].ActionConnectorID })

	result, next := paginateActionConnectors(filtered, maxResults, nextToken)

	return result, next, nil
}

func paginateActionConnectors(
	all []*storedActionConnector,
	maxResults int32,
	nextToken string,
) ([]*ActionConnector, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range all {
			if a.ActionConnectorID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].ActionConnectorID
	} else {
		end = len(all)
	}

	result := make([]*ActionConnector, 0, end-start)
	for _, a := range all[start:end] {
		result = append(result, a.toActionConnector())
	}

	return result, next
}

// ---- Action Connector permissions ----

func (b *InMemoryBackend) DescribeActionConnectorPermissions(
	accountID, actionConnectorID string,
) (*ActionConnector, []ResourcePermission, error) {
	b.mu.RLock("DescribeActionConnectorPermissions")
	defer b.mu.RUnlock()

	a, ok := b.actionConnectors.Get(actionConnectorKey(accountID, actionConnectorID))
	if !ok {
		return nil, nil, ErrActionConnectorNotFound
	}

	return a.toActionConnector(), clonePermissions(a.Permissions), nil
}

func (b *InMemoryBackend) UpdateActionConnectorPermissions(
	accountID, actionConnectorID string,
	grant, revoke []ResourcePermission,
) (*ActionConnector, []ResourcePermission, error) {
	b.mu.Lock("UpdateActionConnectorPermissions")
	defer b.mu.Unlock()

	key := actionConnectorKey(accountID, actionConnectorID)
	a, ok := b.actionConnectors.Get(key)
	if !ok {
		return nil, nil, ErrActionConnectorNotFound
	}

	a.Permissions = applyGrantRevoke(a.Permissions, grant, revoke)
	a.LastUpdatedTime = time.Now().UTC()

	return a.toActionConnector(), clonePermissions(a.Permissions), nil
}
