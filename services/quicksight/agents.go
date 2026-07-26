package quicksight

import (
	"maps"
	"slices"
	"sort"
	"time"
)

const (
	agentLifecyclePreview   = "PREVIEW"
	agentLifecyclePublished = "PUBLISHED"
	agentStatusActive       = "ACTIVE"

	// filterAgentName is the SearchAgents filter attribute name for matching
	// on an agent's display name (the real API's AGENT_NAME filter).
	filterAgentName = "AGENT_NAME"
)

func agentKey(accountID, agentID string) string {
	return accountID + "/" + agentID
}

// storedAgent is the persisted representation of a QuickSight agent.
type storedAgent struct {
	CreatedAt        time.Time            `json:"createdAt"`
	UpdatedAt        time.Time            `json:"updatedAt"`
	AgentID          string               `json:"agentId"`
	Arn              string               `json:"arn"`
	Name             string               `json:"name"`
	Description      string               `json:"description,omitempty"`
	IconID           string               `json:"iconId,omitempty"`
	WelcomeMessage   string               `json:"welcomeMessage,omitempty"`
	Creator          string               `json:"creator,omitempty"`
	AgentLifecycle   string               `json:"agentLifecycle"`
	AgentStatus      string               `json:"agentStatus"`
	ErrorMessage     string               `json:"errorMessage,omitempty"`
	ActionConnectors []string             `json:"actionConnectors,omitempty"`
	Spaces           []string             `json:"spaces,omitempty"`
	StarterPrompts   []string             `json:"starterPrompts,omitempty"`
	Permissions      []ResourcePermission `json:"permissions,omitempty"`
}

func (a *storedAgent) toAgent() *Agent {
	return &Agent{
		CreatedAt:        a.CreatedAt,
		UpdatedAt:        a.UpdatedAt,
		AgentID:          a.AgentID,
		Arn:              a.Arn,
		Name:             a.Name,
		Description:      a.Description,
		IconID:           a.IconID,
		WelcomeMessage:   a.WelcomeMessage,
		Creator:          a.Creator,
		AgentLifecycle:   a.AgentLifecycle,
		AgentStatus:      a.AgentStatus,
		ErrorMessage:     a.ErrorMessage,
		ActionConnectors: slices.Clone(a.ActionConnectors),
		Spaces:           slices.Clone(a.Spaces),
		StarterPrompts:   slices.Clone(a.StarterPrompts),
		Permissions:      clonePermissions(a.Permissions),
	}
}

// ---- Agents ----

// CreateAgent creates a new agent. Real AWS stamps Creator from the caller's
// identity; this backend has no authenticated-caller concept, so Creator is
// left empty rather than fabricated. CustomPromptInput is intentionally not
// modeled: the real DescribeAgent/CreateAgent response field
// (CustomPromptInterface) requires ModelProfileId/QbsAwsAccountId/
// SubscriptionId, which are minted by a real Amazon Q Business subscription
// this backend has no state for -- synthesizing them would be fabrication,
// so the field is accepted on the request (see handler) but never echoed
// back, an intentional, documented omission (see PARITY.md).
func (b *InMemoryBackend) CreateAgent(
	accountID, agentID, name, description, iconID, welcomeMessage, agentLifecycle string,
	actionConnectors, spaces, starterPrompts []string,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Agent, error) {
	if agentID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAgent")
	defer b.mu.Unlock()

	key := agentKey(accountID, agentID)
	if b.agents.Has(key) {
		return nil, ErrAgentAlreadyExists
	}

	lifecycle := agentLifecycle
	if lifecycle == "" {
		lifecycle = agentLifecyclePreview
	}

	now := time.Now().UTC()
	arn := b.buildARN("agent", agentID)
	a := &storedAgent{
		CreatedAt:        now,
		UpdatedAt:        now,
		AgentID:          agentID,
		Arn:              arn,
		Name:             name,
		Description:      description,
		IconID:           iconID,
		WelcomeMessage:   welcomeMessage,
		AgentLifecycle:   lifecycle,
		AgentStatus:      agentStatusActive,
		ActionConnectors: slices.Clone(actionConnectors),
		Spaces:           slices.Clone(spaces),
		StarterPrompts:   slices.Clone(starterPrompts),
		Permissions:      clonePermissions(permissions),
	}
	b.agents.Put(a)

	if len(tags) > 0 {
		b.tags[arn] = maps.Clone(tags)
	}

	return a.toAgent(), nil
}

func (b *InMemoryBackend) DescribeAgent(accountID, agentID string) (*Agent, error) {
	b.mu.RLock("DescribeAgent")
	defer b.mu.RUnlock()

	a, ok := b.agents.Get(agentKey(accountID, agentID))
	if !ok {
		return nil, ErrAgentNotFound
	}

	return a.toAgent(), nil
}

// UpdateAgent updates an agent's mutable fields and attaches/detaches action
// connectors and spaces. Per-ARN attach/detach failures are real, derived
// checks (an ARN that doesn't identify a resource this backend holds fails
// with RESOURCE_NOT_FOUND) rather than fabricated always-succeeds behavior.
func (b *InMemoryBackend) UpdateAgent(
	accountID, agentID, name, description, iconID, welcomeMessage string,
	actionConnectorsToAdd, actionConnectorsToRemove, spacesToAdd, spacesToRemove, starterPrompts []string,
) (*Agent, *AgentAssociationUpdate, error) {
	b.mu.Lock("UpdateAgent")
	defer b.mu.Unlock()

	key := agentKey(accountID, agentID)
	a, ok := b.agents.Get(key)
	if !ok {
		return nil, nil, ErrAgentNotFound
	}

	applyAgentScalarUpdates(a, name, description, iconID, welcomeMessage, starterPrompts)

	result := &AgentAssociationUpdate{}
	a.ActionConnectors, result.FailedToAddActionConnectors, result.FailedToRemoveActionConnectors =
		b.updateAgentAssociations(a.ActionConnectors, actionConnectorsToAdd, actionConnectorsToRemove)
	a.Spaces, result.FailedToAddSpaces, result.FailedToRemoveSpaces =
		b.updateAgentAssociations(a.Spaces, spacesToAdd, spacesToRemove)

	a.UpdatedAt = time.Now().UTC()

	return a.toAgent(), result, nil
}

func applyAgentScalarUpdates(
	a *storedAgent,
	name, description, iconID, welcomeMessage string,
	starterPrompts []string,
) {
	if name != "" {
		a.Name = name
	}
	if description != "" {
		a.Description = description
	}
	if iconID != "" {
		a.IconID = iconID
	}
	if welcomeMessage != "" {
		a.WelcomeMessage = welcomeMessage
	}
	if starterPrompts != nil {
		a.StarterPrompts = slices.Clone(starterPrompts)
	}
}

// updateAgentAssociations applies add/remove ARN lists against current,
// validating each ARN against arnExists (caller must hold b.mu). ARNs that
// don't identify a live resource fail rather than being silently accepted.
func (b *InMemoryBackend) updateAgentAssociations(
	current, toAdd, toRemove []string,
) ([]string, []AssociationFailure, []AssociationFailure) {
	set := make(map[string]bool, len(current))
	for _, arn := range current {
		set[arn] = true
	}

	var failedAdd, failedRemove []AssociationFailure

	for _, arn := range toAdd {
		if !b.arnExists(arn) {
			failedAdd = append(failedAdd, AssociationFailure{
				Arn: arn, ErrorCode: errResourceNotFound, ErrorMessage: "resource not found",
			})

			continue
		}
		set[arn] = true
	}

	for _, arn := range toRemove {
		if !set[arn] {
			failedRemove = append(failedRemove, AssociationFailure{
				Arn: arn, ErrorCode: errResourceNotFound, ErrorMessage: "association not found",
			})

			continue
		}
		delete(set, arn)
	}

	updated := make([]string, 0, len(set))
	for arn := range set {
		updated = append(updated, arn)
	}
	sort.Strings(updated)

	return updated, failedAdd, failedRemove
}

func (b *InMemoryBackend) DeleteAgent(accountID, agentID string) error {
	b.mu.Lock("DeleteAgent")
	defer b.mu.Unlock()

	key := agentKey(accountID, agentID)
	a, ok := b.agents.Get(key)
	if !ok {
		return ErrAgentNotFound
	}

	delete(b.tags, a.Arn)
	b.agents.Delete(key)

	return nil
}

func (b *InMemoryBackend) ListAgents(_ string, maxResults int32, nextToken string) ([]*Agent, string, error) {
	b.mu.RLock("ListAgents")
	defer b.mu.RUnlock()

	all := b.agents.All()
	sort.Slice(all, func(i, j int) bool { return all[i].AgentID < all[j].AgentID })

	result, next := paginateAgents(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchAgents(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Agent, string, error) {
	b.mu.RLock("SearchAgents")
	defer b.mu.RUnlock()

	var filtered []*storedAgent
	for _, a := range b.agents.All() {
		if matchesAllNameFilters(a.Name, filters, filterAgentName) {
			filtered = append(filtered, a)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].AgentID < filtered[j].AgentID })

	result, next := paginateAgents(filtered, maxResults, nextToken)

	return result, next, nil
}

func paginateAgents(all []*storedAgent, maxResults int32, nextToken string) ([]*Agent, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range all {
			if a.AgentID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].AgentID
	} else {
		end = len(all)
	}

	result := make([]*Agent, 0, end-start)
	for _, a := range all[start:end] {
		result = append(result, a.toAgent())
	}

	return result, next
}

// ---- Agent permissions ----

func (b *InMemoryBackend) DescribeAgentPermissions(accountID, agentID string) (*Agent, []ResourcePermission, error) {
	b.mu.RLock("DescribeAgentPermissions")
	defer b.mu.RUnlock()

	a, ok := b.agents.Get(agentKey(accountID, agentID))
	if !ok {
		return nil, nil, ErrAgentNotFound
	}

	return a.toAgent(), clonePermissions(a.Permissions), nil
}

func (b *InMemoryBackend) UpdateAgentPermissions(
	accountID, agentID string,
	grant, revoke []ResourcePermission,
) (*Agent, []ResourcePermission, error) {
	b.mu.Lock("UpdateAgentPermissions")
	defer b.mu.Unlock()

	key := agentKey(accountID, agentID)
	a, ok := b.agents.Get(key)
	if !ok {
		return nil, nil, ErrAgentNotFound
	}

	a.Permissions = applyGrantRevoke(a.Permissions, grant, revoke)
	a.UpdatedAt = time.Now().UTC()

	return a.toAgent(), clonePermissions(a.Permissions), nil
}
