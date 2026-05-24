package bedrock

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// New models — Flows, Prompts, AgentVersions, AgentCollaborators,
// KnowledgeBaseDocuments
// ---------------------------------------------------------------------------

// Flow represents an Amazon Bedrock Flow.
type Flow struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	FlowID      string            `json:"flowId"`
	FlowArn     string            `json:"flowArn"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
}

// FlowAlias represents an alias for a Bedrock Flow.
type FlowAlias struct {
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	FlowAliasID  string    `json:"flowAliasId"`
	FlowAliasArn string    `json:"flowAliasArn"`
	FlowID       string    `json:"flowId"`
	Name         string    `json:"name"`
	Description  string    `json:"description,omitempty"`
}

// FlowVersion represents a snapshot version of a Flow.
type FlowVersion struct {
	CreatedAt time.Time `json:"createdAt"`
	FlowID    string    `json:"flowId"`
	Version   string    `json:"version"`
	Status    string    `json:"status"`
}

// Prompt represents an Amazon Bedrock Prompt.
type Prompt struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	PromptID    string            `json:"promptId"`
	PromptArn   string            `json:"promptArn"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
}

// PromptVersion represents a numbered version of a Prompt.
type PromptVersion struct {
	CreatedAt time.Time `json:"createdAt"`
	PromptID  string    `json:"promptId"`
	Version   string    `json:"version"`
	Name      string    `json:"name,omitempty"`
}

// AgentVersion represents a numbered version of an Agent.
type AgentVersion struct {
	CreatedAt    time.Time `json:"createdAt"`
	AgentID      string    `json:"agentId"`
	AgentVersion string    `json:"agentVersion"`
	AgentStatus  string    `json:"agentStatus"`
}

// AgentCollaborator represents an agent collaboration association.
type AgentCollaborator struct {
	CreatedAt         time.Time `json:"createdAt"`
	CollaboratorID    string    `json:"collaboratorId"`
	AgentID           string    `json:"agentId"`
	AgentVersion      string    `json:"agentVersion"`
	CollaboratorArn   string    `json:"collaboratorArn"`
	RelayConversation string    `json:"relayConversationHistory"`
}

// KnowledgeBaseDocument represents a document in a KB data source.
type KnowledgeBaseDocument struct {
	KnowledgeBaseID string `json:"knowledgeBaseId"`
	DataSourceID    string `json:"dataSourceId"`
	DocumentID      string `json:"documentId"`
	Status          string `json:"status"`
}

// ---------------------------------------------------------------------------
// Status constants for new types
// ---------------------------------------------------------------------------

const (
	flowStatusNotPrepared = "NOT_PREPARED"
	flowStatusPrepared    = "PREPARED"
	docStatusActive       = "ACTIVE"
	jobStatusStopped      = "STOPPED"
)

// ---------------------------------------------------------------------------
// Map key helpers for new types
// ---------------------------------------------------------------------------

func flowAliasKey(flowID, aliasID string) string { return flowID + "/" + aliasID }
func kbDocKey(kbID, dsID, docID string) string   { return kbID + "/" + dsID + "/" + docID }

// ---------------------------------------------------------------------------
// Flows
// ---------------------------------------------------------------------------

// CreateFlow creates a new Bedrock Flow.
func (b *InMemoryBackend) CreateFlow(
	name, description string,
	tags map[string]string,
) (*Flow, error) {
	b.mu.Lock("CreateFlow")
	defer b.mu.Unlock()

	if _, ok := b.flowsByName[name]; ok {
		return nil, fmt.Errorf("%w: flow %q already exists", ErrAlreadyExists, name)
	}

	b.flowCounter++
	id := fmt.Sprintf("flow-%08d", b.flowCounter)
	flowArn := arn.Build("bedrock-agent", b.region, b.accountID, "flow/"+id)
	now := time.Now()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	f := &Flow{
		CreatedAt:   now,
		UpdatedAt:   now,
		FlowID:      id,
		FlowArn:     flowArn,
		Name:        name,
		Description: description,
		Status:      flowStatusNotPrepared,
		Tags:        tagsCopy,
	}
	b.flows[id] = f
	b.flowsByName[name] = id
	cp := *f

	return &cp, nil
}

// GetFlow returns a Flow by ID.
func (b *InMemoryBackend) GetFlow(flowID string) (*Flow, error) {
	b.mu.RLock("GetFlow")
	defer b.mu.RUnlock()

	f, ok := b.flows[flowID]
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	cp := *f

	return &cp, nil
}

// ListFlows returns all flows with pagination.
func (b *InMemoryBackend) ListFlows(maxResults int, nextToken string) ([]*Flow, string) {
	b.mu.RLock("ListFlows")
	defer b.mu.RUnlock()

	list := make([]*Flow, 0, len(b.flows))
	for _, f := range b.flows {
		cp := *f
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, maxResults, nextToken)
}

// UpdateFlow updates a Flow.
func (b *InMemoryBackend) UpdateFlow(flowID, name, description string) (*Flow, error) {
	b.mu.Lock("UpdateFlow")
	defer b.mu.Unlock()

	f, ok := b.flows[flowID]
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	if name != "" && name != f.Name {
		delete(b.flowsByName, f.Name)
		f.Name = name
		b.flowsByName[name] = flowID
	}

	if description != "" {
		f.Description = description
	}

	f.UpdatedAt = time.Now()
	cp := *f

	return &cp, nil
}

// DeleteFlow deletes a Flow.
func (b *InMemoryBackend) DeleteFlow(flowID string) error {
	b.mu.Lock("DeleteFlow")
	defer b.mu.Unlock()

	f, ok := b.flows[flowID]
	if !ok {
		return fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	delete(b.flowsByName, f.Name)
	delete(b.flows, flowID)

	return nil
}

// PrepareFlow transitions a Flow to PREPARED status.
func (b *InMemoryBackend) PrepareFlow(flowID string) (*Flow, error) {
	b.mu.Lock("PrepareFlow")
	defer b.mu.Unlock()

	f, ok := b.flows[flowID]
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	f.Status = flowStatusPrepared
	f.UpdatedAt = time.Now()
	cp := *f

	return &cp, nil
}

// ---------------------------------------------------------------------------
// Flow Aliases
// ---------------------------------------------------------------------------

// CreateFlowAlias creates an alias for a Flow.
func (b *InMemoryBackend) CreateFlowAlias(
	flowID, name, description string,
) (*FlowAlias, error) {
	b.mu.Lock("CreateFlowAlias")
	defer b.mu.Unlock()

	if _, ok := b.flows[flowID]; !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	b.flowAliasCounter++
	aliasID := fmt.Sprintf("flowAlias-%08d", b.flowAliasCounter)
	aliasArn := arn.Build(
		"bedrock-agent",
		b.region,
		b.accountID,
		"flow/"+flowID+"/alias/"+aliasID,
	)
	now := time.Now()

	fa := &FlowAlias{
		CreatedAt:    now,
		UpdatedAt:    now,
		FlowAliasID:  aliasID,
		FlowAliasArn: aliasArn,
		FlowID:       flowID,
		Name:         name,
		Description:  description,
	}
	b.flowAliases[flowAliasKey(flowID, aliasID)] = fa
	cp := *fa

	return &cp, nil
}

// GetFlowAlias returns a Flow alias by ID.
func (b *InMemoryBackend) GetFlowAlias(flowID, aliasID string) (*FlowAlias, error) {
	b.mu.RLock("GetFlowAlias")
	defer b.mu.RUnlock()

	fa, ok := b.flowAliases[flowAliasKey(flowID, aliasID)]
	if !ok {
		return nil, fmt.Errorf("%w: flow alias %q not found", ErrNotFound, aliasID)
	}

	cp := *fa

	return &cp, nil
}

// ListFlowAliases lists aliases for a Flow.
func (b *InMemoryBackend) ListFlowAliases(
	flowID string,
	maxResults int,
	nextToken string,
) ([]*FlowAlias, string) {
	b.mu.RLock("ListFlowAliases")
	defer b.mu.RUnlock()

	list := make([]*FlowAlias, 0)
	prefix := flowID + "/"

	for k, fa := range b.flowAliases {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *fa
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, maxResults, nextToken)
}

// UpdateFlowAlias updates a Flow alias.
func (b *InMemoryBackend) UpdateFlowAlias(
	flowID, aliasID, name, description string,
) (*FlowAlias, error) {
	b.mu.Lock("UpdateFlowAlias")
	defer b.mu.Unlock()

	fa, ok := b.flowAliases[flowAliasKey(flowID, aliasID)]
	if !ok {
		return nil, fmt.Errorf("%w: flow alias %q not found", ErrNotFound, aliasID)
	}

	if name != "" {
		fa.Name = name
	}

	if description != "" {
		fa.Description = description
	}

	fa.UpdatedAt = time.Now()
	cp := *fa

	return &cp, nil
}

// DeleteFlowAlias deletes a Flow alias.
func (b *InMemoryBackend) DeleteFlowAlias(flowID, aliasID string) error {
	b.mu.Lock("DeleteFlowAlias")
	defer b.mu.Unlock()

	key := flowAliasKey(flowID, aliasID)

	if _, ok := b.flowAliases[key]; !ok {
		return fmt.Errorf("%w: flow alias %q not found", ErrNotFound, aliasID)
	}

	delete(b.flowAliases, key)

	return nil
}

// ---------------------------------------------------------------------------
// Flow Versions
// ---------------------------------------------------------------------------

// CreateFlowVersion creates a numbered snapshot version of a Flow.
func (b *InMemoryBackend) CreateFlowVersion(flowID string) (*FlowVersion, error) {
	b.mu.Lock("CreateFlowVersion")
	defer b.mu.Unlock()

	if _, ok := b.flows[flowID]; !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	b.flowVersionCounters[flowID]++
	ver := strconv.Itoa(b.flowVersionCounters[flowID])

	fv := &FlowVersion{
		CreatedAt: time.Now(),
		FlowID:    flowID,
		Version:   ver,
		Status:    flowStatusPrepared,
	}

	if b.flowVersions[flowID] == nil {
		b.flowVersions[flowID] = make(map[string]*FlowVersion)
	}

	b.flowVersions[flowID][ver] = fv
	cp := *fv

	return &cp, nil
}

// GetFlowVersion returns a specific Flow version.
func (b *InMemoryBackend) GetFlowVersion(flowID, version string) (*FlowVersion, error) {
	b.mu.RLock("GetFlowVersion")
	defer b.mu.RUnlock()

	versions, versionsOK := b.flowVersions[flowID]
	if !versionsOK {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	fv, verOK := versions[version]
	if !verOK {
		return nil, fmt.Errorf(
			"%w: flow version %q not found for flow %q",
			ErrNotFound,
			version,
			flowID,
		)
	}

	cp := *fv

	return &cp, nil
}

// ListFlowVersions lists all versions for a Flow.
func (b *InMemoryBackend) ListFlowVersions(
	flowID string,
	maxResults int,
	nextToken string,
) ([]*FlowVersion, string) {
	b.mu.RLock("ListFlowVersions")
	defer b.mu.RUnlock()

	versions := b.flowVersions[flowID]
	list := make([]*FlowVersion, 0, len(versions))

	for _, fv := range versions {
		cp := *fv
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Version < list[j].Version })

	return paginate(list, maxResults, nextToken)
}

// DeleteFlowVersion deletes a specific Flow version.
func (b *InMemoryBackend) DeleteFlowVersion(flowID, version string) error {
	b.mu.Lock("DeleteFlowVersion")
	defer b.mu.Unlock()

	versions, versionsOK := b.flowVersions[flowID]
	if !versionsOK {
		return fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	if _, verOK := versions[version]; !verOK {
		return fmt.Errorf(
			"%w: flow version %q not found for flow %q",
			ErrNotFound,
			version,
			flowID,
		)
	}

	delete(versions, version)

	return nil
}

// ValidateFlowDefinition validates a flow definition (stub — always succeeds).
func (b *InMemoryBackend) ValidateFlowDefinition() ([]any, error) {
	return []any{}, nil
}

// ---------------------------------------------------------------------------
// Prompts
// ---------------------------------------------------------------------------

// CreatePrompt creates a new Bedrock Prompt.
func (b *InMemoryBackend) CreatePrompt(
	name, description string,
	tags map[string]string,
) (*Prompt, error) {
	b.mu.Lock("CreatePrompt")
	defer b.mu.Unlock()

	if _, ok := b.promptsByName[name]; ok {
		return nil, fmt.Errorf("%w: prompt %q already exists", ErrAlreadyExists, name)
	}

	b.promptCounter++
	id := fmt.Sprintf("prompt-%08d", b.promptCounter)
	promptArn := arn.Build("bedrock-agent", b.region, b.accountID, "prompt/"+id)
	now := time.Now()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	p := &Prompt{
		CreatedAt:   now,
		UpdatedAt:   now,
		PromptID:    id,
		PromptArn:   promptArn,
		Name:        name,
		Description: description,
		Tags:        tagsCopy,
	}
	b.prompts[id] = p
	b.promptsByName[name] = id
	cp := *p

	return &cp, nil
}

// GetPrompt returns a Prompt by ID.
func (b *InMemoryBackend) GetPrompt(promptID string) (*Prompt, error) {
	b.mu.RLock("GetPrompt")
	defer b.mu.RUnlock()

	p, ok := b.prompts[promptID]
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	cp := *p

	return &cp, nil
}

// ListPrompts returns all prompts with pagination.
func (b *InMemoryBackend) ListPrompts(maxResults int, nextToken string) ([]*Prompt, string) {
	b.mu.RLock("ListPrompts")
	defer b.mu.RUnlock()

	list := make([]*Prompt, 0, len(b.prompts))
	for _, p := range b.prompts {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, maxResults, nextToken)
}

// UpdatePrompt updates a Prompt.
func (b *InMemoryBackend) UpdatePrompt(promptID, name, description string) (*Prompt, error) {
	b.mu.Lock("UpdatePrompt")
	defer b.mu.Unlock()

	p, ok := b.prompts[promptID]
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if name != "" && name != p.Name {
		delete(b.promptsByName, p.Name)
		p.Name = name
		b.promptsByName[name] = promptID
	}

	if description != "" {
		p.Description = description
	}

	p.UpdatedAt = time.Now()
	cp := *p

	return &cp, nil
}

// DeletePrompt deletes a Prompt.
func (b *InMemoryBackend) DeletePrompt(promptID string) error {
	b.mu.Lock("DeletePrompt")
	defer b.mu.Unlock()

	p, ok := b.prompts[promptID]
	if !ok {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	delete(b.promptsByName, p.Name)
	delete(b.prompts, promptID)

	return nil
}

// ---------------------------------------------------------------------------
// Prompt Versions
// ---------------------------------------------------------------------------

// CreatePromptVersion creates a numbered snapshot version of a Prompt.
func (b *InMemoryBackend) CreatePromptVersion(promptID string) (*PromptVersion, error) {
	b.mu.Lock("CreatePromptVersion")
	defer b.mu.Unlock()

	p, ok := b.prompts[promptID]
	if !ok {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	b.promptVersionCounters[promptID]++
	ver := strconv.Itoa(b.promptVersionCounters[promptID])

	pv := &PromptVersion{
		CreatedAt: time.Now(),
		PromptID:  promptID,
		Version:   ver,
		Name:      p.Name,
	}

	if b.promptVersions[promptID] == nil {
		b.promptVersions[promptID] = make(map[string]*PromptVersion)
	}

	b.promptVersions[promptID][ver] = pv
	cp := *pv

	return &cp, nil
}

// GetPromptVersion returns a specific Prompt version.
func (b *InMemoryBackend) GetPromptVersion(promptID, version string) (*PromptVersion, error) {
	b.mu.RLock("GetPromptVersion")
	defer b.mu.RUnlock()

	versions, versionsOK := b.promptVersions[promptID]
	if !versionsOK {
		return nil, fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	pv, verOK := versions[version]
	if !verOK {
		return nil, fmt.Errorf(
			"%w: prompt version %q not found for prompt %q",
			ErrNotFound,
			version,
			promptID,
		)
	}

	cp := *pv

	return &cp, nil
}

// ListPromptVersions lists all versions for a Prompt.
func (b *InMemoryBackend) ListPromptVersions(
	promptID string,
	maxResults int,
	nextToken string,
) ([]*PromptVersion, string) {
	b.mu.RLock("ListPromptVersions")
	defer b.mu.RUnlock()

	versions := b.promptVersions[promptID]
	list := make([]*PromptVersion, 0, len(versions))

	for _, pv := range versions {
		cp := *pv
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Version < list[j].Version })

	return paginate(list, maxResults, nextToken)
}

// DeletePromptVersion deletes a specific Prompt version.
func (b *InMemoryBackend) DeletePromptVersion(promptID, version string) error {
	b.mu.Lock("DeletePromptVersion")
	defer b.mu.Unlock()

	versions, versionsOK := b.promptVersions[promptID]
	if !versionsOK {
		return fmt.Errorf("%w: prompt %q not found", ErrNotFound, promptID)
	}

	if _, verOK := versions[version]; !verOK {
		return fmt.Errorf(
			"%w: prompt version %q not found for prompt %q",
			ErrNotFound,
			version,
			promptID,
		)
	}

	delete(versions, version)

	return nil
}

// ---------------------------------------------------------------------------
// Agent Versions
// ---------------------------------------------------------------------------

// CreateAgentVersion creates a numbered version snapshot of an Agent.
func (b *InMemoryBackend) CreateAgentVersion(agentID string) (*AgentVersion, error) {
	b.mu.Lock("CreateAgentVersion")
	defer b.mu.Unlock()

	ag, ok := b.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.agentVersionCounters[agentID]++
	ver := strconv.Itoa(b.agentVersionCounters[agentID])

	av := &AgentVersion{
		CreatedAt:    time.Now(),
		AgentID:      agentID,
		AgentVersion: ver,
		AgentStatus:  ag.AgentStatus,
	}

	if b.agentVersions[agentID] == nil {
		b.agentVersions[agentID] = make(map[string]*AgentVersion)
	}

	b.agentVersions[agentID][ver] = av
	cp := *av

	return &cp, nil
}

// GetAgentVersion returns a specific Agent version.
func (b *InMemoryBackend) GetAgentVersion(agentID, version string) (*AgentVersion, error) {
	b.mu.RLock("GetAgentVersion")
	defer b.mu.RUnlock()

	versions, versionsOK := b.agentVersions[agentID]
	if !versionsOK {
		return nil, fmt.Errorf("%w: agent %q has no versions", ErrNotFound, agentID)
	}

	av, verOK := versions[version]
	if !verOK {
		return nil, fmt.Errorf(
			"%w: agent version %q not found for agent %q",
			ErrNotFound,
			version,
			agentID,
		)
	}

	cp := *av

	return &cp, nil
}

// ListAgentVersions lists all versions for an Agent.
func (b *InMemoryBackend) ListAgentVersions(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentVersion, string) {
	b.mu.RLock("ListAgentVersions")
	defer b.mu.RUnlock()

	versions := b.agentVersions[agentID]
	list := make([]*AgentVersion, 0, len(versions))

	for _, av := range versions {
		cp := *av
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AgentVersion < list[j].AgentVersion })

	return paginate(list, maxResults, nextToken)
}

// DeleteAgentVersion deletes a specific Agent version.
func (b *InMemoryBackend) DeleteAgentVersion(agentID, version string) error {
	b.mu.Lock("DeleteAgentVersion")
	defer b.mu.Unlock()

	versions, versionsOK := b.agentVersions[agentID]
	if !versionsOK {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if _, verOK := versions[version]; !verOK {
		return fmt.Errorf(
			"%w: agent version %q not found for agent %q",
			ErrNotFound,
			version,
			agentID,
		)
	}

	delete(versions, version)

	return nil
}

// ---------------------------------------------------------------------------
// Agent Collaborators
// ---------------------------------------------------------------------------

// AssociateAgentCollaborator associates a collaborator agent with an agent.
func (b *InMemoryBackend) AssociateAgentCollaborator(
	agentID, agentVersion, collaboratorArn, relayConversation string,
) (*AgentCollaborator, error) {
	b.mu.Lock("AssociateAgentCollaborator")
	defer b.mu.Unlock()

	if _, ok := b.agents[agentID]; !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.agentCollabCounter++
	collabID := fmt.Sprintf("collab-%08d", b.agentCollabCounter)

	ac := &AgentCollaborator{
		CreatedAt:         time.Now(),
		CollaboratorID:    collabID,
		AgentID:           agentID,
		AgentVersion:      agentVersion,
		CollaboratorArn:   collaboratorArn,
		RelayConversation: relayConversation,
	}

	if b.agentCollaborators[agentID] == nil {
		b.agentCollaborators[agentID] = make(map[string]*AgentCollaborator)
	}

	b.agentCollaborators[agentID][collabID] = ac
	cp := *ac

	return &cp, nil
}

// GetAgentCollaborator returns an agent collaborator by ID.
func (b *InMemoryBackend) GetAgentCollaborator(
	agentID, collaboratorID string,
) (*AgentCollaborator, error) {
	b.mu.RLock("GetAgentCollaborator")
	defer b.mu.RUnlock()

	collabs := b.agentCollaborators[agentID]

	ac, ok := collabs[collaboratorID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: collaborator %q not found for agent %q",
			ErrNotFound,
			collaboratorID,
			agentID,
		)
	}

	cp := *ac

	return &cp, nil
}

// ListAgentCollaborators lists collaborators for an agent.
func (b *InMemoryBackend) ListAgentCollaborators(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentCollaborator, string) {
	b.mu.RLock("ListAgentCollaborators")
	defer b.mu.RUnlock()

	collabs := b.agentCollaborators[agentID]
	list := make([]*AgentCollaborator, 0, len(collabs))

	for _, ac := range collabs {
		cp := *ac
		list = append(list, &cp)
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].CollaboratorID < list[j].CollaboratorID },
	)

	return paginate(list, maxResults, nextToken)
}

// UpdateAgentCollaborator updates an agent collaborator.
func (b *InMemoryBackend) UpdateAgentCollaborator(
	agentID, collaboratorID, relayConversation string,
) (*AgentCollaborator, error) {
	b.mu.Lock("UpdateAgentCollaborator")
	defer b.mu.Unlock()

	collabs := b.agentCollaborators[agentID]

	ac, ok := collabs[collaboratorID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: collaborator %q not found for agent %q",
			ErrNotFound,
			collaboratorID,
			agentID,
		)
	}

	if relayConversation != "" {
		ac.RelayConversation = relayConversation
	}

	cp := *ac

	return &cp, nil
}

// DisassociateAgentCollaborator removes a collaborator from an agent.
func (b *InMemoryBackend) DisassociateAgentCollaborator(agentID, collaboratorID string) error {
	b.mu.Lock("DisassociateAgentCollaborator")
	defer b.mu.Unlock()

	collabs := b.agentCollaborators[agentID]

	if _, ok := collabs[collaboratorID]; !ok {
		return fmt.Errorf(
			"%w: collaborator %q not found for agent %q",
			ErrNotFound,
			collaboratorID,
			agentID,
		)
	}

	delete(collabs, collaboratorID)

	return nil
}

// ---------------------------------------------------------------------------
// Knowledge Base Documents
// ---------------------------------------------------------------------------

// IngestKnowledgeBaseDocuments adds documents to a KB data source.
func (b *InMemoryBackend) IngestKnowledgeBaseDocuments(
	kbID, dsID string,
	documentIDs []string,
) ([]*KnowledgeBaseDocument, error) {
	b.mu.Lock("IngestKnowledgeBaseDocuments")
	defer b.mu.Unlock()

	if _, ok := b.knowledgeBases[kbID]; !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if _, ok := b.dataSources[kbID+"/"+dsID]; !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	docs := make([]*KnowledgeBaseDocument, 0, len(documentIDs))

	for _, docID := range documentIDs {
		doc := &KnowledgeBaseDocument{
			KnowledgeBaseID: kbID,
			DataSourceID:    dsID,
			DocumentID:      docID,
			Status:          docStatusActive,
		}
		b.kbDocuments[kbDocKey(kbID, dsID, docID)] = doc
		cp := *doc
		docs = append(docs, &cp)
	}

	return docs, nil
}

// ListKnowledgeBaseDocuments returns documents for a KB data source.
func (b *InMemoryBackend) ListKnowledgeBaseDocuments(
	kbID, dsID string,
	maxResults int,
	nextToken string,
) ([]*KnowledgeBaseDocument, string) {
	b.mu.RLock("ListKnowledgeBaseDocuments")
	defer b.mu.RUnlock()

	prefix := kbID + "/" + dsID + "/"
	list := make([]*KnowledgeBaseDocument, 0)

	for k, doc := range b.kbDocuments {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *doc
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].DocumentID < list[j].DocumentID })

	return paginate(list, maxResults, nextToken)
}

// GetKnowledgeBaseDocuments returns selected documents for a KB data source.
func (b *InMemoryBackend) GetKnowledgeBaseDocuments(
	kbID, dsID string,
	documentIDs []string,
) ([]*KnowledgeBaseDocument, error) {
	b.mu.RLock("GetKnowledgeBaseDocuments")
	defer b.mu.RUnlock()

	if _, ok := b.dataSources[kbID+"/"+dsID]; !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	docs := make([]*KnowledgeBaseDocument, 0, len(documentIDs))
	for _, documentID := range documentIDs {
		doc, ok := b.kbDocuments[kbDocKey(kbID, dsID, documentID)]
		if !ok {
			continue
		}
		cp := *doc
		docs = append(docs, &cp)
	}

	return docs, nil
}

// DeleteKnowledgeBaseDocuments removes documents from a KB data source.
func (b *InMemoryBackend) DeleteKnowledgeBaseDocuments(
	kbID, dsID string,
	documentIDs []string,
) error {
	b.mu.Lock("DeleteKnowledgeBaseDocuments")
	defer b.mu.Unlock()

	for _, docID := range documentIDs {
		delete(b.kbDocuments, kbDocKey(kbID, dsID, docID))
	}

	return nil
}

// UpdateKnowledgeBaseDocuments upserts document records in a KB data source.
func (b *InMemoryBackend) UpdateKnowledgeBaseDocuments(
	kbID, dsID string,
	documentIDs []string,
) ([]*KnowledgeBaseDocument, error) {
	b.mu.Lock("UpdateKnowledgeBaseDocuments")
	defer b.mu.Unlock()

	docs := make([]*KnowledgeBaseDocument, 0, len(documentIDs))

	for _, docID := range documentIDs {
		key := kbDocKey(kbID, dsID, docID)
		doc, ok := b.kbDocuments[key]

		if !ok {
			doc = &KnowledgeBaseDocument{
				KnowledgeBaseID: kbID,
				DataSourceID:    dsID,
				DocumentID:      docID,
				Status:          docStatusActive,
			}
			b.kbDocuments[key] = doc
		}

		cp := *doc
		docs = append(docs, &cp)
	}

	return docs, nil
}

// ---------------------------------------------------------------------------
// Ingestion job management extensions
// ---------------------------------------------------------------------------

// StopIngestionJob stops a running ingestion job.
func (b *InMemoryBackend) StopIngestionJob(kbID, dsID, jobID string) (*IngestionJob, error) {
	b.mu.Lock("StopIngestionJob")
	defer b.mu.Unlock()

	job, ok := b.ingestionJobs[ingestionJobKey(kbID, dsID, jobID)]
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	job.Status = jobStatusStopped
	job.UpdatedAt = time.Now()
	cp := *job

	return &cp, nil
}

// ---------------------------------------------------------------------------
// Resource Tags (agent-domain, map[string]string)
// ---------------------------------------------------------------------------

// TagAgentResource adds tags to an agent-domain resource identified by its ARN.
func (b *InMemoryBackend) TagAgentResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagAgentResource")
	defer b.mu.Unlock()

	if b.agentTags[resourceArn] == nil {
		b.agentTags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.agentTags[resourceArn], tags)

	return nil
}

// UntagAgentResource removes tags from an agent-domain resource identified by its ARN.
func (b *InMemoryBackend) UntagAgentResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagAgentResource")
	defer b.mu.Unlock()

	existing := b.agentTags[resourceArn]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListAgentResourceTags returns tags for an agent-domain resource identified by its ARN.
func (b *InMemoryBackend) ListAgentResourceTags(resourceArn string) map[string]string {
	b.mu.RLock("ListAgentResourceTags")
	defer b.mu.RUnlock()

	existing := b.agentTags[resourceArn]
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result
}

// ---------------------------------------------------------------------------
// Agent Memory
// ---------------------------------------------------------------------------

// GetAgentMemory returns memory entries for an agent session (stub).
func (b *InMemoryBackend) GetAgentMemory(agentID, sessionID string) []any {
	b.mu.RLock("GetAgentMemory")
	defer b.mu.RUnlock()

	key := agentID + "/" + sessionID
	entries := b.agentMemory[key]

	result := make([]any, len(entries))
	copy(result, entries)

	return result
}

// DeleteAgentMemory deletes memory entries for an agent session.
func (b *InMemoryBackend) DeleteAgentMemory(agentID, sessionID string) error {
	b.mu.Lock("DeleteAgentMemory")
	defer b.mu.Unlock()

	if _, ok := b.agents[agentID]; !ok {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	delete(b.agentMemory, agentID+"/"+sessionID)

	return nil
}
