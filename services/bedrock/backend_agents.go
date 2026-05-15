package bedrock

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Agent models
// ---------------------------------------------------------------------------

// Agent represents an Amazon Bedrock Agent.
type Agent struct {
	CreatedAt       time.Time         `json:"createdAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
	Tags            map[string]string `json:"tags,omitempty"`
	AgentID         string            `json:"agentId"`
	AgentArn        string            `json:"agentArn"`
	AgentName       string            `json:"agentName"`
	AgentStatus     string            `json:"agentStatus"`
	AgentVersion    string            `json:"agentVersion"`
	FoundationModel string            `json:"foundationModel,omitempty"`
	Instruction     string            `json:"instruction,omitempty"`
	RoleArn         string            `json:"agentResourceRoleArn,omitempty"`
}

// AgentActionGroup represents an action group for a Bedrock Agent.
type AgentActionGroup struct {
	CreatedAt           time.Time      `json:"createdAt"`
	UpdatedAt           time.Time      `json:"updatedAt"`
	ActionGroupExecutor map[string]any `json:"actionGroupExecutor,omitempty"`
	ActionGroupID       string         `json:"actionGroupId"`
	ActionGroupName     string         `json:"actionGroupName"`
	AgentID             string         `json:"agentId"`
	AgentVersion        string         `json:"agentVersion"`
	ActionGroupState    string         `json:"actionGroupState"`
	Description         string         `json:"description,omitempty"`
}

// AgentAlias represents an alias for a Bedrock Agent.
type AgentAlias struct {
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	AgentAliasID   string    `json:"agentAliasId"`
	AgentAliasArn  string    `json:"agentAliasArn"`
	AgentAliasName string    `json:"agentAliasName"`
	AgentID        string    `json:"agentId"`
	AgentVersion   string    `json:"routingConfiguration,omitempty"`
	AliasStatus    string    `json:"agentAliasStatus"`
}

// AgentKnowledgeBaseAssociation represents an association between agent and knowledge base.
type AgentKnowledgeBaseAssociation struct {
	AgentID         string `json:"agentId"`
	AgentVersion    string `json:"agentVersion"`
	KnowledgeBaseID string `json:"knowledgeBaseId"`
	Description     string `json:"description,omitempty"`
	KBState         string `json:"knowledgeBaseState"`
}

// KnowledgeBase represents an Amazon Bedrock Knowledge Base.
type KnowledgeBase struct {
	CreatedAt                  time.Time         `json:"createdAt"`
	UpdatedAt                  time.Time         `json:"updatedAt"`
	KnowledgeBaseConfiguration map[string]any    `json:"knowledgeBaseConfiguration,omitempty"`
	StorageConfiguration       map[string]any    `json:"storageConfiguration,omitempty"`
	Tags                       map[string]string `json:"tags,omitempty"`
	KnowledgeBaseID            string            `json:"knowledgeBaseId"`
	KnowledgeBaseArn           string            `json:"knowledgeBaseArn"`
	Name                       string            `json:"name"`
	Description                string            `json:"description,omitempty"`
	Status                     string            `json:"status"`
	RoleArn                    string            `json:"roleArn,omitempty"`
}

// DataSource represents a data source for a Knowledge Base.
type DataSource struct {
	CreatedAt               time.Time      `json:"createdAt"`
	UpdatedAt               time.Time      `json:"updatedAt"`
	DataSourceConfiguration map[string]any `json:"dataSourceConfiguration,omitempty"`
	DataSourceID            string         `json:"dataSourceId"`
	DataSourceStatus        string         `json:"dataSourceStatus"`
	KnowledgeBaseID         string         `json:"knowledgeBaseId"`
	Name                    string         `json:"name"`
	Description             string         `json:"description,omitempty"`
}

// IngestionJob represents a data source ingestion job.
type IngestionJob struct {
	StartedAt       time.Time `json:"startedAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	IngestionJobID  string    `json:"ingestionJobId"`
	KnowledgeBaseID string    `json:"knowledgeBaseId"`
	DataSourceID    string    `json:"dataSourceId"`
	Status          string    `json:"status"`
	Description     string    `json:"description,omitempty"`
}

// ---------------------------------------------------------------------------
// Status and timing constants
// ---------------------------------------------------------------------------

const (
	agentTransitionDelay   = 100 * time.Millisecond // delay for agent CREATING → READY transition
	ingestionCompleteDelay = 200 * time.Millisecond // delay for ingestion job STARTING → COMPLETE
)

const (
	agentStatusDraft    = "DRAFT"
	agentStatusReady    = "READY"
	agentStatusPrepared = "PREPARED"
	agentStatusCreating = "CREATING"
	kbStatusActive      = "ACTIVE"
	dsStatusAvailable   = "AVAILABLE"
	aliasStatusPrepared = "PREPARED"
	actionGroupEnabled  = "ENABLED"
	jobStatusStarting   = "STARTING"
	jobStatusComplete   = "COMPLETE"
)

// ---------------------------------------------------------------------------
// Map key helpers
// ---------------------------------------------------------------------------

func agentActionGroupKey(
	agentID, actionGroupID string,
) string {
	return agentID + "/" + actionGroupID
}
func agentAliasKey(agentID, aliasID string) string { return agentID + "/" + aliasID }
func agentKBKey(agentID, kbID string) string       { return agentID + "/" + kbID }

func ingestionJobKey(
	kbID, dsID, jobID string,
) string {
	return kbID + "/" + dsID + "/" + jobID
}

// ---------------------------------------------------------------------------
// Agent CRUD
// ---------------------------------------------------------------------------

// CreateAgent creates a new Bedrock Agent.
func (b *InMemoryBackend) CreateAgent(
	agentName, foundationModel, instruction, roleArn string,
	tags map[string]string,
) (*Agent, error) {
	b.mu.Lock("CreateAgent")
	defer b.mu.Unlock()

	if _, ok := b.agentsByName[agentName]; ok {
		return nil, fmt.Errorf("%w: agent %q already exists", ErrAlreadyExists, agentName)
	}

	b.agentCounter++
	id := fmt.Sprintf("agent-%08d", b.agentCounter)
	agentArn := arn.Build("bedrock", b.region, b.accountID, "agent/"+id)
	now := time.Now()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	ag := &Agent{
		CreatedAt:       now,
		UpdatedAt:       now,
		AgentID:         id,
		AgentArn:        agentArn,
		AgentName:       agentName,
		AgentStatus:     agentStatusCreating,
		AgentVersion:    agentStatusDraft,
		FoundationModel: foundationModel,
		Instruction:     instruction,
		RoleArn:         roleArn,
		Tags:            tagsCopy,
	}
	b.agents[id] = ag
	b.agentsByName[agentName] = id

	// Transition to READY after a short delay.
	var once sync.Once
	go func() {
		once.Do(func() {
			time.Sleep(agentTransitionDelay)
			b.mu.Lock("CreateAgent.goroutine")
			defer b.mu.Unlock()
			if a, exists := b.agents[id]; exists {
				a.AgentStatus = agentStatusReady
			}
		})
	}()

	cp := *ag

	return &cp, nil
}

// GetAgent returns a Bedrock Agent by ID.
func (b *InMemoryBackend) GetAgent(agentID string) (*Agent, error) {
	b.mu.RLock("GetAgent")
	defer b.mu.RUnlock()

	ag, ok := b.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	cp := *ag

	return &cp, nil
}

// ListAgents returns all agents with pagination.
func (b *InMemoryBackend) ListAgents(maxResults int, nextToken string) ([]*Agent, string) {
	b.mu.RLock("ListAgents")
	defer b.mu.RUnlock()

	list := make([]*Agent, 0, len(b.agents))
	for _, ag := range b.agents {
		cp := *ag
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AgentName < list[j].AgentName })

	return paginate(list, maxResults, nextToken)
}

// UpdateAgent updates a Bedrock Agent.
func (b *InMemoryBackend) UpdateAgent(
	agentID, foundationModel, instruction, roleArn string,
) (*Agent, error) {
	b.mu.Lock("UpdateAgent")
	defer b.mu.Unlock()

	ag, ok := b.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if foundationModel != "" {
		ag.FoundationModel = foundationModel
	}

	if instruction != "" {
		ag.Instruction = instruction
	}

	if roleArn != "" {
		ag.RoleArn = roleArn
	}

	ag.UpdatedAt = time.Now()
	cp := *ag

	return &cp, nil
}

// DeleteAgent deletes a Bedrock Agent.
func (b *InMemoryBackend) DeleteAgent(agentID string) error {
	b.mu.Lock("DeleteAgent")
	defer b.mu.Unlock()

	ag, ok := b.agents[agentID]
	if !ok {
		return fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	delete(b.agentsByName, ag.AgentName)
	delete(b.agents, agentID)

	return nil
}

// PrepareAgent transitions an agent to PREPARED status.
func (b *InMemoryBackend) PrepareAgent(agentID string) (*Agent, error) {
	b.mu.Lock("PrepareAgent")
	defer b.mu.Unlock()

	ag, ok := b.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	ag.AgentStatus = agentStatusPrepared
	ag.UpdatedAt = time.Now()
	cp := *ag

	return &cp, nil
}

// ---------------------------------------------------------------------------
// Agent Action Groups
// ---------------------------------------------------------------------------

// CreateAgentActionGroup creates an action group for an agent.
func (b *InMemoryBackend) CreateAgentActionGroup(
	agentID, actionGroupName, description string,
	executor map[string]any,
) (*AgentActionGroup, error) {
	b.mu.Lock("CreateAgentActionGroup")
	defer b.mu.Unlock()

	if _, ok := b.agents[agentID]; !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.actionGroupCounter++
	id := fmt.Sprintf("ag-%08d", b.actionGroupCounter)
	now := time.Now()

	ag := &AgentActionGroup{
		CreatedAt:           now,
		UpdatedAt:           now,
		ActionGroupID:       id,
		ActionGroupName:     actionGroupName,
		AgentID:             agentID,
		AgentVersion:        agentStatusDraft,
		ActionGroupState:    actionGroupEnabled,
		Description:         description,
		ActionGroupExecutor: executor,
	}
	b.agentActionGroups[agentActionGroupKey(agentID, id)] = ag
	cp := *ag

	return &cp, nil
}

// GetAgentActionGroup returns an action group by ID.
func (b *InMemoryBackend) GetAgentActionGroup(
	agentID, actionGroupID string,
) (*AgentActionGroup, error) {
	b.mu.RLock("GetAgentActionGroup")
	defer b.mu.RUnlock()

	ag, ok := b.agentActionGroups[agentActionGroupKey(agentID, actionGroupID)]
	if !ok {
		return nil, fmt.Errorf(
			"%w: action group %q not found for agent %q",
			ErrNotFound,
			actionGroupID,
			agentID,
		)
	}

	cp := *ag

	return &cp, nil
}

// ListAgentActionGroups lists action groups for an agent.
func (b *InMemoryBackend) ListAgentActionGroups(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentActionGroup, string) {
	b.mu.RLock("ListAgentActionGroups")
	defer b.mu.RUnlock()

	list := make([]*AgentActionGroup, 0, len(b.agentActionGroups))
	prefix := agentID + "/"

	for k, ag := range b.agentActionGroups {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *ag
			list = append(list, &cp)
		}
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].ActionGroupName < list[j].ActionGroupName },
	)

	return paginate(list, maxResults, nextToken)
}

// UpdateAgentActionGroup updates an action group.
func (b *InMemoryBackend) UpdateAgentActionGroup(
	agentID, actionGroupID, description string,
	executor map[string]any,
) (*AgentActionGroup, error) {
	b.mu.Lock("UpdateAgentActionGroup")
	defer b.mu.Unlock()

	key := agentActionGroupKey(agentID, actionGroupID)

	ag, ok := b.agentActionGroups[key]
	if !ok {
		return nil, fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	if description != "" {
		ag.Description = description
	}

	if executor != nil {
		ag.ActionGroupExecutor = executor
	}

	ag.UpdatedAt = time.Now()
	cp := *ag

	return &cp, nil
}

// DeleteAgentActionGroup deletes an action group.
func (b *InMemoryBackend) DeleteAgentActionGroup(agentID, actionGroupID string) error {
	b.mu.Lock("DeleteAgentActionGroup")
	defer b.mu.Unlock()

	key := agentActionGroupKey(agentID, actionGroupID)

	if _, ok := b.agentActionGroups[key]; !ok {
		return fmt.Errorf("%w: action group %q not found", ErrNotFound, actionGroupID)
	}

	delete(b.agentActionGroups, key)

	return nil
}

// ---------------------------------------------------------------------------
// Agent Aliases
// ---------------------------------------------------------------------------

// CreateAgentAlias creates an alias for an agent.
func (b *InMemoryBackend) CreateAgentAlias(
	agentID, aliasName, agentVersion string,
) (*AgentAlias, error) {
	b.mu.Lock("CreateAgentAlias")
	defer b.mu.Unlock()

	if _, ok := b.agents[agentID]; !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	b.agentAliasCounter++
	aliasID := fmt.Sprintf("alias-%08d", b.agentAliasCounter)
	aliasArn := arn.Build("bedrock", b.region, b.accountID, "agent-alias/"+agentID+"/"+aliasID)
	now := time.Now()

	alias := &AgentAlias{
		CreatedAt:      now,
		UpdatedAt:      now,
		AgentAliasID:   aliasID,
		AgentAliasArn:  aliasArn,
		AgentAliasName: aliasName,
		AgentID:        agentID,
		AgentVersion:   agentVersion,
		AliasStatus:    aliasStatusPrepared,
	}
	b.agentAliases[agentAliasKey(agentID, aliasID)] = alias
	cp := *alias

	return &cp, nil
}

// GetAgentAlias returns an alias by ID.
func (b *InMemoryBackend) GetAgentAlias(agentID, aliasID string) (*AgentAlias, error) {
	b.mu.RLock("GetAgentAlias")
	defer b.mu.RUnlock()

	alias, ok := b.agentAliases[agentAliasKey(agentID, aliasID)]
	if !ok {
		return nil, fmt.Errorf("%w: agent alias %q not found", ErrNotFound, aliasID)
	}

	cp := *alias

	return &cp, nil
}

// ListAgentAliases lists aliases for an agent.
func (b *InMemoryBackend) ListAgentAliases(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentAlias, string) {
	b.mu.RLock("ListAgentAliases")
	defer b.mu.RUnlock()

	list := make([]*AgentAlias, 0, len(b.agentAliases))
	prefix := agentID + "/"

	for k, alias := range b.agentAliases {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *alias
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AgentAliasName < list[j].AgentAliasName })

	return paginate(list, maxResults, nextToken)
}

// UpdateAgentAlias updates an agent alias.
func (b *InMemoryBackend) UpdateAgentAlias(
	agentID, aliasID, aliasName, agentVersion string,
) (*AgentAlias, error) {
	b.mu.Lock("UpdateAgentAlias")
	defer b.mu.Unlock()

	key := agentAliasKey(agentID, aliasID)

	alias, ok := b.agentAliases[key]
	if !ok {
		return nil, fmt.Errorf("%w: agent alias %q not found", ErrNotFound, aliasID)
	}

	if aliasName != "" {
		alias.AgentAliasName = aliasName
	}

	if agentVersion != "" {
		alias.AgentVersion = agentVersion
	}

	alias.UpdatedAt = time.Now()
	cp := *alias

	return &cp, nil
}

// DeleteAgentAlias deletes an agent alias.
func (b *InMemoryBackend) DeleteAgentAlias(agentID, aliasID string) error {
	b.mu.Lock("DeleteAgentAlias")
	defer b.mu.Unlock()

	key := agentAliasKey(agentID, aliasID)

	if _, ok := b.agentAliases[key]; !ok {
		return fmt.Errorf("%w: agent alias %q not found", ErrNotFound, aliasID)
	}

	delete(b.agentAliases, key)

	return nil
}

// ---------------------------------------------------------------------------
// Agent Knowledge Base associations
// ---------------------------------------------------------------------------

// AssociateAgentKnowledgeBase links a knowledge base to an agent.
func (b *InMemoryBackend) AssociateAgentKnowledgeBase(
	agentID, kbID, description string,
) (*AgentKnowledgeBaseAssociation, error) {
	b.mu.Lock("AssociateAgentKnowledgeBase")
	defer b.mu.Unlock()

	if _, ok := b.agents[agentID]; !ok {
		return nil, fmt.Errorf("%w: agent %q not found", ErrNotFound, agentID)
	}

	if _, ok := b.knowledgeBases[kbID]; !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	assoc := &AgentKnowledgeBaseAssociation{
		AgentID:         agentID,
		AgentVersion:    agentStatusDraft,
		KnowledgeBaseID: kbID,
		Description:     description,
		KBState:         actionGroupEnabled,
	}
	b.agentKBAssociations[agentKBKey(agentID, kbID)] = assoc
	cp := *assoc

	return &cp, nil
}

// DisassociateAgentKnowledgeBase removes a knowledge base association from an agent.
func (b *InMemoryBackend) DisassociateAgentKnowledgeBase(agentID, kbID string) error {
	b.mu.Lock("DisassociateAgentKnowledgeBase")
	defer b.mu.Unlock()

	key := agentKBKey(agentID, kbID)

	if _, ok := b.agentKBAssociations[key]; !ok {
		return fmt.Errorf(
			"%w: association not found for agent %q and kb %q",
			ErrNotFound,
			agentID,
			kbID,
		)
	}

	delete(b.agentKBAssociations, key)

	return nil
}

// GetAgentKnowledgeBase returns an agent knowledge base association.
func (b *InMemoryBackend) GetAgentKnowledgeBase(
	agentID, kbID string,
) (*AgentKnowledgeBaseAssociation, error) {
	b.mu.RLock("GetAgentKnowledgeBase")
	defer b.mu.RUnlock()

	assoc, ok := b.agentKBAssociations[agentKBKey(agentID, kbID)]
	if !ok {
		return nil, fmt.Errorf(
			"%w: association not found for agent %q and kb %q",
			ErrNotFound,
			agentID,
			kbID,
		)
	}

	cp := *assoc

	return &cp, nil
}

// ListAgentKnowledgeBases returns all knowledge base associations for an agent.
func (b *InMemoryBackend) ListAgentKnowledgeBases(
	agentID string,
	maxResults int,
	nextToken string,
) ([]*AgentKnowledgeBaseAssociation, string) {
	b.mu.RLock("ListAgentKnowledgeBases")
	defer b.mu.RUnlock()

	list := make([]*AgentKnowledgeBaseAssociation, 0, len(b.agentKBAssociations))
	prefix := agentID + "/"

	for k, assoc := range b.agentKBAssociations {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *assoc
			list = append(list, &cp)
		}
	}

	sort.Slice(
		list,
		func(i, j int) bool { return list[i].KnowledgeBaseID < list[j].KnowledgeBaseID },
	)

	return paginate(list, maxResults, nextToken)
}

// ---------------------------------------------------------------------------
// Knowledge Bases
// ---------------------------------------------------------------------------

// CreateKnowledgeBase creates a new knowledge base.
func (b *InMemoryBackend) CreateKnowledgeBase(
	name, description, roleArn string,
	kbConfig, storageConfig map[string]any,
	tags map[string]string,
) (*KnowledgeBase, error) {
	b.mu.Lock("CreateKnowledgeBase")
	defer b.mu.Unlock()

	if _, ok := b.kbByName[name]; ok {
		return nil, fmt.Errorf("%w: knowledge base %q already exists", ErrAlreadyExists, name)
	}

	b.kbCounter++
	id := fmt.Sprintf("kb-%08d", b.kbCounter)
	kbArn := arn.Build("bedrock", b.region, b.accountID, "knowledge-base/"+id)
	now := time.Now()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	kb := &KnowledgeBase{
		CreatedAt:                  now,
		UpdatedAt:                  now,
		KnowledgeBaseID:            id,
		KnowledgeBaseArn:           kbArn,
		Name:                       name,
		Description:                description,
		Status:                     kbStatusActive,
		RoleArn:                    roleArn,
		KnowledgeBaseConfiguration: kbConfig,
		StorageConfiguration:       storageConfig,
		Tags:                       tagsCopy,
	}
	b.knowledgeBases[id] = kb
	b.kbByName[name] = id
	cp := *kb

	return &cp, nil
}

// GetKnowledgeBase returns a knowledge base by ID.
func (b *InMemoryBackend) GetKnowledgeBase(kbID string) (*KnowledgeBase, error) {
	b.mu.RLock("GetKnowledgeBase")
	defer b.mu.RUnlock()

	kb, ok := b.knowledgeBases[kbID]
	if !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	cp := *kb

	return &cp, nil
}

// ListKnowledgeBases returns all knowledge bases with pagination.
func (b *InMemoryBackend) ListKnowledgeBases(
	maxResults int,
	nextToken string,
) ([]*KnowledgeBase, string) {
	b.mu.RLock("ListKnowledgeBases")
	defer b.mu.RUnlock()

	list := make([]*KnowledgeBase, 0, len(b.knowledgeBases))
	for _, kb := range b.knowledgeBases {
		cp := *kb
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, maxResults, nextToken)
}

// UpdateKnowledgeBase updates a knowledge base.
func (b *InMemoryBackend) UpdateKnowledgeBase(
	kbID, name, description, roleArn string,
) (*KnowledgeBase, error) {
	b.mu.Lock("UpdateKnowledgeBase")
	defer b.mu.Unlock()

	kb, ok := b.knowledgeBases[kbID]
	if !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if name != "" && name != kb.Name {
		delete(b.kbByName, kb.Name)
		kb.Name = name
		b.kbByName[name] = kbID
	}

	if description != "" {
		kb.Description = description
	}

	if roleArn != "" {
		kb.RoleArn = roleArn
	}

	kb.UpdatedAt = time.Now()
	cp := *kb

	return &cp, nil
}

// DeleteKnowledgeBase deletes a knowledge base.
func (b *InMemoryBackend) DeleteKnowledgeBase(kbID string) error {
	b.mu.Lock("DeleteKnowledgeBase")
	defer b.mu.Unlock()

	kb, ok := b.knowledgeBases[kbID]
	if !ok {
		return fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	delete(b.kbByName, kb.Name)
	delete(b.knowledgeBases, kbID)

	return nil
}

// ---------------------------------------------------------------------------
// Data Sources
// ---------------------------------------------------------------------------

// CreateDataSource creates a data source for a knowledge base.
func (b *InMemoryBackend) CreateDataSource(
	kbID, name, description string,
	dsConfig map[string]any,
) (*DataSource, error) {
	b.mu.Lock("CreateDataSource")
	defer b.mu.Unlock()

	if _, ok := b.knowledgeBases[kbID]; !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	b.dataSourceCounter++
	id := fmt.Sprintf("ds-%08d", b.dataSourceCounter)
	now := time.Now()

	ds := &DataSource{
		CreatedAt:               now,
		UpdatedAt:               now,
		DataSourceID:            id,
		DataSourceStatus:        dsStatusAvailable,
		KnowledgeBaseID:         kbID,
		Name:                    name,
		Description:             description,
		DataSourceConfiguration: dsConfig,
	}
	b.dataSources[kbID+"/"+id] = ds
	cp := *ds

	return &cp, nil
}

// GetDataSource returns a data source by ID.
func (b *InMemoryBackend) GetDataSource(kbID, dsID string) (*DataSource, error) {
	b.mu.RLock("GetDataSource")
	defer b.mu.RUnlock()

	ds, ok := b.dataSources[kbID+"/"+dsID]
	if !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	cp := *ds

	return &cp, nil
}

// ListDataSources lists data sources for a knowledge base.
func (b *InMemoryBackend) ListDataSources(
	kbID string,
	maxResults int,
	nextToken string,
) ([]*DataSource, string) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	list := make([]*DataSource, 0, len(b.dataSources))
	prefix := kbID + "/"

	for k, ds := range b.dataSources {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *ds
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return paginate(list, maxResults, nextToken)
}

// UpdateDataSource updates a data source.
func (b *InMemoryBackend) UpdateDataSource(
	kbID, dsID, name, description string,
) (*DataSource, error) {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	key := kbID + "/" + dsID

	ds, ok := b.dataSources[key]
	if !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	if name != "" {
		ds.Name = name
	}

	if description != "" {
		ds.Description = description
	}

	ds.UpdatedAt = time.Now()
	cp := *ds

	return &cp, nil
}

// DeleteDataSource deletes a data source.
func (b *InMemoryBackend) DeleteDataSource(kbID, dsID string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	key := kbID + "/" + dsID

	if _, ok := b.dataSources[key]; !ok {
		return fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	delete(b.dataSources, key)

	return nil
}

// ---------------------------------------------------------------------------
// Ingestion Jobs
// ---------------------------------------------------------------------------

// StartIngestionJob starts an ingestion job for a data source.
func (b *InMemoryBackend) StartIngestionJob(kbID, dsID, description string) (*IngestionJob, error) {
	b.mu.Lock("StartIngestionJob")
	defer b.mu.Unlock()

	if _, ok := b.knowledgeBases[kbID]; !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if _, ok := b.dataSources[kbID+"/"+dsID]; !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	b.ingestionJobCounter++
	jobID := fmt.Sprintf("ij-%08d", b.ingestionJobCounter)
	now := time.Now()

	job := &IngestionJob{
		StartedAt:       now,
		UpdatedAt:       now,
		IngestionJobID:  jobID,
		KnowledgeBaseID: kbID,
		DataSourceID:    dsID,
		Status:          jobStatusStarting,
		Description:     description,
	}
	b.ingestionJobs[ingestionJobKey(kbID, dsID, jobID)] = job

	// Transition to COMPLETE after a short delay.
	var once sync.Once
	go func() {
		once.Do(func() {
			time.Sleep(ingestionCompleteDelay)
			b.mu.Lock("StartIngestionJob.goroutine")
			defer b.mu.Unlock()
			if j, exists := b.ingestionJobs[ingestionJobKey(kbID, dsID, jobID)]; exists {
				j.Status = jobStatusComplete
				j.UpdatedAt = time.Now()
			}
		})
	}()

	cp := *job

	return &cp, nil
}

// GetIngestionJob returns an ingestion job by ID.
func (b *InMemoryBackend) GetIngestionJob(kbID, dsID, jobID string) (*IngestionJob, error) {
	b.mu.RLock("GetIngestionJob")
	defer b.mu.RUnlock()

	job, ok := b.ingestionJobs[ingestionJobKey(kbID, dsID, jobID)]
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// ListIngestionJobs lists ingestion jobs for a data source.
func (b *InMemoryBackend) ListIngestionJobs(
	kbID, dsID string,
	maxResults int,
	nextToken string,
) ([]*IngestionJob, string) {
	b.mu.RLock("ListIngestionJobs")
	defer b.mu.RUnlock()

	list := make([]*IngestionJob, 0, len(b.ingestionJobs))
	prefix := kbID + "/" + dsID + "/"

	for k, job := range b.ingestionJobs {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *job
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].IngestionJobID < list[j].IngestionJobID })

	return paginate(list, maxResults, nextToken)
}

// ---------------------------------------------------------------------------
// Generic pagination helper
// ---------------------------------------------------------------------------

func paginate[T any](list []*T, maxResults int, nextToken string) ([]*T, string) {
	if maxResults <= 0 {
		maxResults = bedrockDefaultPageSize
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*T{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
