package bedrockagent

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// Context key
// ---------------------------------------------------------------------------

type regionKey struct{}

func ctxRegion(ctx context.Context, dflt string) string {
	if r, ok := ctx.Value(regionKey{}).(string); ok && r != "" {
		return r
	}

	return dflt
}

// ---------------------------------------------------------------------------
// InMemoryBackend
// ---------------------------------------------------------------------------

// InMemoryBackend implements StorageBackend with in-memory maps, isolated by region.
//
// Phase 3.3 datalayer conversion: every map[string]*T resource collection is
// a *store.Table[T] registered on b.registry, with a *store.Index[T] added
// where a parent-scoped List/bulk-delete previously did a linear key-prefix
// scan (see store_setup.go's registerAllTables). Resources with a real,
// globally-unique identity field (agents, knowledgeBases, flows, prompts)
// are registered directly by that ID; resources whose ID is only unique
// within a parent (agent/flow/knowledge-base versions, action groups,
// aliases, collaborators, associations, data sources, ingestion jobs, KB
// documents) keep the same "parent/child[/grandchild]" composite key the
// original map already used, matching the composite-key rule in
// .claude/memories/parity-principles.md.
//
// agentsByName, kbsByName, flowsByName, and promptsByName are left as plain
// map[string]string reverse-lookup caches (name -> ID): their values are
// bare strings, not *T, so there is nothing for store.Table to key on.
// agentVersionCtrs, flowVersionCtrs, and promptVersionCtrs (map[string]int)
// are likewise left raw for the same reason. tags (map[string]map[string]string)
// is the one remaining grouping map with a non-*T value.
type InMemoryBackend struct {
	kbDocuments             *store.Table[KBDocumentDetail]
	kbDocumentsByDataSource *store.Index[KBDocumentDetail]

	agentsByName map[string]string

	agents *store.Table[Agent]

	agentVersions        *store.Table[AgentVersion]
	agentVersionsByAgent *store.Index[AgentVersion]

	actionGroups               *store.Table[AgentActionGroup]
	actionGroupsByAgentVersion *store.Index[AgentActionGroup]

	agentAliases        *store.Table[AgentAlias]
	agentAliasesByAgent *store.Index[AgentAlias]

	agentCollaborators               *store.Table[AgentCollaborator]
	agentCollaboratorsByAgentVersion *store.Index[AgentCollaborator]

	knowledgeBases *store.Table[KnowledgeBase]
	kbsByName      map[string]string

	agentKBAssocs               *store.Table[AgentKnowledgeBase]
	agentKBAssocsByAgentVersion *store.Index[AgentKnowledgeBase]

	dataSources     *store.Table[DataSource]
	dataSourcesByKB *store.Index[DataSource]

	ingestionJobs             *store.Table[IngestionJob]
	ingestionJobsByDataSource *store.Index[IngestionJob]

	flows       *store.Table[Flow]
	flowsByName map[string]string

	flowVersions       *store.Table[FlowVersion]
	flowVersionsByFlow *store.Index[FlowVersion]

	flowAliases       *store.Table[FlowAlias]
	flowAliasesByFlow *store.Index[FlowAlias]

	prompts *store.Table[Prompt]

	promptVersions         *store.Table[PromptVersion]
	promptVersionsByPrompt *store.Index[PromptVersion]
	promptsByName          map[string]string
	promptVersionCtrs      map[string]int

	tags             map[string]map[string]string
	flowVersionCtrs  map[string]int
	agentVersionCtrs map[string]int

	registry *store.Registry

	accountID          string
	defaultRegion      string
	dsCounter          int
	collabCounter      int
	kbCounter          int
	flowCounter        int
	aliasCounter       int
	agentCounter       int
	actionGroupCounter int
	flowAliasCounter   int
	promptCounter      int
	jobCounter         int
	mu                 sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates and initialises an InMemoryBackend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:          store.NewRegistry(),
		agentsByName:      make(map[string]string),
		kbsByName:         make(map[string]string),
		flowsByName:       make(map[string]string),
		promptsByName:     make(map[string]string),
		tags:              make(map[string]map[string]string),
		agentVersionCtrs:  make(map[string]int),
		flowVersionCtrs:   make(map[string]int),
		promptVersionCtrs: make(map[string]int),
		defaultRegion:     region,
		accountID:         accountID,
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state (used in tests).
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.agentsByName = make(map[string]string)
	b.kbsByName = make(map[string]string)
	b.flowsByName = make(map[string]string)
	b.promptsByName = make(map[string]string)
	b.tags = make(map[string]map[string]string)
	b.agentVersionCtrs = make(map[string]int)
	b.flowVersionCtrs = make(map[string]int)
	b.promptVersionCtrs = make(map[string]int)
	b.agentCounter = 0
	b.actionGroupCounter = 0
	b.aliasCounter = 0
	b.collabCounter = 0
	b.kbCounter = 0
	b.dsCounter = 0
	b.jobCounter = 0
	b.flowCounter = 0
	b.flowAliasCounter = 0
	b.promptCounter = 0
}

// ---------------------------------------------------------------------------
// ID/ARN helpers
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) nextID(prefix string, counter *int) string {
	*counter++

	return fmt.Sprintf("%s-%08d", prefix, *counter)
}

func (b *InMemoryBackend) buildAgentARN(region, agentID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "agent/"+agentID)
}

func (b *InMemoryBackend) buildKBARN(region, kbID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "knowledge-base/"+kbID)
}

func (b *InMemoryBackend) buildFlowARN(region, flowID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "flow/"+flowID)
}

func (b *InMemoryBackend) buildPromptARN(region, promptID string) string {
	return arn.Build(bedrockAgentService, region, b.accountID, "prompt/"+promptID)
}

func (b *InMemoryBackend) buildAliasARN(region, agentID, aliasID string) string {
	return arn.Build(
		bedrockAgentService,
		region,
		b.accountID,
		fmt.Sprintf("agent-alias/%s/%s", agentID, aliasID),
	)
}

func (b *InMemoryBackend) buildFlowAliasARN(region, flowID, aliasID string) string {
	return arn.Build(
		bedrockAgentService,
		region,
		b.accountID,
		fmt.Sprintf("flow-alias/%s/%s", flowID, aliasID),
	)
}

// ---------------------------------------------------------------------------
// store.Table composite-key helpers
// ---------------------------------------------------------------------------

// agentVersionScope is the composite key shared by every table nested two
// levels under an agent (actionGroups, agentCollaborators, agentKBAssocs):
// agentID + "/" + agentVersion. It doubles as the byAgentVersion secondary
// index key for each of those tables.
func agentVersionScope(agentID, agentVersion string) string {
	return agentID + "/" + agentVersion
}

func agentVersionKey(agentID, version string) string { return agentID + "/" + version }

func agentCollabKey(agentID, agentVersion, collaboratorID string) string {
	return agentVersionScope(agentID, agentVersion) + "/" + collaboratorID
}

func flowVersionKey(flowID, version string) string { return flowID + "/" + version }

func promptVersionKey(promptID, version string) string { return promptID + "/" + version }

// ---------------------------------------------------------------------------
// Pagination helper
// ---------------------------------------------------------------------------

const defaultPageSize = 100

func paginate(ids []string, nextToken string, maxResults int) ([]string, string) {
	start := 0

	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	size := defaultPageSize

	if maxResults > 0 && maxResults < defaultPageSize {
		size = maxResults
	}

	end := min(start+size, len(ids))

	page := ids[start:end]

	var outToken string

	if end < len(ids) {
		outToken = ids[end]
	}

	return page, outToken
}

// tableIDs extracts an ID from every item in items via idFn and returns the
// IDs sorted ascending -- the store.Table/store.Index equivalent of the
// pre-Phase-3.3 sortedKeys(map[string]V) helper (both ultimately do a plain
// lexicographic string sort, matching pkgs/collections.SortedKeys, which
// pkgs/store.Table.Snapshot also uses internally). items may come from
// [store.Table.Snapshot] (already key-sorted, so the sort below is a cheap
// no-op) or a [store.Index.Get] group (unordered, so the sort is required).
func tableIDs[V any](items []*V, idFn func(*V) string) []string {
	ids := make([]string, len(items))
	for i, v := range items {
		ids[i] = idFn(v)
	}

	sort.Strings(ids)

	return ids
}
