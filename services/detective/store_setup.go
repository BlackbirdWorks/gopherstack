package detective

// Code in this file supports Phase 3.3 of the datalayer refactor: graphs,
// members, and investigations -- the three map[string]*T / nested
// map[string]map[string]*T resource collections on InMemoryBackend -- are
// replaced with *store.Table[T], following the pattern services/sesv2 and
// services/guardduty established. See pkgs/store's package doc for the
// underlying primitive.
//
// graphs already carries its own identity (storedGraph.Arn), so it is a
// "clean" table keyed directly on that field and registered on b.registry.
//
// members and investigations were nested maps keyed by graphARN then a
// per-graph child ID (accountID / investigationID). Both storedMember and
// storedInvestigation already carry their parent's GraphARN as a real
// (non-hidden) field alongside their own child identity (AccountID /
// InvestigationID), so both flatten cleanly into a single Table keyed by the
// composite "<graphARN>|<childID>" string, with a secondary store.Index
// grouping by GraphARN for the graph-scoped lookups ("all members of graph
// X", "all investigations of graph X") the nested map used to answer
// directly. Both are therefore "clean" tables too -- no DTO wrapper needed.
//
// Left as plain maps (not store.Table-backed):
//   - tags, datasources (map[string]map[string]string): values are plain
//     strings, not *T, so they do not fit store.Table's keyed-by-identity
//     shape.
//   - orgConfigs (map[string]bool): same reason -- plain bool values.
//   - orgAdmins ([]*storedOrgAdmin): order-sensitive (append-ordered, walked
//     in insertion order by ListOrganizationAdminAccounts' pagination) with
//     no field guaranteed unique across entries, so it stays a raw slice --
//     store.Index does not preserve insertion order.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func graphKeyFn(v *storedGraph) string { return v.Arn }

// memberKey builds the composite "<graphARN>|<accountID>" key shared by every
// member nested under a behavior graph. Access sites use this directly so the
// exact same key is used for Put/Get/Has/Delete.
func memberKey(graphARN, accountID string) string { return graphARN + "|" + accountID }

func memberKeyFn(v *storedMember) string { return memberKey(v.GraphARN, v.AccountID) }

func memberGraphIndexKeyFn(v *storedMember) string { return v.GraphARN }

// investigationKey builds the composite "<graphARN>|<investigationID>" key
// shared by every investigation nested under a behavior graph. Access sites
// use this directly so the exact same key is used for Put/Get/Has/Delete.
func investigationKey(graphARN, investigationID string) string {
	return graphARN + "|" + investigationID
}

func investigationKeyFn(v *storedInvestigation) string {
	return investigationKey(v.GraphARN, v.InvestigationID)
}

func investigationGraphIndexKeyFn(v *storedInvestigation) string { return v.GraphARN }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.graphs = store.Register(b.registry, "graphs", store.New(graphKeyFn))

	b.members = store.Register(b.registry, "members", store.New(memberKeyFn))
	b.membersByGraph = b.members.AddIndex("byGraph", memberGraphIndexKeyFn)

	b.investigations = store.Register(b.registry, "investigations", store.New(investigationKeyFn))
	b.investigationsByGraph = b.investigations.AddIndex("byGraph", investigationGraphIndexKeyFn)
}
