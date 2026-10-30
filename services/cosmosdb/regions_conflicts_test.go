package cosmosdb_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func multiWrite() cosmosdb.AccountRegions {
	const lagMs = 5000

	return cosmosdb.AccountRegions{MultiWrite: true, Regions: []cosmosdb.AccountRegion{
		{Name: eastUS, FailoverPriority: 0, ReplicationLagMs: lagMs},
		{Name: westUS, FailoverPriority: 1, ReplicationLagMs: lagMs},
	}}
}

func (e *regionEnv) newColl(policy string) {
	e.t.Helper()

	body := `{"id":"p","partitionKey":{"paths":["/pk"],"kind":"Hash"}`
	if policy != "" {
		body += `,"conflictResolutionPolicy":` + policy
	}

	require.Equal(e.t, http.StatusCreated, e.do(http.MethodPost, "/dbs/d/colls", nil, body+`}`).status)
}

func (e *regionEnv) putIn(region, doc string) rawResponse {
	e.t.Helper()

	return e.doAt(region, http.MethodPost, "/dbs/d/colls/p/docs", regionHdr(map[string]string{
		"X-Ms-Documentdb-Is-Upsert": "true",
	}), doc)
}

func (e *regionEnv) readIn(region, id string) (int, map[string]any) {
	e.t.Helper()

	r := e.doAt(region, http.MethodGet, "/dbs/d/colls/p/docs/"+id, regionHdr(nil), "")
	if r.status != http.StatusOK {
		return r.status, nil
	}

	return r.status, r.object(e.t)
}

func (e *regionEnv) rankIn(region string) float64 {
	e.t.Helper()

	status, doc := e.readIn(region, "x")
	require.Equal(e.t, http.StatusOK, status, "x in %s", region)

	rank, _ := doc["rank"].(float64)

	return rank
}

func TestConflicts_LastWriterWins(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		eastDoc    string
		westDoc    string
		eastLater  bool
		wantWinner float64
	}{
		{
			name:       "custom path: the higher value wins even when written first",
			policy:     `{"mode":"LastWriterWins","conflictResolutionPath":"/rank"}`,
			eastDoc:    `{"id":"x","pk":"a","rank":9}`,
			westDoc:    `{"id":"x","pk":"a","rank":5}`,
			wantWinner: 9,
		},
		{
			name: "custom path: west higher", policy: `{"mode":"LastWriterWins","conflictResolutionPath":"/rank"}`,
			eastDoc: `{"id":"x","pk":"a","rank":2}`, westDoc: `{"id":"x","pk":"a","rank":7}`, wantWinner: 7,
		},
		{
			name: "default path _ts: the later write wins", policy: ``,
			eastDoc: `{"id":"x","pk":"a","rank":1}`, westDoc: `{"id":"x","pk":"a","rank":2}`, wantWinner: 2,
		},
		{
			name:       "missing path falls back to _ts",
			policy:     `{"mode":"LastWriterWins","conflictResolutionPath":"/absent"}`,
			eastDoc:    `{"id":"x","pk":"a","rank":1}`,
			westDoc:    `{"id":"x","pk":"a","rank":2}`,
			wantWinner: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRegionEnv(t, multiWrite())
			e.newColl(tt.policy)

			requireWritten(t, e.putIn(eastUS, tt.eastDoc))
			e.clock.advance(2 * time.Second)
			requireWritten(t, e.putIn(westUS, tt.westDoc))

			assert.InDelta(t, jsonRank(t, tt.eastDoc), e.rankIn(eastUS), 0, "each region sees its own write first")
			assert.InDelta(t, jsonRank(t, tt.westDoc), e.rankIn(westUS), 0)

			e.clock.advance(10 * time.Second)

			assert.InDelta(t, tt.wantWinner, e.rankIn(eastUS), 0, "all regions converge to one winner")
			assert.InDelta(t, tt.wantWinner, e.rankIn(westUS), 0)

			feed := e.doAt(eastUS, http.MethodGet, "/dbs/d/colls/p/conflicts", nil, "")
			require.Equal(t, http.StatusOK, feed.status)
			assert.Empty(t, feed.object(t)["Conflicts"], "last-writer-wins conflicts never reach the feed")
		})
	}
}

func jsonRank(t *testing.T, doc string) float64 {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(doc), &m))

	r, _ := m["rank"].(float64)

	return r
}

func TestConflicts_DeleteWins(t *testing.T) {
	t.Parallel()

	e := newRegionEnv(t, multiWrite())
	e.newColl(`{"mode":"LastWriterWins","conflictResolutionPath":"/rank"}`)

	requireWritten(t, e.putIn(eastUS, `{"id":"x","pk":"a","rank":1}`))
	e.clock.advance(10 * time.Second)

	del := e.doAt(eastUS, http.MethodDelete, "/dbs/d/colls/p/docs/x", regionHdr(nil), "")
	require.Equal(t, http.StatusNoContent, del.status)
	requireWritten(t, e.putIn(westUS, `{"id":"x","pk":"a","rank":99}`))

	e.clock.advance(10 * time.Second)

	for _, region := range []string{eastUS, westUS} {
		status, _ := e.readIn(region, "x")
		assert.Equal(t, http.StatusNotFound, status, region+": a delete beats a higher-ranked replace")
	}
}

const resolverSproc = `function resolver(incomingItem, existingItem, isTombstone, conflictingItems) {
	var collection = getContext().getCollection();
	if (!incomingItem || isTombstone) { return; }
	if (existingItem && incomingItem.rank > existingItem.rank) { return; }
	if (existingItem) {
		collection.replaceDocument(existingItem._self, incomingItem, function (err) { if (err) throw err; });
	} else {
		collection.createDocument(collection.getSelfLink(), incomingItem, function (err) { if (err) throw err; });
	}
}`

func TestConflicts_CustomResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		policy       string
		sproc        string
		wantFeed     bool
		wantEastRank float64
		wantWestRank float64
	}{
		{
			name:         "merge procedure picks the lowest rank in every region",
			policy:       `{"mode":"Custom","conflictResolutionProcedure":"dbs/d/colls/p/sprocs/resolver"}`,
			sproc:        resolverSproc,
			wantEastRank: 3,
			wantWestRank: 3,
		},
		{
			name:         "a failing procedure sends the conflict to the feed",
			policy:       `{"mode":"Custom","conflictResolutionProcedure":"dbs/d/colls/p/sprocs/resolver"}`,
			sproc:        `function resolver() { throw new Error("cannot merge"); }`,
			wantFeed:     true,
			wantEastRank: 3,
			wantWestRank: 8,
		},
		{
			name: "custom without a procedure uses the feed", policy: `{"mode":"Custom"}`,
			wantFeed: true, wantEastRank: 3, wantWestRank: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRegionEnv(t, multiWrite())
			e.newColl(tt.policy)

			if tt.sproc != "" {
				require.Equal(t, http.StatusCreated, e.do(http.MethodPost, "/dbs/d/colls/p/sprocs", nil,
					scriptJSON(t, map[string]string{"id": "resolver", "body": tt.sproc})).status)
			}

			requireWritten(t, e.putIn(eastUS, `{"id":"x","pk":"a","rank":3}`))
			e.clock.advance(time.Second)
			requireWritten(t, e.putIn(westUS, `{"id":"x","pk":"a","rank":8}`))
			e.clock.advance(10 * time.Second)

			assert.InDelta(t, tt.wantEastRank, e.rankIn(eastUS), 0)
			assert.InDelta(t, tt.wantWestRank, e.rankIn(westUS), 0)

			count := 0

			for _, region := range []string{eastUS, westUS} {
				feed := e.doAt(region, http.MethodGet, "/dbs/d/colls/p/conflicts", nil, "")
				require.Equal(t, http.StatusOK, feed.status)

				items, _ := feed.object(t)["Conflicts"].([]any)
				count += len(items)

				for _, it := range items {
					m := rowObj(t, it)
					assert.Equal(t, "document", m["resourceType"])
					assert.Contains(t, []any{"create", "replace"}, m["operationType"])
					assert.Contains(t, m["content"], `"id":"x"`)

					got := e.doAt(region, http.MethodGet, "/dbs/d/colls/p/conflicts/"+m["id"].(string), nil, "")
					assert.Equal(t, http.StatusOK, got.status)

					del := e.doAt(region, http.MethodDelete, "/dbs/d/colls/p/conflicts/"+m["id"].(string), nil, "")
					assert.Equal(t, http.StatusNoContent, del.status)
					assert.Equal(t, http.StatusNotFound,
						e.doAt(region, http.MethodGet, "/dbs/d/colls/p/conflicts/"+m["id"].(string), nil, "").status)
				}
			}

			if tt.wantFeed {
				assert.Positive(t, count)
			} else {
				assert.Zero(t, count, "a successful merge procedure leaves nothing in the feed")
			}
		})
	}
}

func TestRegions_AutomaticFailover(t *testing.T) {
	t.Parallel()

	topo := cosmosdb.AccountRegions{Regions: []cosmosdb.AccountRegion{
		{Name: eastUS, FailoverPriority: 0},
		{Name: westUS, FailoverPriority: 1},
		{Name: euWest, FailoverPriority: 2},
	}}
	e := newRegionEnv(t, topo)

	writeRegion := func() string {
		body := e.admin(http.MethodGet, "regions", "").object(t)

		return body["writeRegion"].(string)
	}

	require.Equal(t, eastUS, writeRegion())
	requireWritten(t, e.putAt(eastUS, `{"id":"a","pk":"a"}`))

	off := e.admin(http.MethodPost, "regions/East%20US/offline", "")
	require.Equal(t, http.StatusOK, off.status, string(off.body))
	assert.Equal(t, westUS, writeRegion(), "the next failover priority takes over")

	down := e.getAt(eastUS, "a", nil)
	assert.Equal(t, http.StatusForbidden, down.status)
	assert.Equal(t, "1008", down.header.Get("X-Ms-Substatus"))

	requireWritten(t, e.putAt(westUS, `{"id":"b","pk":"a"}`))
	assert.Equal(t, http.StatusForbidden, e.putAt(euWest, `{"id":"c","pk":"a"}`).status)

	root := e.do(http.MethodGet, "/", nil, "").object(t)
	writable, _ := root["writableLocations"].([]any)
	require.Len(t, writable, 1)
	assert.Equal(t, westUS, rowObj(t, writable[0])["name"])

	on := e.admin(http.MethodPost, "regions/East%20US/online", "")
	require.Equal(t, http.StatusOK, on.status)
	assert.Equal(t, westUS, writeRegion(), "a recovered region rejoins as a read region")
	assert.Equal(t, http.StatusOK, e.getAt(eastUS, "b", nil).status)

	assert.Equal(t, http.StatusNotFound, e.admin(http.MethodPost, "regions/Nowhere/offline", "").status)

	e.admin(http.MethodPost, "regions/West%20US/offline", "")
	assert.Equal(t, eastUS, writeRegion())
}

func TestRegions_FailoverDoesNotLoseCommittedWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policy   cosmosdb.ConsistencyPolicy
		wantLost bool
	}{
		{name: "strong has nothing in flight", policy: cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencyStrong}},
		{
			name:     "session write in flight is delivered after recovery",
			policy:   cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencySession},
			wantLost: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			topo := twoRegions(60_000)
			topo.Consistency = &tt.policy
			e := newRegionEnv(t, topo)

			requireWritten(t, e.putAt(eastUS, `{"id":"inflight","pk":"a"}`))
			require.Equal(t, http.StatusOK, e.admin(http.MethodPost, "regions/East%20US/offline", "").status)

			status := e.getAt(westUS, "inflight", nil).status
			if tt.wantLost {
				assert.Equal(t, http.StatusNotFound, status, "the unreplicated write is not in the new write region")
			} else {
				assert.Equal(t, http.StatusOK, status)
			}

			require.Equal(t, http.StatusOK, e.admin(http.MethodPost, "regions/East%20US/online", "").status)
			e.clock.advance(2 * time.Minute)
			assert.Equal(
				t,
				http.StatusOK,
				e.getAt(westUS, "inflight", nil).status,
				"it arrives once the old region is back",
			)
		})
	}
}

func TestRegions_FailoverConflictsGoToTheFeed(t *testing.T) {
	t.Parallel()

	e := newRegionEnv(t, twoRegions(60_000))

	requireWritten(t, e.putAt(eastUS, `{"id":"x","pk":"a","v":"old-home"}`))
	require.Equal(t, http.StatusOK, e.admin(http.MethodPost, "regions/East%20US/offline", "").status)
	requireWritten(t, e.putAt(westUS, `{"id":"x","pk":"a","v":"new-home"}`))

	require.Equal(t, http.StatusOK, e.admin(http.MethodPost, "regions/East%20US/online", "").status)
	e.clock.advance(2 * time.Minute)

	assert.Equal(t, "new-home", e.getAt(westUS, "x", nil).object(t)["v"])

	feed := e.doAt(westUS, http.MethodGet, "/dbs/d/colls/c/conflicts", nil, "")
	require.Equal(t, http.StatusOK, feed.status)

	items, _ := feed.object(t)["Conflicts"].([]any)
	require.Len(t, items, 1, "the write the former write region never replicated surfaces as a conflict")
	assert.Contains(t, rowObj(t, items[0])["content"], "old-home")
}

func TestRegions_AdminTopologyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "valid",
			body: `{"regions":[{"name":"A"},{"name":"B","failoverPriority":1,"replicationLagMs":10}],"automaticFailover":true}`,
			want: http.StatusOK,
		},
		{
			name: "strong with multiple writers",
			body: `{"multiWrite":true,"automaticFailover":true,` +
				`"consistency":{"defaultConsistencyLevel":"Strong"},"regions":[{"name":"A"},{"name":"B","failoverPriority":1}]}`,
			want: http.StatusBadRequest,
		},
		{
			name: "unknown level",
			body: `{"automaticFailover":true,"consistency":{"defaultConsistencyLevel":"Maybe"},"regions":[{"name":"A"}]}`,
			want: http.StatusBadRequest,
		},
		{
			name: "staleness below the multi-region minimum",
			body: `{"automaticFailover":true,` +
				`"consistency":{"defaultConsistencyLevel":"BoundedStaleness","maxStalenessPrefix":10},` +
				`"regions":[{"name":"A"},{"name":"B","failoverPriority":1}]}`,
			want: http.StatusBadRequest,
		},
		{
			name: "staleness at the single-region minimum",
			body: `{"automaticFailover":true,` +
				`"consistency":{"defaultConsistencyLevel":"BoundedStaleness","maxStalenessPrefix":10,"maxIntervalInSeconds":5},` +
				`"regions":[{"name":"A"}]}`,
			want: http.StatusOK,
		},
		{
			name: "negative lag",
			body: `{"automaticFailover":true,"regions":[{"name":"A","replicationLagMs":-1}]}`,
			want: http.StatusBadRequest,
		},
		{name: "not json", body: `{`, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLiveServer(t)
			env := &regionEnv{liveServer: e, clock: &fakeClock{}}
			assert.Equal(t, tt.want, env.admin(http.MethodPut, "regions", tt.body).status)
		})
	}
}

func TestRegions_DefaultAccountUnchanged(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)

	resp := s.create(`{"id":"d1","pk":"a"}`)
	require.Equal(t, http.StatusCreated, resp.status)
	assert.Equal(t, "0:0#0", resp.header.Get("X-Ms-Session-Token"), "single region keeps the static token")

	body := s.do(http.MethodGet, "/", nil, "").object(t)
	assert.Equal(t, map[string]any{"defaultConsistencyLevel": "Session"}, body["userConsistencyPolicy"])

	locs, _ := body["readableLocations"].([]any)
	require.Len(t, locs, 1)
	assert.Equal(t, s.base+"/", rowObj(t, locs[0])["databaseAccountEndpoint"])
}

func TestRegions_SnapshotKeepsTopologyAndConflicts(t *testing.T) {
	t.Parallel()

	e := newRegionEnv(t, multiWrite())
	e.newColl(`{"mode":"Custom"}`)

	requireWritten(t, e.putIn(eastUS, `{"id":"x","pk":"a","rank":3}`))
	e.clock.advance(time.Second)
	requireWritten(t, e.putIn(westUS, `{"id":"x","pk":"a","rank":8}`))
	e.clock.advance(10 * time.Second)
	status, _ := e.readIn(eastUS, "x")
	require.Equal(t, http.StatusOK, status)

	policy := cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencyBoundedStaleness, MaxStalenessPrefix: 7}
	topo := e.backend.AccountRegions()
	topo.Consistency = &policy
	e.backend.ConfigureAccountRegions(topo)
	require.NoError(t, e.backend.SetRegionOffline(westUS, true))

	snap := e.h.Snapshot(t.Context())

	restored := newLiveServer(t)
	require.NoError(t, restored.h.Restore(t.Context(), snap))

	got := restored.backend.AccountRegions()
	assert.True(t, got.MultiWrite)
	assert.Equal(t, policy, *got.Consistency)
	assert.Equal(t, []cosmosdb.AccountRegion{
		{Name: eastUS, ReplicationLagMs: 5000},
		{Name: westUS, FailoverPriority: 1, ReplicationLagMs: 5000, Offline: true},
	}, got.Regions)

	feed, err := restored.backend.ListConflicts("d", "p", eastUS)
	require.NoError(t, err)
	assert.Len(t, feed, 1, "the conflicts feed survives a restore")
}
