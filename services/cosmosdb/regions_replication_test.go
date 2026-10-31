package cosmosdb_test

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const (
	eastUS = "East US"
	westUS = "West US"
	euWest = "North Europe"
)

// regionEnv is a live multi-region account on a fake clock.
type regionEnv struct {
	*liveServer
	clock *fakeClock
}

func newRegionEnv(t *testing.T, topo cosmosdb.AccountRegions) *regionEnv {
	t.Helper()

	s := newLiveServer(t)
	clk := &fakeClock{}
	cosmosdb.SetNowFunc(s.backend, clk.now)
	s.backend.ConfigureAccountRegions(topo)

	return &regionEnv{liveServer: s, clock: clk}
}

func twoRegions(lagMs int) cosmosdb.AccountRegions {
	return cosmosdb.AccountRegions{Regions: []cosmosdb.AccountRegion{
		{Name: eastUS, FailoverPriority: 0},
		{Name: westUS, FailoverPriority: 1, ReplicationLagMs: lagMs},
	}}
}

// endpoint is the base URL of a region's own listener, as the account root advertises it.
func (e *regionEnv) endpoint(region string) string {
	e.t.Helper()

	body := e.do(http.MethodGet, "/", nil, "").object(e.t)

	list, _ := body["readableLocations"].([]any)
	for _, l := range list {
		m := rowObj(e.t, l)
		if m["name"] == region {
			return strings.TrimSuffix(m["databaseAccountEndpoint"].(string), "/")
		}
	}

	require.FailNow(e.t, "no endpoint for region", region)

	return ""
}

func (e *regionEnv) doAt(region, method, path string, headers map[string]string, body string) rawResponse {
	e.t.Helper()

	req, err := http.NewRequestWithContext(e.t.Context(), method, e.endpoint(region)+path, strings.NewReader(body))
	require.NoError(e.t, err)

	req.Header.Set("Content-Type", "application/json")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	require.NoError(e.t, e.h.SignRequest(req))

	resp, err := testclient.New(e.t).Do(req)
	require.NoError(e.t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)

	return rawResponse{status: resp.StatusCode, header: resp.Header, body: raw}
}

func regionHdr(extra map[string]string) map[string]string {
	out := map[string]string{"X-Ms-Documentdb-Partitionkey": `["a"]`}
	maps.Copy(out, extra)

	return out
}

func (e *regionEnv) putAt(region, doc string) rawResponse {
	e.t.Helper()

	return e.doAt(region, http.MethodPost, "/dbs/d/colls/c/docs", regionHdr(map[string]string{
		"X-Ms-Documentdb-Is-Upsert": "true",
	}), doc)
}

func (e *regionEnv) getAt(region, id string, extra map[string]string) rawResponse {
	e.t.Helper()

	return e.doAt(region, http.MethodGet, "/dbs/d/colls/c/docs/"+id, regionHdr(extra), "")
}

func (e *regionEnv) admin(method, path, body string) rawResponse {
	e.t.Helper()

	req, err := http.NewRequestWithContext(e.t.Context(), method, e.base+"/_gopherstack/cosmosdb/"+path,
		strings.NewReader(body))
	require.NoError(e.t, err)

	resp, err := testclient.New(e.t).Do(req)
	require.NoError(e.t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)

	return rawResponse{status: resp.StatusCode, header: resp.Header, body: raw}
}

func TestRegions_EndpointsAreDistinct(t *testing.T) {
	t.Parallel()

	e := newRegionEnv(t, cosmosdb.AccountRegions{Regions: []cosmosdb.AccountRegion{
		{Name: eastUS}, {Name: westUS, FailoverPriority: 1}, {Name: euWest, FailoverPriority: 2},
	}})

	body := e.do(http.MethodGet, "/", nil, "").object(t)
	endpoints := map[string]string{}

	list, _ := body["readableLocations"].([]any)
	for _, l := range list {
		m := rowObj(t, l)
		endpoints[m["name"].(string)] = m["databaseAccountEndpoint"].(string)
	}

	require.Len(t, endpoints, 3)
	assert.Len(t, map[string]struct{}{
		endpoints[eastUS]: {}, endpoints[westUS]: {}, endpoints[euWest]: {},
	}, 3, "every region has its own host:port")

	writable, _ := body["writableLocations"].([]any)
	require.Len(t, writable, 1)
	assert.Equal(t, eastUS, rowObj(t, writable[0])["name"])

	for region := range endpoints {
		root := e.doAt(region, http.MethodGet, "/", nil, "")
		assert.Equal(t, http.StatusOK, root.status, region)
	}
}

func TestRegions_WritesGoToTheWriteRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region     string
		wantSub    string
		wantStatus int
	}{
		{region: eastUS, wantStatus: http.StatusCreated},
		{region: westUS, wantStatus: http.StatusForbidden, wantSub: "3"},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			e := newRegionEnv(t, twoRegions(0))
			resp := e.putAt(tt.region, `{"id":"w","pk":"a"}`)
			assert.Equal(t, tt.wantStatus, resp.status, string(resp.body))
			assert.Equal(t, tt.wantSub, resp.header.Get("X-Ms-Substatus"))

			read := e.getAt(westUS, "w", nil)
			wantRead := http.StatusNotFound
			if tt.wantStatus == http.StatusCreated {
				wantRead = http.StatusOK
			}

			assert.Equal(t, wantRead, read.status, "with no lag the read region sees the write at once")
		})
	}
}

func TestRegions_ReplicationLag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *regionEnv)
		name  string
	}{
		{name: "new item is missing until the lag elapses", check: func(t *testing.T, e *regionEnv) {
			t.Helper()

			requireWritten(t, e.putAt(eastUS, `{"id":"n","pk":"a","v":1}`))
			assert.Equal(t, http.StatusOK, e.getAt(eastUS, "n", nil).status, "the write region reads its own write")
			assert.Equal(t, http.StatusNotFound, e.getAt(westUS, "n", nil).status)

			e.clock.advance(1999 * time.Millisecond)
			assert.Equal(t, http.StatusNotFound, e.getAt(westUS, "n", nil).status)

			e.clock.advance(time.Millisecond)
			assert.Equal(t, http.StatusOK, e.getAt(westUS, "n", nil).status)
		}},
		{name: "a stale version is read until the update arrives", check: func(t *testing.T, e *regionEnv) {
			t.Helper()

			requireWritten(t, e.putAt(eastUS, `{"id":"n","pk":"a","v":1}`))
			e.clock.advance(2 * time.Second)
			requireWritten(t, e.putAt(eastUS, `{"id":"n","pk":"a","v":2}`))

			assert.InDelta(t, 1, e.getAt(westUS, "n", nil).object(t)["v"], 0)
			assert.InDelta(t, 2, e.getAt(eastUS, "n", nil).object(t)["v"], 0)

			e.clock.advance(2 * time.Second)
			assert.InDelta(t, 2, e.getAt(westUS, "n", nil).object(t)["v"], 0)
		}},
		{name: "a delete reaches the read region late", check: func(t *testing.T, e *regionEnv) {
			t.Helper()

			requireWritten(t, e.putAt(eastUS, `{"id":"n","pk":"a"}`))
			e.clock.advance(2 * time.Second)

			del := e.doAt(eastUS, http.MethodDelete, "/dbs/d/colls/c/docs/n", regionHdr(nil), "")
			require.Equal(t, http.StatusNoContent, del.status)
			assert.Equal(t, http.StatusOK, e.getAt(westUS, "n", nil).status)

			e.clock.advance(2 * time.Second)
			assert.Equal(t, http.StatusNotFound, e.getAt(westUS, "n", nil).status)
		}},
		{name: "queries and lists see the regional copy", check: func(t *testing.T, e *regionEnv) {
			t.Helper()

			requireWritten(t, e.putAt(eastUS, `{"id":"n","pk":"a"}`))

			list := e.doAt(westUS, http.MethodGet, "/dbs/d/colls/c/docs", nil, "")
			assert.Empty(t, list.object(t)["Documents"])

			q := e.doAt(westUS, http.MethodPost, "/dbs/d/colls/c/docs", map[string]string{
				"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json",
			}, `{"query":"SELECT * FROM c"}`)
			assert.Empty(t, q.object(t)["Documents"])

			e.clock.advance(2 * time.Second)
			list = e.doAt(westUS, http.MethodGet, "/dbs/d/colls/c/docs", nil, "")
			assert.Len(t, list.object(t)["Documents"], 1)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.check(t, newRegionEnv(t, twoRegions(2000)))
		})
	}
}

func TestRegions_SessionToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers     func(token string) map[string]string
		name        string
		wantSubstat string
		advance     time.Duration
		wantStatus  int
	}{
		{
			name:       "own token not yet replicated",
			headers:    func(tok string) map[string]string { return map[string]string{"X-Ms-Session-Token": tok} },
			wantStatus: http.StatusNotFound, wantSubstat: "1002",
		},
		{
			name:    "own token once replicated",
			headers: func(tok string) map[string]string { return map[string]string{"X-Ms-Session-Token": tok} },
			advance: 2 * time.Second, wantStatus: http.StatusOK,
		},
		{
			name: "eventual override ignores the token",
			headers: func(tok string) map[string]string {
				return map[string]string{"X-Ms-Session-Token": tok, "X-Ms-Consistency-Level": "Eventual"}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "no token reads like eventual",
			headers:    func(string) map[string]string { return nil },
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRegionEnv(t, twoRegions(2000))
			w := e.putAt(eastUS, `{"id":"s","pk":"a"}`)
			requireWritten(t, w)

			token := w.header.Get("X-Ms-Session-Token")
			require.True(t, strings.HasPrefix(token, "0:-1#"), token)

			e.clock.advance(tt.advance)
			r := e.getAt(westUS, "s", tt.headers(token))
			assert.Equal(t, tt.wantStatus, r.status, string(r.body))
			assert.Equal(t, tt.wantSubstat, r.header.Get("X-Ms-Substatus"))
		})
	}
}

func TestRegions_ConsistencyLevels(t *testing.T) {
	t.Parallel()

	const hour = 3_600_000

	tests := []struct {
		name        string
		policy      cosmosdb.ConsistencyPolicy
		advance     time.Duration
		writes      int
		wantVisible int
	}{
		{
			name:        "strong replicates synchronously",
			policy:      cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencyStrong},
			writes:      3,
			wantVisible: 3,
		},
		{
			name:        "session lags",
			policy:      cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencySession},
			writes:      3,
			wantVisible: 0,
		},
		{
			name: "bounded staleness caps the lag at the interval",
			policy: cosmosdb.ConsistencyPolicy{
				Level:                cosmosdb.ConsistencyBoundedStaleness,
				MaxIntervalInSeconds: 30,
				MaxStalenessPrefix:   100,
			},
			writes:      3,
			advance:     30 * time.Second,
			wantVisible: 3,
		},
		{
			name: "bounded staleness before the interval",
			policy: cosmosdb.ConsistencyPolicy{
				Level:                cosmosdb.ConsistencyBoundedStaleness,
				MaxIntervalInSeconds: 30,
				MaxStalenessPrefix:   100,
			},
			writes:      3,
			advance:     29 * time.Second,
			wantVisible: 0,
		},
		{
			name: "bounded staleness never trails by more than K versions",
			policy: cosmosdb.ConsistencyPolicy{
				Level:                cosmosdb.ConsistencyBoundedStaleness,
				MaxIntervalInSeconds: 3600,
				MaxStalenessPrefix:   2,
			},
			writes:      5,
			wantVisible: 3,
		},
		{
			name:        "eventual lags",
			policy:      cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencyEventual},
			writes:      3,
			wantVisible: 0,
		},
		{
			name:        "consistent prefix lags",
			policy:      cosmosdb.ConsistencyPolicy{Level: cosmosdb.ConsistencyConsistentPrefix},
			writes:      3,
			wantVisible: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			topo := twoRegions(hour)
			topo.Consistency = &tt.policy
			e := newRegionEnv(t, topo)

			for i := range tt.writes {
				requireWritten(t, e.putAt(eastUS, fmt.Sprintf(`{"id":"i%d","pk":"a"}`, i)))
			}

			e.clock.advance(tt.advance)

			list := e.doAt(westUS, http.MethodGet, "/dbs/d/colls/c/docs", nil, "")
			docs, _ := list.object(t)["Documents"].([]any)
			assert.Len(t, docs, tt.wantVisible)

			for i := range tt.wantVisible {
				assert.Equal(
					t,
					http.StatusOK,
					e.getAt(westUS, fmt.Sprintf("i%d", i), nil).status,
					"oldest writes arrive first",
				)
			}
		})
	}
}

func TestRegions_ConsistencyOverrideMayOnlyWeaken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		account string
		header  string
		want    int
	}{
		{account: cosmosdb.ConsistencySession, header: "Eventual", want: http.StatusOK},
		{account: cosmosdb.ConsistencySession, header: "Session", want: http.StatusOK},
		{account: cosmosdb.ConsistencySession, header: "Strong", want: http.StatusBadRequest},
		{account: cosmosdb.ConsistencySession, header: "BoundedStaleness", want: http.StatusBadRequest},
		{account: cosmosdb.ConsistencyEventual, header: "ConsistentPrefix", want: http.StatusBadRequest},
		{account: cosmosdb.ConsistencyStrong, header: "Session", want: http.StatusOK},
		{account: cosmosdb.ConsistencySession, header: "Whatever", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.account+"/"+tt.header, func(t *testing.T) {
			t.Parallel()

			topo := twoRegions(0)
			topo.Consistency = &cosmosdb.ConsistencyPolicy{Level: tt.account}
			e := newRegionEnv(t, topo)

			resp := e.do(
				http.MethodGet,
				"/dbs/d/colls/c/docs",
				map[string]string{"X-Ms-Consistency-Level": tt.header},
				"",
			)
			assert.Equal(t, tt.want, resp.status, string(resp.body))
		})
	}
}

func requireWritten(t *testing.T, r rawResponse) {
	t.Helper()

	require.Less(t, r.status, http.StatusMultipleChoices, string(r.body))
}
