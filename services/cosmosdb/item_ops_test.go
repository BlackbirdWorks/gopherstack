package cosmosdb_test

import (
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func versionFeedStatus(s *liveServer, cont string) int {
	return s.do(http.MethodGet, "/dbs/d/colls/c/docs", avadHeaders(map[string]string{"If-None-Match": cont}), "").status
}

func TestChangeFeed_VersionsFollowTheContinuousBackupWindow(t *testing.T) {
	t.Parallel()

	const day = 24 * time.Hour

	tests := []struct {
		name       string
		backup     string
		wantOld    int
		wantNewest int
	}{
		{name: "seven day tier", backup: "Continuous7Days", wantOld: http.StatusBadRequest, wantNewest: http.StatusOK},
		{name: "thirty day tier", backup: "Continuous30Days", wantOld: http.StatusOK, wantNewest: http.StatusOK},
		{name: "default is the seven day tier", wantOld: http.StatusBadRequest, wantNewest: http.StatusOK},
		{
			name:       "periodic backup keeps nothing",
			backup:     "Periodic",
			wantOld:    http.StatusBadRequest,
			wantNewest: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			clk := &fakeClock{}
			cosmosdb.SetNowFunc(s.backend, clk.now)

			if tt.backup != "" {
				s.backend.SetAccountPolicies(cosmosdb.AccountPolicies{Consistency: "Session", Backup: tt.backup})
			}

			pk := map[string]string{pkHeader: `["a"]`}
			put := func(id string) {
				resp := s.do(http.MethodPost, "/dbs/d/colls/c/docs", pk, `{"id":"`+id+`","pk":"a"}`)
				require.Equal(t, http.StatusCreated, resp.status, string(resp.body))
			}

			put("old")
			clk.advance(8 * day)
			put("new")

			assert.Equal(t, tt.wantOld, versionFeedStatus(s, `"0"`), "a position before the window")
			assert.Equal(t, tt.wantNewest, versionFeedStatus(s, `"1"`), "the position the first write left")
		})
	}
}

func TestChangeFeed_VersionsKeepMoreThanTenThousandInsideTheWindow(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	pk := map[string]string{pkHeader: `["a"]`}

	for i := range 10_200 {
		resp := s.do(http.MethodPost, "/dbs/d/colls/c/docs", pk, `{"id":"`+time.Duration(i).String()+`","pk":"a"}`)
		require.Equal(t, http.StatusCreated, resp.status)
	}

	feed := s.do(http.MethodGet, "/dbs/d/colls/c/docs", avadHeaders(map[string]string{"If-None-Match": `"0"`}), "")
	require.Equal(t, http.StatusOK, feed.status)
	assert.Equal(t, `"10200"`, feed.header.Get("ETag"), "no entry was dropped by a count cap")
}

func TestPatch_RunsPreAndPostTriggers(t *testing.T) {
	t.Parallel()

	const (
		pre = `function pre() {
			var req = getContext().getRequest();
			var body = req.getBody();
			body.operations.push({op: "set", path: "/touched", value: true});
			req.setBody(body);
		}`
		post = `function post() {
			var doc = getContext().getResponse().getBody();
			doc.postSeen = true;
			getContext().getResponse().setBody(doc);
		}`
		failing = `function failing() { throw new Error("no patch today"); }`
	)

	s := newLiveServer(t)
	require.Equal(t, http.StatusCreated, s.putScript("triggers", "pre", pre,
		map[string]string{"triggerType": "Pre", "triggerOperation": "Replace"}).status)
	require.Equal(t, http.StatusCreated, s.putScript("triggers", "post", post,
		map[string]string{"triggerType": "Post", "triggerOperation": "All"}).status)
	require.Equal(t, http.StatusCreated, s.putScript("triggers", "failing", failing,
		map[string]string{"triggerType": "Pre", "triggerOperation": "Create"}).status)
	require.Equal(t, http.StatusCreated, s.create(`{"id":"p","pk":"a","n":1}`).status)

	patch := func(headers map[string]string) rawResponse {
		h := map[string]string{pkHeader: `["a"]`}
		maps.Copy(h, headers)

		return s.do(http.MethodPatch, "/dbs/d/colls/c/docs/p", h,
			`{"operations":[{"op":"incr","path":"/n","value":1}]}`)
	}

	tests := []struct {
		headers    map[string]string
		name       string
		wantStatus int
		wantN      float64
		wantTouch  bool
		wantPost   bool
	}{
		{
			name:       "pre-trigger edits the operations",
			headers:    map[string]string{"X-Ms-Documentdb-Pre-Trigger-Include": "pre"},
			wantStatus: http.StatusOK,
			wantTouch:  true,
			wantN:      2,
		},
		{
			name:       "post-trigger edits the response",
			headers:    map[string]string{"X-Ms-Documentdb-Post-Trigger-Include": "post"},
			wantStatus: http.StatusOK,
			wantTouch:  true,
			wantPost:   true,
			wantN:      3,
		},
		{
			name:       "a trigger of another operation is refused",
			headers:    map[string]string{"X-Ms-Documentdb-Pre-Trigger-Include": "failing"},
			wantStatus: http.StatusBadRequest,
		},
		{name: "no trigger header runs none", wantStatus: http.StatusOK, wantTouch: true, wantN: 4},
	}

	for _, tt := range tests {
		resp := patch(tt.headers)
		require.Equal(t, tt.wantStatus, resp.status, tt.name+": "+string(resp.body))

		if tt.wantStatus != http.StatusOK {
			continue
		}

		body := resp.object(t)
		_, touched := body["touched"]
		_, post := body["postSeen"]
		assert.Equal(t, tt.wantTouch, touched, tt.name)
		assert.Equal(t, tt.wantPost, post, tt.name)
		assert.InDelta(t, tt.wantN, body["n"], 0, tt.name)
	}
}
