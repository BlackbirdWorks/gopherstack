package cosmosdb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

type throughputServer struct {
	t       *testing.T
	h       *cosmosdb.Handler
	backend *cosmosdb.InMemoryBackend
	clock   *fakeClock
	base    string
	seq     int
}

func newThroughputServer(t *testing.T, enforce, serverless bool) *throughputServer {
	t.Helper()

	return newBurstServer(t, enforce, serverless, false)
}

func newBurstServer(t *testing.T, enforce, serverless, burst bool) *throughputServer {
	t.Helper()

	backend := cosmosdb.NewInMemoryBackend()
	backend.SetServerless(serverless)

	h := cosmosdb.NewHandler(backend)
	h.Port = 0
	h.EnforceThroughput = enforce
	h.BurstCapacity = burst

	clock := &fakeClock{}
	cosmosdb.SetHandlerClock(h, clock.now)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	return &throughputServer{
		t:       t,
		h:       h,
		backend: backend,
		clock:   clock,
		base:    fmt.Sprintf("http://127.0.0.1:%d", h.Port),
	}
}

func (s *throughputServer) do(method, path string, headers map[string]string, body string) rawResponse {
	s.t.Helper()

	req, err := http.NewRequestWithContext(s.t.Context(), method, s.base+path, strings.NewReader(body))
	require.NoError(s.t, err)

	req.Header.Set("Content-Type", "application/json")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(s.t).Do(req)
	require.NoError(s.t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(s.t, err)

	return rawResponse{status: resp.StatusCode, header: resp.Header, body: raw}
}

func (s *throughputServer) mustCreate(path, body string, headers map[string]string) {
	s.t.Helper()

	require.Equal(s.t, http.StatusCreated, s.do(http.MethodPost, path, headers, body).status)
}

func (s *throughputServer) container(coll string, offer map[string]string) {
	s.t.Helper()

	s.mustCreate("/dbs/d/colls", `{"id":"`+coll+`","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`, offer)
}

func (s *throughputServer) put(db, coll string) rawResponse {
	s.t.Helper()

	s.seq++

	return s.do(http.MethodPost, "/dbs/"+db+"/colls/"+coll+"/docs",
		map[string]string{"X-Ms-Documentdb-Partitionkey": `["a"]`},
		fmt.Sprintf(`{"id":"i%d","pk":"a"}`, s.seq))
}

// fill writes until a 429 or max requests, returning the throttled response (status 0 when none).
func (s *throughputServer) fill(db, coll string, maxRequests int) (rawResponse, int) {
	s.t.Helper()

	for i := 1; i <= maxRequests; i++ {
		if r := s.put(db, coll); r.status == http.StatusTooManyRequests {
			return r, i
		}
	}

	return rawResponse{}, maxRequests
}

func manualHeader(n int) map[string]string {
	return map[string]string{"X-Ms-Offer-Throughput": strconv.Itoa(n)}
}

func autoHeader(maxRU int) map[string]string {
	return map[string]string{"X-Ms-Cosmos-Offer-Autopilot-Settings": fmt.Sprintf(`{"maxThroughput":%d}`, maxRU)}
}

func TestThroughput_Throttling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		offer      map[string]string
		name       string
		limit      int
		enforce    bool
		serverless bool
		wantThrot  bool
	}{
		{name: "manual enforced", enforce: true, offer: manualHeader(400), limit: 200, wantThrot: true},
		{name: "autoscale enforced", enforce: true, offer: autoHeader(1000), limit: 400, wantThrot: true},
		{name: "not enforced", offer: manualHeader(400), limit: 200},
		{name: "no offer unlimited", enforce: true, limit: 200},
		{name: "serverless cap", enforce: true, serverless: true, limit: 1500, wantThrot: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, tt.enforce, tt.serverless)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			s.container("c", tt.offer)

			res, n := s.fill("d", "c", tt.limit)
			if !tt.wantThrot {
				assert.Zero(t, res.status, "never throttled after %d requests", n)

				return
			}

			require.Equal(t, http.StatusTooManyRequests, res.status)
			assert.Equal(t, "3200", res.header.Get("X-Ms-Substatus"))
			assert.NotEmpty(t, res.header.Get("X-Ms-Retry-After-Ms"))
			assert.Equal(t, "TooManyRequests", res.object(t)["code"])
			assert.Contains(t, res.object(t)["message"], "Request rate is large")

			retry, err := time.ParseDuration(res.header.Get("X-Ms-Retry-After-Ms") + "ms")
			require.NoError(t, err)

			s.clock.advance(retry)
			assert.Equal(t, http.StatusCreated, s.put("d", "c").status, "admitted once the debt is repaid")
		})
	}
}

func TestThroughput_SharedAndDedicated(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, true, false)
	s.mustCreate("/dbs", `{"id":"d"}`, manualHeader(400))
	s.container("a", nil)
	s.container("b", nil)
	s.container("own", manualHeader(100000))

	res, _ := s.fill("d", "a", 300)
	require.Equal(t, http.StatusTooManyRequests, res.status)
	assert.Equal(t, http.StatusTooManyRequests, s.put("d", "b").status, "siblings share the database budget")
	assert.Equal(t, http.StatusCreated, s.put("d", "own").status, "a dedicated container has its own budget")

	s.clock.advance(time.Minute)
	assert.Equal(t, http.StatusCreated, s.put("d", "b").status)
}

func TestThroughput_ResetClearsMeter(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, true, false)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(400))

	res, _ := s.fill("d", "c", 200)
	require.Equal(t, http.StatusTooManyRequests, res.status)

	s.h.Reset()
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(400))
	assert.Equal(t, http.StatusCreated, s.put("d", "c").status)
}

func TestThroughput_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		spec    cosmosdb.ThroughputSpec
	}{
		{name: "manual ok", spec: cosmosdb.ThroughputSpec{Manual: 400}},
		{name: "manual max", spec: cosmosdb.ThroughputSpec{Manual: 1_000_000}},
		{name: "manual below min", spec: cosmosdb.ThroughputSpec{Manual: 300}, wantErr: "below the minimum of 400"},
		{name: "manual step", spec: cosmosdb.ThroughputSpec{Manual: 450}, wantErr: "multiple of 100"},
		{name: "manual above max", spec: cosmosdb.ThroughputSpec{Manual: 1_000_100}, wantErr: "exceeds the maximum"},
		{name: "autoscale ok", spec: cosmosdb.ThroughputSpec{AutoscaleMax: 4000, IncrementPercent: 10}},
		{
			name:    "autoscale below min",
			spec:    cosmosdb.ThroughputSpec{AutoscaleMax: 500},
			wantErr: "below the minimum of 1000",
		},
		{name: "autoscale step", spec: cosmosdb.ThroughputSpec{AutoscaleMax: 1500}, wantErr: "multiple of 1000"},
		{
			name:    "autoscale increment",
			spec:    cosmosdb.ThroughputSpec{AutoscaleMax: 1000, IncrementPercent: 101},
			wantErr: "between 0 and 100",
		},
		{name: "both", spec: cosmosdb.ThroughputSpec{Manual: 400, AutoscaleMax: 1000}, wantErr: "not both"},
		{name: "neither", spec: cosmosdb.ThroughputSpec{}, wantErr: "required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.spec.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, cosmosdb.ErrInvalidThroughput)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestOffers_REST(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, false, false)
	s.mustCreate("/dbs", `{"id":"d"}`, manualHeader(400))
	s.container("c", autoHeader(4000))
	s.container("plain", nil)

	list := s.do(http.MethodGet, "/offers", nil, "")
	require.Equal(t, http.StatusOK, list.status)

	offers, _ := list.object(t)["Offers"].([]any)
	require.Len(t, offers, 2)

	dbInfo, err := s.backend.GetDatabase("d")
	require.NoError(t, err)

	q := s.do(http.MethodPost, "/offers", map[string]string{"X-Ms-Documentdb-Isquery": "true"},
		fmt.Sprintf(`{"query":"SELECT * FROM c WHERE c.offerResourceId = '%s'"}`, dbInfo.RID))
	require.Equal(t, http.StatusOK, q.status, string(q.body))

	found, _ := q.object(t)["Offers"].([]any)
	require.Len(t, found, 1)

	offer := rowObj(t, found[0])
	assert.Equal(t, "V2", offer["offerVersion"])
	assert.InDelta(t, 400, rowObj(t, offer["content"])["offerThroughput"], 0)

	rid, _ := offer["id"].(string)

	get := s.do(http.MethodGet, "/offers/"+rid, nil, "")
	require.Equal(t, http.StatusOK, get.status)
	assert.Equal(t, "400", get.header.Get("X-Ms-Cosmos-Min-Throughput"))

	put := s.do(http.MethodPut, "/offers/"+rid, nil, `{"content":{"offerThroughput":900}}`)
	require.Equal(t, http.StatusOK, put.status, string(put.body))
	assert.InDelta(t, 900, rowObj(t, put.object(t)["content"])["offerThroughput"], 0)

	bad := s.do(http.MethodPut, "/offers/"+rid, nil, `{"content":{"offerThroughput":50}}`)
	assert.Equal(t, http.StatusBadRequest, bad.status)
	assert.Equal(t, http.StatusNotFound, s.do(http.MethodGet, "/offers/nope", nil, "").status)
	assert.Equal(
		t,
		http.StatusNotFound,
		s.do(http.MethodPut, "/offers/nope", nil, `{"content":{"offerThroughput":400}}`).status,
	)
}

func TestOffers_CreateHeaderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		wantMsg    string
		serverless bool
	}{
		{name: "too low", headers: manualHeader(100), wantMsg: "below the minimum"},
		{name: "not a multiple", headers: manualHeader(450), wantMsg: "multiple of 100"},
		{name: "autoscale step", headers: autoHeader(1500), wantMsg: "multiple of 1000"},
		{name: "both", headers: map[string]string{
			"X-Ms-Offer-Throughput": "400", "X-Ms-Cosmos-Offer-Autopilot-Settings": `{"maxThroughput":1000}`,
		}, wantMsg: "not both"},
		{
			name:       "serverless",
			serverless: true,
			headers:    manualHeader(400),
			wantMsg:    "not supported for serverless accounts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, false, tt.serverless)

			for _, path := range []string{"/dbs"} {
				r := s.do(http.MethodPost, path, tt.headers, `{"id":"d"}`)
				require.Equal(t, http.StatusBadRequest, r.status)
				assert.Contains(t, r.object(t)["message"], tt.wantMsg)
			}

			_, err := s.backend.GetDatabase("d")
			require.ErrorIs(t, err, cosmosdb.ErrDatabaseNotFound, "a rejected create leaves nothing behind")

			s.mustCreate("/dbs", `{"id":"e"}`, nil)

			r := s.do(http.MethodPost, "/dbs/e/colls", tt.headers,
				`{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`)
			require.Equal(t, http.StatusBadRequest, r.status)
		})
	}
}

func sdkThroughputClient(t *testing.T) *azcosmos.Client {
	t.Helper()

	s := newThroughputServer(t, false, false)

	cred, err := azcosmos.NewKeyCredential(cosmosdb.DefaultMasterKey)
	require.NoError(t, err)

	client, err := azcosmos.NewClientWithKey(s.base+"/", cred, &azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
		Retry:                        policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return client
}

func TestSDK_DatabaseThroughput(t *testing.T) {
	t.Parallel()

	client := sdkThroughputClient(t)
	ctx := context.Background()

	manual := azcosmos.NewManualThroughputProperties(500)
	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "d"},
		&azcosmos.CreateDatabaseOptions{ThroughputProperties: &manual})
	require.NoError(t, err)

	db, err := client.NewDatabase("d")
	require.NoError(t, err)

	got, err := db.ReadThroughput(ctx, nil)
	require.NoError(t, err)

	v, ok := got.ThroughputProperties.ManualThroughput()
	require.True(t, ok)
	assert.EqualValues(t, 500, v)
	require.NotNil(t, got.MinThroughput)
	assert.EqualValues(t, 400, *got.MinThroughput)

	_, err = db.ReplaceThroughput(ctx, azcosmos.NewManualThroughputProperties(1200), nil)
	require.NoError(t, err)

	got, err = db.ReadThroughput(ctx, nil)
	require.NoError(t, err)
	v, _ = got.ThroughputProperties.ManualThroughput()
	assert.EqualValues(t, 1200, v)

	_, err = db.ReplaceThroughput(ctx, azcosmos.NewManualThroughputProperties(120), nil)
	var respErr *azcore.ResponseError
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
}

func TestSDK_ContainerThroughput(t *testing.T) {
	t.Parallel()

	client := sdkThroughputClient(t)
	ctx := context.Background()

	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "d"}, nil)
	require.NoError(t, err)

	db, err := client.NewDatabase("d")
	require.NoError(t, err)

	auto := azcosmos.NewAutoscaleThroughputPropertiesWithIncrement(4000, 10)
	_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: "auto", PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
	}, &azcosmos.CreateContainerOptions{ThroughputProperties: &auto})
	require.NoError(t, err)

	_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: "none", PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
	}, nil)
	require.NoError(t, err)

	c, err := db.NewContainer("auto")
	require.NoError(t, err)

	got, err := c.ReadThroughput(ctx, nil)
	require.NoError(t, err)

	maxRU, ok := got.ThroughputProperties.AutoscaleMaxThroughput()
	require.True(t, ok)
	assert.EqualValues(t, 4000, maxRU)

	inc, ok := got.ThroughputProperties.AutoscaleIncrement()
	require.True(t, ok)
	assert.EqualValues(t, 10, inc)

	_, manualOK := got.ThroughputProperties.ManualThroughput()
	assert.False(t, manualOK)
	require.NotNil(t, got.MinThroughput)
	assert.EqualValues(t, 400, *got.MinThroughput)

	_, err = c.ReplaceThroughput(ctx, azcosmos.NewAutoscaleThroughputProperties(8000), nil)
	require.NoError(t, err)

	got, err = c.ReadThroughput(ctx, nil)
	require.NoError(t, err)
	maxRU, _ = got.ThroughputProperties.AutoscaleMaxThroughput()
	assert.EqualValues(t, 8000, maxRU)

	none, err := db.NewContainer("none")
	require.NoError(t, err)

	_, err = none.ReadThroughput(ctx, nil)
	var respErr *azcore.ResponseError
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusNotFound, respErr.StatusCode)
}

func TestThroughput_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	src := cosmosdb.NewInMemoryBackend()
	_, err := src.CreateDatabase("d")
	require.NoError(t, err)
	_, err = src.CreateContainer("d", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"})
	require.NoError(t, err)
	_, err = src.SetThroughput("d", "", cosmosdb.ThroughputSpec{Manual: 700})
	require.NoError(t, err)
	_, err = src.SetThroughput("d", "c", cosmosdb.ThroughputSpec{AutoscaleMax: 3000, IncrementPercent: 5})
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(snap, &raw))

	dst := cosmosdb.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), snap))

	got, err := dst.GetThroughput("d", "")
	require.NoError(t, err)
	assert.Equal(t, 700, got.Manual)

	got, err = dst.GetThroughput("d", "c")
	require.NoError(t, err)
	assert.Equal(t, cosmosdb.ThroughputSpec{AutoscaleMax: 3000, IncrementPercent: 5}, got.ThroughputSpec)

	require.NoError(t, dst.DeleteContainer("d", "c"))
	_, err = dst.GetThroughput("d", "c")
	require.ErrorIs(t, err, cosmosdb.ErrContainerNotFound)
	require.NoError(t, dst.DeleteDatabase("d"))
	assert.Empty(t, dst.ListThroughputs(), "offers die with their resource")
}

func TestSDK_ThroughputWithAuthValidation(t *testing.T) {
	t.Parallel()

	client := sdkClient(t, true, cosmosdb.DefaultMasterKey)
	ctx := context.Background()

	tp := azcosmos.NewManualThroughputProperties(400)
	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "d"},
		&azcosmos.CreateDatabaseOptions{ThroughputProperties: &tp})
	require.NoError(t, err)

	db, err := client.NewDatabase("d")
	require.NoError(t, err)

	_, err = db.ReplaceThroughput(ctx, azcosmos.NewManualThroughputProperties(800), nil)
	require.NoError(t, err, "rid-addressed offer requests verify against the offer rid")

	got, err := db.ReadThroughput(ctx, nil)
	require.NoError(t, err)
	v, _ := got.ThroughputProperties.ManualThroughput()
	assert.EqualValues(t, 800, v)
}

func TestThroughput_MinimumFollowsDocumentedFormula(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *cosmosdb.InMemoryBackend)
		target  string
		spec    cosmosdb.ThroughputSpec
		wantMin int
	}{
		{
			name: "manual: highest ever provisioned divided by 100", target: "c",
			setup: func(b *cosmosdb.InMemoryBackend) {
				_, err := b.SetThroughput("d", "c", cosmosdb.ThroughputSpec{Manual: 50_000})
				require.NoError(t, err)
			},
			spec: cosmosdb.ThroughputSpec{Manual: 400}, wantMin: 500,
		},
		{
			name: "autoscale: highest ever divided by 10 rounded up to 1000", target: "c",
			setup: func(b *cosmosdb.InMemoryBackend) {
				_, err := b.SetThroughput("d", "c", cosmosdb.ThroughputSpec{AutoscaleMax: 55_000})
				require.NoError(t, err)
			},
			spec: cosmosdb.ThroughputSpec{AutoscaleMax: 1000}, wantMin: 6000,
		},
		{
			name: "shared database: 100 RU/s per container past 25", target: "",
			setup: func(b *cosmosdb.InMemoryBackend) {
				for i := range 29 {
					spec := cosmosdb.ContainerSpec{ID: fmt.Sprintf("x%d", i), PartitionKeyPath: "/pk"}
					_, err := b.CreateContainer("d", spec)
					require.NoError(t, err)
				}
			},
			spec: cosmosdb.ThroughputSpec{Manual: 400}, wantMin: 900,
		},
		{
			name: "shared autoscale database: 1000 per container past 25", target: "",
			setup: func(b *cosmosdb.InMemoryBackend) {
				for i := range 29 {
					spec := cosmosdb.ContainerSpec{ID: fmt.Sprintf("x%d", i), PartitionKeyPath: "/pk"}
					_, err := b.CreateContainer("d", spec)
					require.NoError(t, err)
				}
			},
			spec: cosmosdb.ThroughputSpec{AutoscaleMax: 1000}, wantMin: 6000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cosmosdb.NewInMemoryBackend()
			_, err := b.CreateDatabase("d")
			require.NoError(t, err)
			_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"})
			require.NoError(t, err)
			tt.setup(b)

			_, err = b.SetThroughput("d", tt.target, tt.spec)
			require.ErrorIs(t, err, cosmosdb.ErrInvalidThroughput)

			ok := tt.spec
			if ok.AutoscaleMax > 0 {
				ok.AutoscaleMax = tt.wantMin
			} else {
				ok.Manual = tt.wantMin
			}

			_, err = b.SetThroughput("d", tt.target, ok)
			require.NoError(t, err)
		})
	}
}

func TestThroughput_MetadataOperationsShareTheFixedAccountBudget(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, true, false)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)

	throttled := -1

	for i := range 400 {
		if s.do(http.MethodGet, "/dbs/d", nil, "").status == http.StatusTooManyRequests {
			throttled = i

			break
		}
	}

	require.Positive(t, throttled, "metadata reads draw on the 240 RU/s account budget")
	assert.InDelta(t, 240, throttled, 5)

	s.clock.advance(time.Second)
	assert.Equal(t, http.StatusOK, s.do(http.MethodGet, "/dbs/d", nil, "").status)
}
