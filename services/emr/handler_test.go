package emr_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emr"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"

	// internalOnlyDispatchOps counts dispatch-table entries that are wired
	// but deliberately excluded from GetSupportedOperations(): today just
	// "ListTagsForResource", which is not a real EMR SDK operation (only
	// AddTags/RemoveTags exist) and is kept routed solely as internal
	// test/tooling scaffolding -- see the comment on that entry in
	// buildOps() (handler.go). GetSupportedOperations() intentionally
	// undercounts the dispatch table by exactly this many entries.
	internalOnlyDispatchOps = 1
)

func newTestHandler(t *testing.T) *emr.Handler {
	t.Helper()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)

	return emr.NewHandler(backend)
}

func doEMRRequest(t *testing.T, h *emr.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ElasticMapReduce."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ----- Provider tests -----

func TestEMR_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	assert.Equal(t, "EMR", p.Name())
}

func TestEMR_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "EMR", svc.Name())
}

func TestEMR_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "EMR", h.Name())
}

func TestEMR_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RunJobFlow")
	assert.Contains(t, ops, "DescribeCluster")
	assert.Contains(t, ops, "ListClusters")
	assert.Contains(t, ops, "TerminateJobFlows")
	assert.Contains(t, ops, "AddTags")
	assert.Contains(t, ops, "RemoveTags")
}

func TestEMR_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestEMR_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching EMR target",
			target: "ElasticMapReduce.RunJobFlow",
			want:   true,
		},
		{
			name:   "non-matching target",
			target: "AmazonEC2ContainerServiceV20141113.CreateCluster",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestEMR_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "RunJobFlow",
			target: "ElasticMapReduce.RunJobFlow",
			want:   "RunJobFlow",
		},
		{
			name:   "DescribeCluster",
			target: "ElasticMapReduce.DescribeCluster",
			want:   "DescribeCluster",
		},
		{
			name:   "empty target",
			target: "",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestEMR_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "NonExistentOp", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "emr", h.ChaosServiceName())
}

func TestEMR_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, []string{testRegion}, h.ChaosRegions())
}

func TestEMR_Backend_Region(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, testRegion, b.Region())
}

func TestEMR_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "RunJobFlow")
	assert.Contains(t, ops, "DescribeCluster")
}

func TestEMR_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "extracts ClusterId",
			body: map[string]any{"ClusterId": "j-123"},
			want: "j-123",
		},
		{
			name: "extracts JobFlowId",
			body: map[string]any{"JobFlowId": "j-456"},
			want: "j-456",
		},
		{
			name: "extracts ResourceId",
			body: map[string]any{"ResourceId": "j-789"},
			want: "j-789",
		},
		{
			name: "returns empty for no IDs",
			body: map[string]any{"Name": "cluster"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestEMR_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AddInstanceFleet",
		"AddInstanceGroups",
		"CancelSteps",
		"CreatePersistentAppUI",
		"CreateSecurityConfiguration",
		"CreateStudio",
		"CreateStudioSessionMapping",
		"DeleteSecurityConfiguration",
		"DeleteStudio",
		"DeleteStudioSessionMapping",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}

// TestReset verifies that Backend.Reset() wipes all state maps.
func TestReset(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{Name: "cluster1", ReleaseLabel: "emr-6.0.0"})
	require.NoError(t, err)
	require.Equal(t, 1, b.ClusterCount())

	b.Reset()

	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.SecurityConfigCount())
	assert.Equal(t, 0, b.StudioCount())
	assert.Equal(t, 0, b.PersistentAppUICount())
	assert.Equal(t, 0, b.StudioSessionMappingCount())
}

// TestMultipleResetCycle verifies that Reset is idempotent.
func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	b.Reset()
	b.Reset()

	assert.Equal(t, 0, b.ClusterCount())
}

// TestHandlerReset verifies that Handler.Reset() delegates to backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "cluster1", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, h.Backend.ClusterCount())
}

// TestProviderInit_NilCtx verifies that ErrNilAppContext is returned for nil AppContext.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, emr.ErrNilAppContext)
}

// TestHandlerOpsPreBuilt verifies that the handler builds its dispatch table at construction time.
func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.HandlerOpsLen(), "dispatch table must be non-empty after NewHandler")
}

// TestGetSupportedOperations_AllOps verifies all new ops are in the supported list.
func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"AddInstanceFleet",
		"AddInstanceGroups",
		"CancelSteps",
		"CreatePersistentAppUI",
		"CreateSecurityConfiguration",
		"CreateStudio",
		"CreateStudioSessionMapping",
		"DeleteSecurityConfiguration",
		"DeleteStudio",
		"DeleteStudioSessionMapping",
		"DescribeSecurityConfiguration",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "operation %s must be in GetSupportedOperations()", op)
	}
}

// TestGetSupportedOperations_MatchDispatch verifies the dispatch table has
// exactly one more entry than the advertised op list -- internalOnlyDispatchOps
// (see its doc comment) accounts for the deliberate exception.
func TestGetSupportedOperations_MatchDispatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, len(h.GetSupportedOperations())+internalOnlyDispatchOps, h.HandlerOpsLen(),
		"dispatch table length must equal GetSupportedOperations length plus internalOnlyDispatchOps")
}

// TestSeedHelpers verifies seed helpers correctly insert into backend.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)

	cluster := &emr.Cluster{
		ID:   "j-0000000000001",
		Name: "seed-cluster",
		ARN:  "arn:aws:elasticmapreduce:us-east-1:000000000000:cluster/j-0000000000001",
		Status: emr.ClusterStatus{
			State: emr.StateWaiting,
		},
	}
	b.AddClusterInternal(context.Background(), cluster)
	assert.Equal(t, 1, b.ClusterCount())

	b.AddSecurityConfigInternal(context.Background(), emr.SecurityConfiguration{
		Name:           "sc1",
		SecurityConfig: `{"EncryptionConfiguration":{}}`,
	})
	assert.Equal(t, 1, b.SecurityConfigCount())

	b.AddStudioInternal(context.Background(), emr.Studio{
		StudioID: "es-0000000000001",
		Name:     "studio1",
	})
	assert.Equal(t, 1, b.StudioCount())

	b.AddPersistentAppUIInternal(context.Background(), emr.PersistentAppUI{
		ID:                "pau-0000000000001",
		TargetResourceArn: cluster.ARN,
	})
	assert.Equal(t, 1, b.PersistentAppUICount())
}

// TestExportCountHelpers verifies all count helpers work.
func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.SecurityConfigCount())
	assert.Equal(t, 0, b.StudioCount())
	assert.Equal(t, 0, b.PersistentAppUICount())
	assert.Equal(t, 0, b.StudioSessionMappingCount())
}

// TestErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreatePersistentAppUI with empty TargetResourceArn.
	rec := doEMRRequest(t, h, "CreatePersistentAppUI", map[string]any{
		"TargetResourceArn": "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errBody struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
	// EMR's real error model has only InvalidRequestException (client fault)
	// and InternalServerException (server fault) -- no ValidationException.
	assert.Equal(t, "InvalidRequestException", errBody.Type)
}

// TestPersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	src := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := src.RunJobFlow(context.Background(), emr.RunJobFlowParams{
		Name:         "persist-cluster",
		ReleaseLabel: "emr-6.8.0",
		Tags:         []emr.Tag{{Key: "env", Value: "dev"}},
	})
	require.NoError(t, err)
	_, err = src.CreateSecurityConfiguration(context.Background(), "sc-1", `{"EncryptionConfiguration":{}}`)
	require.NoError(t, err)
	studio, err := src.CreateStudio(
		context.Background(),
		"studio-persist",
		"",
		"SSO",
		"s3://b",
		"sg-1",
		"arn:role",
		"vpc-1",
		"sg-2",
		nil,
		nil,
		"",
		false,
	)
	require.NoError(t, err)
	err = src.CreateStudioSessionMapping(context.Background(), studio.StudioID, "USER", "uid-1", "", "arn:policy")
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())
	require.NotNil(t, snap)

	dst := emr.NewInMemoryBackend("", "")
	require.NoError(t, dst.Restore(t.Context(), snap))

	assert.Equal(t, 1, dst.ClusterCount())
	assert.Equal(t, 1, dst.SecurityConfigCount())
	assert.Equal(t, 1, dst.StudioCount())
	assert.Equal(t, 1, dst.StudioSessionMappingCount())

	c, err := dst.DescribeCluster(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "persist-cluster", c.Name)
	assert.Equal(t, "emr-6.8.0", c.ReleaseLabel)

	sc, err := dst.DescribeSecurityConfiguration(context.Background(), "sc-1")
	require.NoError(t, err)
	assert.Equal(t, "sc-1", sc.Name)
}

// TestPersistenceEmpty verifies Snapshot/Restore works on empty backend.
func TestPersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := emr.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 0, b2.ClusterCount())
}

// TestHandlerOpsLength verifies dispatch table has all expected operations.
func TestHandlerOpsLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Equal(t, len(ops)+internalOnlyDispatchOps, h.HandlerOpsLen(),
		"every op in GetSupportedOperations must be in the dispatch table, "+
			"plus internalOnlyDispatchOps internal-only exceptions")
}

// TestMatchPriorityHeaderExact verifies the handler returns PriorityHeaderExact.
func TestMatchPriorityHeaderExact(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

// TestProviderInit_WithConfig verifies that Init succeeds with a valid AppContext.
func TestProviderInit_WithConfig(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestRestore_InvalidJSON verifies Restore returns an error for invalid JSON.
func TestRestore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte("not-json"))
	require.Error(t, err)
}

// TestSnapshot_NonNilAfterReset verifies Snapshot returns non-nil after Reset.
func TestSnapshot_NonNilAfterReset(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	b.Reset()
	snap := b.Snapshot(t.Context())
	assert.NotNil(t, snap)
}
