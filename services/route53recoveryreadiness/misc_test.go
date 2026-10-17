package route53recoveryreadiness_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness"
	"github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	rr "github.com/blackbirdworks/gopherstack/services/route53recoveryreadiness"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	sdkcheck.CheckCompleteness(
		t,
		&sdk.Client{},
		rr.NewHandler(rr.NewInMemoryBackend(testAccount)).GetSupportedOperations(),
		[]string{},
	)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	c, err := f.client.CreateCell(
		ctx,
		&sdk.CreateCellInput{CellName: aws.String("c"), Tags: map[string]string{"k": "v"}},
	)
	require.NoError(t, err)
	_, err = f.client.CreateRecoveryGroup(
		ctx,
		&sdk.CreateRecoveryGroupInput{RecoveryGroupName: aws.String("g"), Cells: []string{aws.ToString(c.CellArn)}},
	)
	require.NoError(t, err)
	rc := f.checkOf(t, "AWS::SQS::Queue", types.Resource{ResourceArn: aws.String(queueARN("q"))})
	_, err = f.client.CreateCrossAccountAuthorization(
		ctx,
		&sdk.CreateCrossAccountAuthorizationInput{CrossAccountAuthorization: aws.String("arn:aws:iam::9:root")},
	)
	require.NoError(t, err)

	snap := f.handler.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := rr.NewHandler(rr.NewInMemoryBackend(testAccount))
	require.NoError(t, restored.Restore(ctx, snap))

	cell, err := restored.Backend.GetCell("c")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, cell.Tags)
	assert.Len(t, cell.ParentReadinessScopes, 1, "the parent link survives a restore")

	_, err = restored.Backend.GetReadinessCheck(rc)
	require.NoError(t, err)

	auths, _, err := restored.Backend.ListCrossAccountAuthorizations("", 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"arn:aws:iam::9:root"}, auths)

	require.NoError(t, restored.Restore(ctx, []byte(`{"version":999}`)))
	_, err = restored.Backend.GetCell("c")
	assert.Error(t, err)
}

func TestReadinessWithoutSiblingsIsUnknown(t *testing.T) {
	t.Parallel()

	b := rr.NewInMemoryBackend(testAccount)
	_, err := b.CreateResourceSet("rs", "AWS::SQS::Queue", []rr.Resource{{ResourceArn: queueARN("q")}}, nil)
	require.NoError(t, err)
	_, err = b.CreateReadinessCheck("rc", "rs", nil)
	require.NoError(t, err)

	readiness, _, resources, _, err := b.ReadinessCheckStatus(t.Context(), "rc", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "UNKNOWN", readiness, "without the owning service there is no basis for READY")
	require.Len(t, resources, 1)
}

func TestRouting(t *testing.T) {
	t.Parallel()

	h := rr.NewHandler(rr.NewInMemoryBackend(testAccount))

	tests := []struct {
		name, method, path, auth, wantOp string
		wantMatch                        bool
	}{
		{"create cell", http.MethodPost, "/cells", "route53-recovery-readiness", "CreateCell", true},
		{
			"cell summary",
			http.MethodGet,
			"/cellreadiness/c1",
			"route53-recovery-readiness",
			"GetCellReadinessSummary",
			true,
		},
		{
			"resource status",
			http.MethodGet,
			"/readinesschecks/rc/resource/arn%3Aaws%3Asqs%3A%3A1%3Aq/status",
			"route53-recovery-readiness",
			"GetReadinessCheckResourceStatus",
			true,
		},
		{
			"group recommendations",
			http.MethodGet,
			"/recoverygroups/g/architectureRecommendations",
			"route53-recovery-readiness",
			"GetArchitectureRecommendations",
			true,
		},
		{"tags", http.MethodGet, "/tags/arn%3Aaws%3Ax", "route53-recovery-readiness", "ListTagsForResources", true},
		{"same path other service", http.MethodPost, "/cells", "route53-recovery-control-config", "", false},
		{"unknown path", http.MethodGet, "/nothing", "route53-recovery-readiness", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+tt.auth+"/aws4_request",
			)
			c := echo.New().NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))

			if tt.wantMatch {
				assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			}
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	t.Parallel()

	h := rr.NewHandler(rr.NewInMemoryBackend(testAccount))

	tests := []struct {
		name, method, path, body, wantCode string
		wantStatus                         int
	}{
		{"not found", http.MethodGet, "/cells/none", "", "ResourceNotFoundException", http.StatusNotFound},
		{"bad json", http.MethodPost, "/cells", "{", "ValidationException", http.StatusBadRequest},
		{"unknown operation", http.MethodPost, "/", "{}", "UnknownOperationException", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantCode, rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.NotEmpty(t, body["message"])
			assert.Equal(t, body["message"], body["Message"])
			assert.False(t, strings.HasPrefix(body["message"], tt.wantCode))
		})
	}
}
