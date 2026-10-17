package route53recoverycontrolconfig_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	sdkcheck.CheckCompleteness(
		t,
		&sdk.Client{},
		cc.NewHandler(cc.NewInMemoryBackend(testAccount)).GetSupportedOperations(),
		[]string{},
	)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.topology(t)
	ctx := t.Context()

	_, err := f.client.CreateSafetyRule(ctx, newAssertion(topo, nil))
	require.NoError(t, err)
	require.NoError(t, f.backend.UpdateRoutingControlStates([]cc.StateUpdate{{ARN: topo.ctlA, State: "On"}}, nil, ""))
	_, err = f.client.TagResource(
		ctx,
		&sdk.TagResourceInput{ResourceArn: aws.String(topo.cluster), Tags: map[string]string{"k": "v"}},
	)
	require.NoError(t, err)

	snap := f.handler.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := cc.NewHandler(cc.NewInMemoryBackend(testAccount))
	require.NoError(t, restored.Restore(ctx, snap))

	v, err := restored.Backend.RoutingControlState(topo.ctlA, "")
	require.NoError(t, err)
	assert.Equal(t, "On", v.State)

	rules, _, err := restored.Backend.ListSafetyRules(topo.defaultPanel, "", 0)
	require.NoError(t, err)
	assert.Len(t, rules, 1)

	tags, err := restored.Backend.ListTagsForResource(topo.cluster)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, tags)

	require.NoError(t, restored.Restore(ctx, []byte(`{"version":999}`)))
	_, err = restored.Backend.DescribeCluster(t.Context(), topo.cluster)
	assert.Error(t, err, "an incompatible snapshot starts empty")
}

func serve(h *cc.Handler, method, target string, auth string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+auth+"/aws4_request")
	}

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	_ = h.Handler()(c)

	return rec
}

func TestRouting(t *testing.T) {
	t.Parallel()

	h := cc.NewHandler(cc.NewInMemoryBackend(testAccount))

	tests := []struct {
		name, method, path, auth, wantOp string
		wantMatch                        bool
	}{
		{"list clusters", http.MethodGet, "/cluster", "route53-recovery-control-config", "ListClusters", true},
		{"update cluster", http.MethodPut, "/cluster", "route53-recovery-control-config", "UpdateCluster", true},
		{
			"describe encoded arn",
			http.MethodGet,
			"/cluster/arn%3Aaws%3Aroute53-recovery-control%3A%3A1%3Acluster%2Fabc",
			"route53-recovery-control-config",
			"DescribeCluster",
			true,
		},
		{"tags", http.MethodPost, "/tags/arn%3Aaws%3Ax", "route53-recovery-control-config", "TagResource", true},
		{
			"health checks",
			http.MethodGet,
			"/routingcontrol/x/associatedRoute53HealthChecks",
			"route53-recovery-control-config",
			"ListAssociatedRoute53HealthChecks",
			true,
		},
		{"other service same path", http.MethodGet, "/cluster", "route53-recovery-readiness", "ListClusters", false},
		{"unknown path", http.MethodGet, "/nothing", "route53-recovery-control-config", "Unknown", false},
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

	h := cc.NewHandler(cc.NewInMemoryBackend(testAccount))

	tests := []struct {
		name, method, path, body, wantCode string
		wantStatus                         int
	}{
		{"not found", http.MethodGet, "/cluster/arn%3Anone", "", "ResourceNotFoundException", http.StatusNotFound},
		{"bad json", http.MethodPost, "/cluster", "{", "ValidationException", http.StatusBadRequest},
		{"unknown operation", http.MethodPost, "/", "{}", "UnknownOperationException", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := serve(h, tt.method, tt.path, "route53-recovery-control-config", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantCode, rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.NotEmpty(t, body["message"])
			assert.Equal(t, body["message"], body["Message"])
			assert.False(
				t,
				strings.HasPrefix(body["message"], tt.wantCode),
				"message must not repeat the code: %q",
				body["message"],
			)
		})
	}
}
