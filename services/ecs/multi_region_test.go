package ecs_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *ecs.Handler {
	t.Helper()

	h := ecs.NewHandler(ecs.NewInMemoryBackend("000000000000", mrHome, ecs.NewNoopRunner()))
	h.EnableRegions(t.Context())

	return h
}

func regionCall(t *testing.T, h *ecs.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func clusterArns(t *testing.T, h *ecs.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, "ListClusters", `{}`)["clusterArns"].([]any)
	out := make([]string, 0, len(list))

	for _, v := range list {
		s, _ := v.(string)
		out = append(out, s)
	}

	return out
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				regionCall(t, h, r, "CreateCluster", `{"clusterName":"shared"}`)
				regionCall(t, h, r, "CreateCluster", `{"clusterName":"only-`+r+`"}`)
			}

			for _, r := range tc.regions {
				got := clusterArns(t, h, r)
				require.Len(t, got, 2)

				for _, arn := range got {
					assert.Contains(t, arn, ":ecs:"+r+":")
				}
			}
		})
	}
}

func TestHandler_MultiRegionTasksAndDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: mrHome},
		{name: "peer", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			other := "ap-south-1"

			regionCall(t, h, tc.region, "CreateCluster", `{"clusterName":"c"}`)
			td := regionCall(t, h, tc.region, "RegisterTaskDefinition",
				`{"family":"web","containerDefinitions":[{"name":"app","image":"busybox","memory":64}]}`)
			arn, _ := td["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)
			assert.Contains(t, arn, ":ecs:"+tc.region+":")

			run := regionCall(t, h, tc.region, "RunTask", `{"cluster":"c","taskDefinition":"web"}`)
			tasks, _ := run["tasks"].([]any)
			require.Len(t, tasks, 1)
			taskArn, _ := tasks[0].(map[string]any)["taskArn"].(string)
			assert.Contains(t, taskArn, ":ecs:"+tc.region+":")

			assert.Empty(t, clusterArns(t, h, other))
			listed, _ := regionCall(t, h, other, "ListTaskDefinitions", `{}`)["taskDefinitionArns"].([]any)
			assert.Empty(t, listed)
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			regionCall(t, src, mrHome, "CreateCluster", `{"clusterName":"home"}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", "CreateCluster", `{"clusterName":"eu"}`)
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.(*ecs.InMemoryBackend).Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Len(t, clusterArns(t, dst, mrHome), 1)
			assert.Equal(t, tc.remote, len(clusterArns(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.(*ecs.InMemoryBackend).Restore(t.Context(), snap))
			assert.Len(t, clusterArns(t, old, mrHome), 1)
		})
	}
}
