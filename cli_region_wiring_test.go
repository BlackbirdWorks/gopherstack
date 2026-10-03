package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

func TestEcrImageURIRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  string
		want string
	}{
		{name: "ecr", uri: "000000000000.dkr.ecr.eu-west-1.amazonaws.com/app:v1", want: "eu-west-1"},
		{name: "digest", uri: "000000000000.dkr.ecr.us-east-1.amazonaws.com/a/b@sha256:abc", want: "us-east-1"},
		{name: "not-ecr", uri: "docker.io/library/busybox:latest", want: ""},
		{name: "empty", uri: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, ecrImageURIRegion(tc.uri))
		})
	}
}

func regionTargetCall(
	t *testing.T, h echo.HandlerFunc, region, target, body string,
) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func TestInitializeServices_RegionAwareCrossServiceWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19300, 19400)
	require.NoError(t, err)

	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(&service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portAlloc,
	})
	require.NoError(t, err)

	byName := serviceByName(services)
	glueH, ok := byName["Glue"].(*gluebackend.Handler)
	require.True(t, ok)

	athenaH, ok := byName["Athena"].(*athenabackend.Handler)
	require.True(t, ok)

	ecrH, ok := byName["ECR"].(*ecrbackend.Handler)
	require.True(t, ok)

	regionCall := func(region string) []string {
		out := regionTargetCall(t, athenaH.Handler(), region, "AmazonAthena.ListDatabases",
			`{"CatalogName":"AwsDataCatalog"}`)

		list, _ := out["DatabaseList"].([]any)

		names := make([]string, 0, len(list))

		for _, v := range list {
			m, _ := v.(map[string]any)
			n, _ := m["Name"].(string)
			names = append(names, n)
		}

		return names
	}

	tests := []struct {
		name    string
		region  string
		wantIn  string
		wantOut string
	}{
		{name: "eu-west-1", region: "eu-west-1", wantIn: "glue-eu", wantOut: "glue-use1"},
		{name: "us-east-1", region: "us-east-1", wantIn: "glue-use1", wantOut: "glue-eu"},
	}

	for region, name := range map[string]string{"eu-west-1": "glue-eu", "us-east-1": "glue-use1"} {
		regionTargetCall(t, glueH.Handler(), region, "AWSGlue.CreateDatabase",
			`{"DatabaseInput":{"Name":"`+name+`"}}`)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := regionCall(tc.region)
			assert.Contains(t, got, tc.wantIn)
			assert.NotContains(t, got, tc.wantOut)
		})
	}

	t.Run("ecr-lambda-resolver-region", func(t *testing.T) {
		t.Parallel()

		regionTargetCall(t, ecrH.Handler(), "eu-west-1",
			"AmazonEC2ContainerRegistry_V20150921.CreateRepository", `{"repositoryName":"eu-app"}`)

		euBk, isMem := ecrH.BackendFor("eu-west-1").(*ecrbackend.InMemoryBackend)
		require.True(t, isMem)

		_, putErr := euBk.PutImage(t.Context(), "eu-app", ecrbackend.Image{
			ImageID:       ecrbackend.ImageIdentifier{ImageTag: "v1"},
			ImageManifest: `{"schemaVersion":2}`,
		})
		require.NoError(t, putErr)

		a := &lambdaECRResolverAdapter{handler: ecrH}
		assert.True(t, a.ResolveImage("000000000000.dkr.ecr.eu-west-1.amazonaws.com/eu-app:v1"))
		assert.False(t, a.ResolveImage("000000000000.dkr.ecr.us-east-1.amazonaws.com/eu-app:v1"))
	})
}
