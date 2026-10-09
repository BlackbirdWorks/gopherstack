package apigateway_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

type metricTally struct {
	counts map[string]float64
	mu     sync.Mutex
}

func (m *metricTally) emit(p cwmetric.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(p.Dimensions) == 1 {
		m.counts[p.Name] += p.Value
	}

	return nil
}

func (m *metricTally) get(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.counts[name]
}

func TestStageResponseCache_Metrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cluster      bool
		cachingOn    bool
		flushBetween bool
		wantHits     float64
		wantMisses   float64
	}{
		{name: "hit_after_miss", cluster: true, cachingOn: true, wantHits: 1, wantMisses: 1},
		{name: "flush_forces_miss", cluster: true, cachingOn: true, flushBetween: true, wantMisses: 2},
		{name: "no_cluster", cachingOn: true},
		{name: "caching_off", cluster: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tally := &metricTally{counts: map[string]float64{}}
			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			h.SetMetricEmitter(cwmetric.EmitterFunc(tally.emit))

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(h))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
			require.NoError(t, err)

			client := apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			apiID, _ := setupMockAPI(t, client, "cached-body")

			_, err = client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{
				RestApiId:           aws.String(apiID),
				StageName:           aws.String("prod"),
				CacheClusterEnabled: aws.Bool(tt.cluster),
			})
			require.NoError(t, err)

			if tt.cachingOn {
				_, err = client.UpdateStage(t.Context(), &apigwsdk.UpdateStageInput{
					RestApiId: aws.String(apiID),
					StageName: aws.String("prod"),
					PatchOperations: []apigwtypes.PatchOperation{
						patchOp(apigwtypes.OpReplace, "/*/*/caching/enabled", "true"),
					},
				})
				require.NoError(t, err)
			}

			for i := range 2 {
				status, body := invokeStage(t, srv.URL, apiID, "/widgets")
				require.Equal(t, http.StatusOK, status)
				assert.Equal(t, "cached-body", body)

				if i == 0 && tt.flushBetween {
					require.Eventually(t, func() bool { return tally.get("Count") >= 1 }, time.Second, time.Millisecond)

					_, err = client.FlushStageCache(t.Context(), &apigwsdk.FlushStageCacheInput{
						RestApiId: aws.String(apiID), StageName: aws.String("prod"),
					})
					require.NoError(t, err)
				}
			}

			require.Eventually(t, func() bool { return tally.get("Count") >= 2 }, time.Second, time.Millisecond)
			assert.InDelta(t, tt.wantHits, tally.get("CacheHitCount"), 0)
			assert.InDelta(t, tt.wantMisses, tally.get("CacheMissCount"), 0)
		})
	}
}

func TestStageResponseCache_ControlInvalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		strategy    string
		wantStatus  int
		wantHits    float64
		wantMisses  float64
		signed      bool
		require     bool
		wantWarning bool
	}{
		{name: "open_invalidates", wantStatus: http.StatusOK, wantHits: 1, wantMisses: 2},
		{
			name: "signed_invalidates", require: true, signed: true,
			wantStatus: http.StatusOK, wantHits: 1, wantMisses: 2,
		},
		{
			name: "unsigned_fail", require: true, strategy: "FAIL_WITH_403",
			wantStatus: http.StatusForbidden, wantHits: 0, wantMisses: 1,
		},
		{
			name: "unsigned_warn", require: true, strategy: "SUCCEED_WITH_RESPONSE_HEADER",
			wantStatus: http.StatusOK, wantWarning: true, wantHits: 2, wantMisses: 1,
		},
		{
			name: "unsigned_silent", require: true, strategy: "SUCCEED_WITHOUT_RESPONSE_HEADER",
			wantStatus: http.StatusOK, wantHits: 2, wantMisses: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tally := &metricTally{counts: map[string]float64{}}
			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			h.SetMetricEmitter(cwmetric.EmitterFunc(tally.emit))

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(h))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
			require.NoError(t, err)

			client := apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
			apiID, _ := setupMockAPI(t, client, "cached-body")

			_, err = client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{
				RestApiId: aws.String(apiID), StageName: aws.String("prod"), CacheClusterEnabled: aws.Bool(true),
			})
			require.NoError(t, err)

			ops := []apigwtypes.PatchOperation{patchOp(apigwtypes.OpReplace, "/*/*/caching/enabled", "true")}
			if tt.require {
				ops = append(ops,
					patchOp(apigwtypes.OpReplace, "/*/*/caching/requireAuthorizationForCacheControl", "true"))
			}

			if tt.strategy != "" {
				ops = append(ops,
					patchOp(apigwtypes.OpReplace, "/*/*/caching/unauthorizedCacheControlHeaderStrategy", tt.strategy))
			}

			_, err = client.UpdateStage(t.Context(), &apigwsdk.UpdateStageInput{
				RestApiId: aws.String(apiID), StageName: aws.String("prod"), PatchOperations: ops,
			})
			require.NoError(t, err)

			get := func(cacheControl string) *http.Response {
				req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet,
					srv.URL+"/restapis/"+apiID+"/prod/_user_request_/widgets", nil)
				require.NoError(t, reqErr)

				if cacheControl != "" {
					req.Header.Set("Cache-Control", cacheControl)
				}

				if tt.signed {
					req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=x")
				}

				resp, doErr := http.DefaultClient.Do(req)
				require.NoError(t, doErr)
				_ = resp.Body.Close()

				return resp
			}

			get("")
			second := get("max-age=0")
			assert.Equal(t, tt.wantStatus, second.StatusCode)
			assert.Equal(t, tt.wantWarning, second.Header.Get("Warning") != "")

			if tt.wantStatus == http.StatusOK {
				get("")
			}

			require.Eventually(t, func() bool {
				return tally.get("CacheHitCount")+tally.get("CacheMissCount") >= tt.wantHits+tt.wantMisses
			}, time.Second, time.Millisecond)
			assert.InDelta(t, tt.wantHits, tally.get("CacheHitCount"), 0)
			assert.InDelta(t, tt.wantMisses, tally.get("CacheMissCount"), 0)
		})
	}
}
