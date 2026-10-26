package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestLocalstackAliasesParseIntoNativeConfig(t *testing.T) {
	tests := []struct {
		check func(t *testing.T, c CLI)
		env   map[string]string
		name  string
	}{
		{
			name: "enforce_iam", env: map[string]string{"ENFORCE_IAM": "true"},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.True(t, c.EnforceIAM)
			},
		},
		{
			name: "native_env_beats_alias",
			env:  map[string]string{"ENFORCE_IAM": "true", "GOPHERSTACK_ENFORCE_IAM": "false"},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.False(t, c.EnforceIAM)
			},
		},
		{
			name: "response_header", env: map[string]string{"LOCALSTACK_RESPONSE_HEADER_ENABLED": "1"},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.True(t, c.Localstack.ResponseHeader)
			},
		},
		{
			name: "pods", env: map[string]string{"AUTO_LOAD_POD": "a,b", "MERGE_STRATEGY": "account-region-merge"},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.Equal(t, "a,b", c.Pods.Autoload)
				assert.Equal(t, "account-region-merge", c.Pods.MergeStrategy)
			},
		},
		{
			name: "sqs_native_switches", env: map[string]string{
				"GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN":            "1",
				"GOPHERSTACK_SQS_DISABLE_RECENTLY_DELETED_COOLDOWN": "true",
				"GOPHERSTACK_SQS_DISABLE_MESSAGE_RETENTION":         "true",
				"SQS_DISABLE_CLOUDWATCH_METRICS":                    "1",
			},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.True(t, c.SQS.DisablePurgeCooldown)
				assert.True(t, c.SQS.DisableRecentlyDeletedWait)
				assert.True(t, c.SQS.DisableMessageRetention)
				assert.True(t, c.SQS.DisableCloudWatchMetrics)
			},
		},
		{
			name: "sqs_defaults_aws_accurate",
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.Equal(t, sqsbackend.Settings{}, c.SQS)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)
			_, err = parser.Parse(nil)
			require.NoError(t, err)

			tt.check(t, root.Serve)
		})
	}
}

func TestLocalstackResponseHeaderMiddleware(t *testing.T) {
	t.Parallel()

	e := echo.New()
	e.Pre(localstackResponseHeaderMiddleware)
	e.GET("/x", func(c *echo.Context) error { return c.NoContent(http.StatusOK) })

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	assert.Equal(t, "true", rec.Header().Get("X-Localstack"))
}

func TestPodAutoloadStrategiesAndOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     string
		strategy string
		want     string
		wantErr  bool
	}{
		{name: "single", spec: "a", want: "one"},
		{name: "later_pod_wins", spec: "a,b", want: "two"},
		{name: "service_merge", spec: "a,b", strategy: "service-merge", want: "two"},
		{name: "overwrite_named", spec: "a", strategy: "OVERWRITE", want: "one"},
		{name: "unknown_strategy", spec: "a", strategy: "bogus", wantErr: true, want: "dirty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := logger.Save(context.Background(), logger.NewTestLogger())
			dir := t.TempDir()
			st := &podFakeState{state: "one"}
			mgr := persistence.NewManager(ctx, persistence.NullStore{})
			mgr.Register("a", st)

			e := echo.New()
			registerPodEndpoints(ctx, logger.NewTestLogger(), e, PodsConfig{Dir: dir}, "", mgr, nil)
			require.Equal(t, http.StatusCreated, podDo(e, http.MethodPost, "/_gopherstack/pods/a", "").Code)

			st.state = "two"
			require.Equal(t, http.StatusCreated, podDo(e, http.MethodPost, "/_gopherstack/pods/b", "").Code)

			st.state = "dirty"
			svc, err := newPodService(ctx, PodsConfig{Dir: dir}, "")
			require.NoError(t, err)

			api := &podAPI{svc: svc, manager: mgr}
			err = api.autoload(ctx, tt.spec, tt.strategy)

			if tt.wantErr {
				require.ErrorIs(t, err, errUnknownMergeStrategy)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, st.get())
		})
	}
}

func TestInitRuntimeStoresSeedsLocalstackFaults(t *testing.T) {
	t.Setenv("DYNAMODB_ERROR_PROBABILITY", "0.25")

	cli := &CLI{}
	initRuntimeStores(cli)

	rules := cli.faultStore.GetRules()
	require.Len(t, rules, 1)
	assert.Equal(t, "dynamodb", rules[0].Service)
	assert.InDelta(t, 0.25, *rules[0].Probability, 1e-9)
}
