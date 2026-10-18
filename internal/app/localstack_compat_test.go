package app

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

func TestLocalstackEnvPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want        map[string]string
		name        string
		environ     []string
		wantIgnored []string
		wantEnable  bool
	}{
		{name: "empty", environ: nil, want: map[string]string{}},
		{
			name: "debug", environ: []string{"DEBUG=1"},
			want: map[string]string{"LOG_LEVEL": "debug"},
		},
		{
			name: "ls_log_trace", environ: []string{"LS_LOG=trace"},
			want: map[string]string{"LOG_LEVEL": "debug"}, wantEnable: true,
		},
		{
			name: "explicit_log_level_wins", environ: []string{"LS_LOG=trace", "LOG_LEVEL=warn"},
			want: map[string]string{}, wantEnable: true,
		},
		{
			name: "persistence_and_data_dir", environ: []string{"PERSISTENCE=1", "DATA_DIR=/d"},
			want: map[string]string{"PERSIST": "true", "GOPHERSTACK_DATA_DIR": "/d"}, wantEnable: true,
		},
		{
			name: "region", environ: []string{"DEFAULT_REGION=eu-west-1"},
			want: map[string]string{"REGION": "eu-west-1"},
		},
		{
			name: "gateway_listen", environ: []string{"GATEWAY_LISTEN=0.0.0.0:4510,:4511"},
			want:       map[string]string{"PORT": "4510", "GOPHERSTACK_EXTRA_LISTEN": "4511"},
			wantEnable: true,
		},
		{
			name: "lambda_remove_containers_off", environ: []string{"LAMBDA_REMOVE_CONTAINERS=0"},
			want: map[string]string{"LAMBDA_KEEP_CONTAINERS": "true"},
		},
		{
			name: "ignored_and_prefix", environ: []string{"SERVICES=s3,sqs", "LOCALSTACK_AUTH_TOKEN=x"},
			want: map[string]string{}, wantEnable: true,
			wantIgnored: []string{"LOCALSTACK_AUTH_TOKEN", "SERVICES"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			set, ignored, enable := localstackEnvPlan(tt.environ)
			assert.Equal(t, tt.want, set)
			assert.Equal(t, tt.wantEnable, enable)
			assert.Equal(t, tt.wantIgnored, ignored)
		})
	}
}

func TestExtraListenPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want []string
		cli  CLI
	}{
		{name: "off", cli: CLI{Port: "8000"}, want: nil},
		{name: "compat", cli: CLI{Port: "8000", LocalstackCompat: true}, want: []string{"4566"}},
		{name: "compat_main_is_4566", cli: CLI{Port: "4566", LocalstackCompat: true}, want: nil},
		{
			name: "extra_dedup",
			cli:  CLI{Port: "8000", LocalstackCompat: true, ExtraListen: []string{":4566", "4571", "8000"}},
			want: []string{"4566", "4571"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, extraListenPorts(&tt.cli))
		})
	}
}

func TestLocalstackInitState(t *testing.T) {
	t.Parallel()

	st := newLocalstackInitState()
	assert.Empty(t, st.scripts())

	st.record([]inithooks.Result{{Script: "a.sh", OK: true}, {Script: "b.sh"}})

	got := st.scripts()
	require.Len(t, got, 2)
	assert.Equal(t, "SUCCESSFUL", got[0]["state"])
	assert.Equal(t, "ERROR", got[1]["state"])
}

func TestLocalstackInternalEndpoints(t *testing.T) {
	t.Parallel()

	srv, _ := devEndpointsServer(t)

	tests := []struct {
		check  func(t *testing.T, body map[string]any)
		name   string
		method string
		path   string
		status int
	}{
		{
			name: "init_all", method: http.MethodGet, path: "/_localstack/init", status: http.StatusOK,
			check: func(t *testing.T, b map[string]any) {
				t.Helper()
				assert.Equal(
					t,
					map[string]any{"BOOT": true, "START": true, "READY": true, "SHUTDOWN": false},
					b["completed"],
				)
				assert.NotNil(t, b["scripts"])
			},
		},
		{
			name: "init_ready", method: http.MethodGet, path: "/_localstack/init/ready", status: http.StatusOK,
			check: func(t *testing.T, b map[string]any) {
				t.Helper()
				assert.Equal(t, true, b["completed"])
			},
		},
		{name: "init_bad_stage", method: http.MethodGet, path: "/_localstack/init/nope", status: http.StatusNotFound},
		{
			name: "config", method: http.MethodGet, path: "/_localstack/config", status: http.StatusOK,
			check: func(t *testing.T, b map[string]any) {
				t.Helper()
				assert.Equal(t, "us-east-1", b["DEFAULT_REGION"])
			},
		},
		{name: "config_post", method: http.MethodPost, path: "/_localstack/config", status: http.StatusBadRequest},
		{
			name: "diagnose", method: http.MethodGet, path: "/_localstack/diagnose", status: http.StatusOK,
			check: func(t *testing.T, b map[string]any) {
				t.Helper()
				assert.Contains(t, b["services"], "s3")
			},
		},
		{
			name: "plugins", method: http.MethodGet, path: "/_localstack/plugins", status: http.StatusOK,
			check: func(t *testing.T, b map[string]any) {
				t.Helper()
				assert.Contains(t, b["localstack.aws.provider"], "s3")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, raw := devDo(t, tt.method, srv.URL+tt.path, "")
			require.Equal(t, tt.status, status)

			if tt.check == nil {
				return
			}

			var body map[string]any
			require.NoError(t, json.Unmarshal(raw, &body))
			tt.check(t, body)
		})
	}
}

// S3 bucket routing must keep working beside the /_localstack/ prefix.
func TestLocalstackPrefixDoesNotBreakS3(t *testing.T) {
	t.Parallel()

	srv, cfg := devEndpointsServer(t)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	_, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("localstack-bucket")})
	require.NoError(t, err)

	out, err := client.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err)
	require.Len(t, out.Buckets, 1)

	status, _ := devDo(t, http.MethodGet, srv.URL+"/_localstack/info", "")
	assert.Equal(t, http.StatusOK, status)
}
