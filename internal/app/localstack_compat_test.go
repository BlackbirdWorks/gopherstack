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
			name: "benign_not_warned", environ: []string{"EAGER_SERVICE_LOADING=1", "LOCALSTACK_AUTH_TOKEN=x"},
			want: map[string]string{}, wantEnable: true,
		},
		{
			name:    "unsupported_warned",
			environ: []string{"KINESIS_LATENCY=5", "LAMBDA_LIMITS_CODE_SIZE_ZIPPED=1", "LOCALSTACK_FOO=1"},
			want:    map[string]string{}, wantEnable: true,
			wantIgnored: []string{"KINESIS_LATENCY", "LAMBDA_LIMITS_CODE_SIZE_ZIPPED", "LOCALSTACK_FOO"},
		},
		{
			name: "dns_address", environ: []string{"DNS_ADDRESS=127.0.0.1", "DNS_SERVER=9.9.9.9"},
			want: map[string]string{"DNS_ADDR": "127.0.0.1:53", "TRANSPARENT_DNS_UPSTREAM": "9.9.9.9:53"},
		},
		{name: "dns_address_disabled", environ: []string{"DNS_ADDRESS=0"}, want: map[string]string{}},
		{
			name:    "snapshot_scheduled",
			environ: []string{"SNAPSHOT_SAVE_STRATEGY=scheduled", "SNAPSHOT_FLUSH_INTERVAL=30"},
			want: map[string]string{
				"GOPHERSTACK_PERSIST_MODE":           "hybrid",
				"GOPHERSTACK_PERSIST_FLUSH_INTERVAL": "30s",
			},
		},
		{
			name:    "snapshot_on_request_startup",
			environ: []string{"SNAPSHOT_SAVE_STRATEGY=ON_REQUEST", "SNAPSHOT_LOAD_STRATEGY=ON_STARTUP"},
			want:    map[string]string{"GOPHERSTACK_PERSIST_MODE": "snapshot"},
		},
		{
			name:    "snapshot_manual_unsupported",
			environ: []string{"SNAPSHOT_SAVE_STRATEGY=MANUAL", "SNAPSHOT_LOAD_STRATEGY=MANUAL"},
			want:    map[string]string{}, wantIgnored: []string{"SNAPSHOT_LOAD_STRATEGY", "SNAPSHOT_SAVE_STRATEGY"},
		},
		{
			name: "lambda_knobs", environ: []string{"LAMBDA_KEEPALIVE_MS=5000", "HOSTNAME_FROM_LAMBDA=host.x"},
			want: map[string]string{"LAMBDA_IDLE_TIMEOUT": "5s", "LAMBDA_DOCKER_HOST": "host.x"},
		},
		{
			name:    "lambda_keepalive_native_wins",
			environ: []string{"LAMBDA_KEEPALIVE_MS=5000", "LAMBDA_IDLE_TIMEOUT=1m"},
			want:    map[string]string{},
		},
		{
			name:    "port_range",
			environ: []string{"EXTERNAL_SERVICE_PORTS_START=4510", "EXTERNAL_SERVICE_PORTS_END=4559"},
			want:    map[string]string{"PORT_RANGE_START": "4510", "PORT_RANGE_END": "4560"},
		},
		{
			name: "proxy_and_ca",
			environ: []string{
				"OUTBOUND_HTTP_PROXY=http://p:1", "OUTBOUND_HTTPS_PROXY=http://p:2", "REQUESTS_CA_BUNDLE=/ca.pem",
			},
			want: map[string]string{
				"HTTP_PROXY":    "http://p:1",
				"HTTPS_PROXY":   "http://p:2",
				"SSL_CERT_FILE": "/ca.pem",
			},
		},
		{
			name: "s3_validate_signatures_unsupported", environ: []string{"S3_VALIDATE_SIGNATURES=1"},
			want: map[string]string{}, wantIgnored: []string{"S3_VALIDATE_SIGNATURES"},
		},
		{
			name: "sqs_toggles_off",
			environ: []string{
				"SQS_DELAY_PURGE_RETRY=0", "SQS_DELAY_RECENTLY_DELETED=false", "SQS_ENABLE_MESSAGE_RETENTION_PERIOD=0",
			},
			want: map[string]string{
				"GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN":            "true",
				"GOPHERSTACK_SQS_DISABLE_RECENTLY_DELETED_COOLDOWN": "true",
				"GOPHERSTACK_SQS_DISABLE_MESSAGE_RETENTION":         "true",
			},
		},
		{
			name:    "sqs_toggles_on_noop",
			environ: []string{"SQS_DELAY_PURGE_RETRY=1", "SQS_ENABLE_MESSAGE_RETENTION_PERIOD=1"},
			want:    map[string]string{},
		},
		{
			name: "docker_flags_family_benign", environ: []string{"LAMBDA_DOCKER_FLAGS=-x", "PROVIDER_OVERRIDE_S3=v2"},
			want: map[string]string{},
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

func TestLocalstackFaultRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env  map[string]string
		name string
		want []string
	}{
		{name: "none", env: map[string]string{}},
		{name: "kinesis", env: map[string]string{"KINESIS_ERROR_PROBABILITY": "0.5"}, want: []string{"kinesis"}},
		{
			name: "both", want: []string{"kinesis", "dynamodb"},
			env: map[string]string{"KINESIS_ERROR_PROBABILITY": "1", "DYNAMODB_ERROR_PROBABILITY": "0.1"},
		},
		{name: "zero", env: map[string]string{"DYNAMODB_ERROR_PROBABILITY": "0"}},
		{name: "out_of_range", env: map[string]string{"DYNAMODB_ERROR_PROBABILITY": "2"}},
		{name: "garbage", env: map[string]string{"DYNAMODB_ERROR_PROBABILITY": "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rules := localstackFaultRules(func(k string) string { return tt.env[k] })

			var got []string

			for _, r := range rules {
				got = append(got, r.Service)
				assert.Equal(t, "ProvisionedThroughputExceededException", r.Error.Code)
				assert.Equal(t, http.StatusBadRequest, r.Error.StatusCode)
				require.NotNil(t, r.Probability)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
