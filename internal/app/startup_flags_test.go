package app

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func startupFlagsServer(t *testing.T, cli CLI) (*httptest.Server, aws.Config) {
	t.Helper()

	log := buildLogger("")
	cli.AccountID, cli.Region = "000000000000", "us-east-1"
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, wireEchoServer(t.Context(), e, log, &cli, services, nil))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	return srv, cfg
}

func apiErrorCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func TestServiceFilterRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings ServiceFilterSettings
		name     string
		wantOff  []string
		wantOn   []string
	}{
		{
			name:     "allow_list",
			settings: ServiceFilterSettings{Services: []string{"DynamoDB", "SQS"}},
			wantOn:   []string{"dynamodb", "sqs"},
			wantOff:  []string{"sns", "kinesis", "s3"},
		},
		{
			name:     "disable_list_with_alias",
			settings: ServiceFilterSettings{DisableServices: []string{"s3", "Kinesis"}},
			wantOn:   []string{"dynamodb", "sqs", "sns"},
			wantOff:  []string{"kinesis", "s3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cfg := startupFlagsServer(t, CLI{ServiceFilter: tt.settings})

			calls := map[string]func() error{
				"dynamodb": func() error {
					_, err := dynamodb.NewFromConfig(cfg).ListTables(t.Context(), nil)

					return err
				},
				"sqs": func() error {
					_, err := sqs.NewFromConfig(cfg).ListQueues(t.Context(), nil)

					return err
				},
				"sns": func() error {
					_, err := sns.NewFromConfig(cfg).ListTopics(t.Context(), nil)

					return err
				},
				"kinesis": func() error {
					_, err := kinesis.NewFromConfig(cfg).ListStreams(t.Context(), nil)

					return err
				},
				"s3": func() error {
					_, err := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }).
						ListBuckets(t.Context(), nil)

					return err
				},
			}

			for _, n := range tt.wantOn {
				assert.NotEqual(t, errCodeServiceNotEnabled, apiErrorCode(calls[n]()), n)
			}

			for _, n := range tt.wantOff {
				assert.Equal(t, errCodeServiceNotEnabled, apiErrorCode(calls[n]()), n)
			}
		})
	}
}

func TestServiceFilterParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		allowed map[string]bool
		in      ServiceFilterSettings
	}{
		{name: "empty", in: ServiceFilterSettings{}, allowed: map[string]bool{"S3": true, "SQS": true}},
		{
			name: "case_and_alias",
			in:   ServiceFilterSettings{Services: []string{"S3", "Logs", "step-functions", "events"}},
			allowed: map[string]bool{"s3": true, "CloudWatchLogs": true, "StepFunctions": true,
				"EventBridge": true, "SQS": false},
		},
		{
			name:    "unknown_lists_valid",
			in:      ServiceFilterSettings{Services: []string{"s3", "nosuchsvc"}},
			wantErr: "nosuchsvc",
		},
		{
			name:    "unknown_disabled",
			in:      ServiceFilterSettings{DisableServices: []string{"bogus"}},
			wantErr: "valid names:",
		},
		{
			name:    "localstack_unknown_skipped",
			in:      ServiceFilterSettings{LocalstackServices: []string{"s3", "nosuchsvc", "sqs"}},
			allowed: map[string]bool{"S3": true, "SQS": true, "SNS": false},
		},
		{
			name:    "localstack_ignored_when_native_set",
			in:      ServiceFilterSettings{Services: []string{"s3"}, LocalstackServices: []string{"sqs"}},
			allowed: map[string]bool{"S3": true, "SQS": false},
		},
		{
			name:    "disable_beats_allow",
			in:      ServiceFilterSettings{Services: []string{"s3", "sqs"}, DisableServices: []string{"sqs"}},
			allowed: map[string]bool{"S3": true, "SQS": false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, err := newServiceFilter(tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Error(t, (&CLI{ServiceFilter: tt.in}).Validate())

				return
			}

			require.NoError(t, err)

			for name, want := range tt.allowed {
				assert.Equal(t, want, f.enabled(name), name)
			}

			assert.True(t, f.enabled("Dashboard"))
		})
	}
}

func TestCORSMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		origin      string
		reqMethod   string
		wantOrigin  string
		wantHeaders string
		wantCreds   string
		wantExpose  string
		settings    CORSSettings
		wantStatus  int
	}{
		{name: "unconfigured_untouched", method: http.MethodGet, origin: "http://a.test", wantStatus: http.StatusOK},
		{
			name:     "allowed_origin_echoed",
			settings: CORSSettings{AllowOrigins: []string{"http://a.test"}, ExposeHeaders: []string{"X-Foo"}},
			method:   http.MethodGet, origin: "http://a.test", wantStatus: http.StatusOK,
			wantOrigin: "http://a.test", wantExpose: "X-Foo",
		},
		{
			name:     "other_origin_untouched",
			settings: CORSSettings{AllowOrigins: []string{"http://a.test"}},
			method:   http.MethodGet, origin: "http://evil.test", wantStatus: http.StatusOK,
		},
		{
			name:     "preflight",
			settings: CORSSettings{AllowOrigins: []string{"http://a.test"}, AllowHeaders: []string{"X-Extra"}},
			method:   http.MethodOptions, origin: "http://a.test", reqMethod: http.MethodPut,
			wantStatus: http.StatusNoContent, wantOrigin: "http://a.test", wantHeaders: "X-Extra",
		},
		{
			name:     "wildcard",
			settings: CORSSettings{AllowOrigins: []string{"*"}},
			method:   http.MethodGet, origin: "http://z.test", wantStatus: http.StatusOK, wantOrigin: "*",
		},
		{
			name:     "wildcard_with_credentials_echoes",
			settings: CORSSettings{AllowOrigins: []string{"*"}, AllowCredentials: true},
			method:   http.MethodGet, origin: "http://z.test", wantStatus: http.StatusOK,
			wantOrigin: "http://z.test", wantCreds: "true",
		},
		{
			name:     "disable_checks",
			settings: CORSSettings{DisableChecks: true},
			method:   http.MethodGet, origin: "http://z.test", wantStatus: http.StatusOK, wantOrigin: "*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, _ := startupFlagsServer(t, CLI{CORS: tt.settings})

			req, err := http.NewRequestWithContext(t.Context(), tt.method, srv.URL+"/_gopherstack/health", http.NoBody)
			require.NoError(t, err)
			req.Header.Set("Origin", tt.origin)

			if tt.reqMethod != "" {
				req.Header.Set("Access-Control-Request-Method", tt.reqMethod)
			}

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Equal(t, tt.wantOrigin, resp.Header.Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tt.wantCreds, resp.Header.Get("Access-Control-Allow-Credentials"))
			assert.Equal(t, tt.wantExpose, resp.Header.Get("Access-Control-Expose-Headers"))
			assert.Equal(t, tt.wantHeaders, resp.Header.Get("Access-Control-Allow-Headers"))
		})
	}
}

func TestListenGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		net       NetworkSettings
		container bool
		wantWarn  bool
	}{
		{name: "all_interfaces_warns", wantWarn: true},
		{name: "loopback_ip", net: NetworkSettings{Host: "127.0.0.1"}},
		{name: "loopback_v6", net: NetworkSettings{Host: "::1"}},
		{name: "localhost", net: NetworkSettings{Host: "localhost"}},
		{name: "explicit_wildcard_warns", net: NetworkSettings{Host: "0.0.0.0"}, wantWarn: true},
		{name: "allow_remote_silences", net: NetworkSettings{AllowRemote: true}},
		{name: "container_silences", container: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			log := slog.New(slog.NewTextHandler(&buf, nil))
			got := warnIfRemote(log, tt.net, tt.container)

			assert.Equal(t, tt.wantWarn, got)
			assert.Equal(t, tt.wantWarn, strings.Contains(buf.String(), "non-loopback"))
		})
	}
}

func TestListenAddr(t *testing.T) {
	t.Parallel()

	tests := []struct{ host, port, want, norm string }{
		{"", "8000", ":8000", ":8000"},
		{"", ":8000", ":8000", ":8000"},
		{"127.0.0.1", "8000", "127.0.0.1:8000", "127.0.0.1:8000"},
		{"::1", "8000", "[::1]:8000", "[::1]:8000"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			got := listenAddr(tt.host, tt.port)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.norm, normalizeListenAddr(got))
			assert.Equal(t, ":8000", listenPortSuffix(got))
		})
	}
}

func TestLocalstackServicesReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		in          ServiceFilterSettings
		wantIgnored []string
	}{
		{name: "unknown_reported", in: ServiceFilterSettings{LocalstackServices: []string{"s3", "bogus", "nope"}},
			wantIgnored: []string{"bogus", "nope"}},
		{name: "all_known", in: ServiceFilterSettings{LocalstackServices: []string{"s3", "logs"}}},
		{name: "native_unknown_not_reported", in: ServiceFilterSettings{Services: []string{"s3"},
			LocalstackServices: []string{"bogus"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, ignored, err := newServiceFilterReport(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.wantIgnored, ignored)
			require.NoError(t, (&CLI{ServiceFilter: tt.in}).Validate())
		})
	}
}
