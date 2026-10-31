package app

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestS3SnapshotOptionEnv(t *testing.T) {
	tests := []struct {
		env   map[string]string
		check func(t *testing.T, c CLI)
		name  string
	}{
		{
			name: "defaults",
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.False(t, c.S3.SkipKMSKeyValidation)
				assert.False(t, c.S3.ValidateSignatures)
				assert.False(t, c.S3.DisableCustomCORS)
				assert.False(t, c.CORS.DisableAPIGWCORS)
				assert.Equal(t, "startup", c.PersistModes.PersistLoad)
			},
		},
		{
			name: "localstack aliases",
			env: map[string]string{
				"S3_SKIP_KMS_KEY_VALIDATION":     "1",
				"S3_VALIDATE_SIGNATURES":         "1",
				"DISABLE_CUSTOM_CORS_S3":         "1",
				"DISABLE_CUSTOM_CORS_APIGATEWAY": "1",
			},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.True(t, c.S3.SkipKMSKeyValidation)
				assert.True(t, c.S3.ValidateSignatures)
				assert.True(t, c.S3.DisableCustomCORS)
				assert.True(t, c.CORS.DisableAPIGWCORS)
			},
		},
		{
			name: "native env wins over alias",
			env: map[string]string{
				"GOPHERSTACK_S3_SKIP_KMS_KEY_VALIDATION": "false",
				"S3_SKIP_KMS_KEY_VALIDATION":             "1",
			},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.False(t, c.S3.SkipKMSKeyValidation)
			},
		},
		{
			name: "persist strategies",
			env:  map[string]string{"GOPHERSTACK_PERSIST_MODE": "manual", "GOPHERSTACK_PERSIST_LOAD": "request"},
			check: func(t *testing.T, c CLI) {
				t.Helper()
				assert.Equal(t, "manual", c.PersistModes.PersistMode)
				assert.Equal(t, "request", c.PersistModes.PersistLoad)
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

func TestPersistLoadStrategyThroughMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		load         string
		wantStartup  string
		wantAfterGet string
	}{
		{name: "startup", load: "startup", wantStartup: "1", wantAfterGet: "1"},
		{name: "request", load: "request", wantStartup: "", wantAfterGet: "1"},
		{name: "manual", load: "manual", wantStartup: "", wantAfterGet: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			seed := bootPersist(t, dir, PersistModeSettings{PersistMode: "snapshot"}, "kv", "other")
			seed.post(t, "/kv/a", "1")
			seed.m.SaveAll(t.Context())
			seed.crash()

			h := bootPersist(t, dir, PersistModeSettings{PersistMode: "snapshot", PersistLoad: tt.load}, "kv", "other")
			assert.Equal(t, tt.wantStartup, h.kvs[0].getA(), "after startup")

			h.post(t, "/other/z", "x")
			assert.Equal(t, tt.wantStartup, h.kvs[0].getA(), "other service request leaves kv unloaded")

			h.post(t, "/kv/b", "y")
			assert.Equal(t, tt.wantAfterGet, h.kvs[0].getA(), "after first kv request")
			h.crash()
		})
	}
}

func TestPersistManualEndpoints(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := PersistModeSettings{PersistMode: "manual", PersistLoad: "manual"}

	h := bootPersist(t, dir, s, "kv")
	h.post(t, "/kv/a", "1")
	h.rt.stop(t.Context())

	h2 := bootPersist(t, dir, s, "kv")
	assert.Empty(t, h2.kvs[0].getA(), "manual save mode never saved at shutdown")

	h2.post(t, "/kv/a", "7")

	save := httptest.NewRecorder()
	buildPersistSaveHandler(h2.m)(echoCtx(t, h2, save, "/_gopherstack/persist/save"))
	assert.Equal(t, http.StatusOK, save.Code)
	assert.JSONEq(t, `{"status":"ok","saved":1}`, save.Body.String())

	h3 := bootPersist(t, dir, s, "kv")
	assert.Empty(t, h3.kvs[0].getA(), "manual load mode does not load at startup")

	load := httptest.NewRecorder()
	buildPersistLoadHandler(h3.m)(echoCtx(t, h3, load, "/_gopherstack/persist/load"))
	assert.Equal(t, http.StatusOK, load.Code)

	var out map[string]any

	require.NoError(t, json.Unmarshal(load.Body.Bytes(), &out))
	assert.InDelta(t, 1, out["loaded"], 0)
	assert.Equal(t, "7", h3.kvs[0].getA())
}

func echoCtx(t *testing.T, h *persistHarness, rec *httptest.ResponseRecorder, path string) *echo.Context {
	t.Helper()

	return h.e.NewContext(httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil), rec)
}

type optionsFixture struct {
	cfg aws.Config
}

func newOptionsFixture(t *testing.T, mutate func(*CLI)) *optionsFixture {
	t.Helper()

	return newWrappedOptionsFixture(t, mutate, nil)
}

func newWrappedOptionsFixture(
	t *testing.T, mutate func(*CLI), wrap func(http.Handler) http.Handler,
) *optionsFixture {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
	mutate(&cli)
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	var h http.Handler = e
	if wrap != nil {
		h = wrap(h)
	}

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	addr := srv.Listener.Addr().String()
	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
		awscfg.WithHTTPClient(&http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, network, addr)
			},
		}}),
	)
	require.NoError(t, err)

	return &optionsFixture{cfg: cfg}
}

func TestS3SSEKMSKeyValidationEndToEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		skip    bool
		useReal bool
	}{
		{name: "real key accepted", useReal: true},
		{name: "unknown key rejected", wantErr: "KMS.NotFoundException"},
		{name: "unknown key accepted when skipped", skip: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newOptionsFixture(t, func(c *CLI) { c.S3.SkipKMSKeyValidation = tt.skip })
			kc := kms.NewFromConfig(fx.cfg)
			sc := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })

			key, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			_, err = sc.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("kms-bkt")})
			require.NoError(t, err)

			keyID := "arn:aws:kms:us-east-1:000000000000:key/does-not-exist"
			if tt.useReal {
				keyID = aws.ToString(key.KeyMetadata.Arn)
			}

			_, err = sc.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket:               aws.String("kms-bkt"),
				Key:                  aws.String("obj"),
				Body:                 strings.NewReader("data"),
				ServerSideEncryption: s3types.ServerSideEncryptionAwsKms,
				SSEKMSKeyId:          aws.String(keyID),
			})

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
		})
	}
}
