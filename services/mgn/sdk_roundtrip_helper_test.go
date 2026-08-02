package mgn_test

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	mgntypes "github.com/aws/aws-sdk-go-v2/service/mgn/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mgn"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// defaultAsyncWait/defaultAsyncPoll bound require.Eventually calls polling
// this service's async state machines. This service's longest chain (a
// StartImport-created SourceServer's 2-tick import completion PLUS its own
// separate 3-tick replication progression to READY_FOR_TEST) is
// 5*asyncTransitionDelay = 500ms; 5s is generous relative to that for CI
// jitter, matching services/resiliencehub/outposts/grafana's identical
// rationale for their own defaultAsyncWait.
const (
	defaultAsyncWait = 5 * time.Second
	defaultAsyncPoll = 20 * time.Millisecond
)

// newRoundTripClient stands up the real aws-sdk-go-v2 mgn client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- including the
// RouteMatcher and MatchPriority a unit test calling h.Handler()(c) directly
// would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response
// are wire-compatible: this service's lowerCamel JSON members (with
// embedded-acronym casing like "sourceServerID"), its literal PascalCase
// action paths (and the /network-migration/ prefix on 25 of them), its
// percent-encoded ARN-in-path /tags/{resourceArn} route, and its
// epoch-seconds vs. RFC3339-string dual timestamp convention all look fine
// to ad-hoc JSON assertions but fail against the real client if the wire
// shape is wrong -- matching services/outposts/resiliencehub/grafana's
// identical rationale for this helper.
func newRoundTripClient(t *testing.T, h *mgn.Handler) *mgnsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mgnsdk.NewFromConfig(cfg, func(o *mgnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
// The backend is pre-initialized (InitializeService) since 69 of 95
// operations return UninitializedAccountException otherwise (PARITY.md) --
// tests exercising that gate explicitly call a fresh, non-initialized
// backend instead.
func newTestHandlerAndClient(t *testing.T) (*mgn.Handler, *mgnsdk.Client) {
	t.Helper()

	backend := mgn.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	backend.InitializeService()

	h := mgn.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return h, client
}

// mockS3 is a minimal, in-memory mgn.S3Accessor for exercising StartImport's
// real S3-read path without needing services/s3's full in-memory backend --
// same pattern services/dynamodb's own import_export_s3_test.go uses for
// its identical S3Accessor seam.
type mockS3 struct {
	objects map[string][]byte
}

func newMockS3() *mockS3 { return &mockS3{objects: map[string][]byte{}} }

func (m *mockS3) put(bucket, key, body string) { m.objects[bucket+"/"+key] = []byte(body) }

func (m *mockS3) GetObject(_ context.Context, in *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, error) {
	data, ok := m.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}

	return &s3sdk.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))}, nil
}

// seedSourceServerViaImport drives the real, wire-reachable StartImport
// path (s3import.go) to create a single SourceServer with the given
// hostname, replacing this package's former SeedSourceServer non-SDK test
// convenience (removed once StartImport itself became a genuine creation
// path -- see sourceservers.go's package doc comment). It waits for the
// ImportTask to reach SUCCEEDED, then returns the one SourceServer
// DescribeSourceServers reports -- callers use a fresh backend per test, so
// a single-row import always yields exactly one result.
func seedSourceServerViaImport(
	t *testing.T, h *mgn.Handler, client *mgnsdk.Client, hostname string,
) mgntypes.SourceServer {
	t.Helper()

	ctx := t.Context()

	s3 := newMockS3()
	s3.put("mgn-import-bucket", "servers.csv", "hostname\n"+hostname+"\n")
	h.Backend.SetS3Backend(s3)

	_, err := client.StartImport(ctx, &mgnsdk.StartImportInput{
		S3BucketSource: &mgntypes.S3BucketSource{
			S3Bucket: aws.String("mgn-import-bucket"), S3Key: aws.String("servers.csv"),
		},
	})
	require.NoError(t, err)

	var created mgntypes.SourceServer

	require.Eventually(t, func() bool {
		out, describeErr := client.DescribeSourceServers(ctx, &mgnsdk.DescribeSourceServersInput{})
		if describeErr != nil || len(out.Items) == 0 {
			return false
		}

		created = out.Items[0]

		return true
	}, defaultAsyncWait, defaultAsyncPoll, "StartImport never created a source server")

	return created
}
