package main

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/klauspost/compress/gzip"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type compressionServers struct {
	on, off *echo.Echo
}

//nolint:gochecknoglobals // full production composition built once for the compression tests
var (
	compressionOnce sync.Once
	compressionSrv  compressionServers
	errCompression  error
)

func buildCompressionServer(compression string) (*echo.Echo, error) {
	log := buildLogger("")
	ctx := context.Background()

	cli := CLI{AccountID: "000000000000", Region: "us-east-1", Compression: compression}
	cli.portAlloc = setupPortAllocatorWithReservations(ctx, log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(&service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: ctx, PortAlloc: cli.portAlloc,
	})
	if err != nil {
		return nil, err
	}

	e := buildEchoServer(ctx, log, nil, services, cli)

	return e, setupChaosAndRegistry(e, log, &cli, services)
}

func compressionFixture(t *testing.T) compressionServers {
	t.Helper()

	compressionOnce.Do(func() {
		var err error

		if compressionSrv.on, err = buildCompressionServer("on"); err != nil {
			errCompression = err

			return
		}

		compressionSrv.off, errCompression = buildCompressionServer("off")
	})

	require.NoError(t, errCompression)

	return compressionSrv
}

type echoTransport struct {
	e    *echo.Echo
	seen []string
	mu   sync.Mutex
}

func (rt *echoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	rt.e.ServeHTTP(rec, req)

	rt.mu.Lock()
	rt.seen = append(rt.seen, rec.Header().Get("Content-Encoding"))
	rt.mu.Unlock()

	return rec.Result(), nil
}

func (rt *echoTransport) encodings() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	return append([]string(nil), rt.seen...)
}

func ddbClient(t *testing.T, e *echo.Echo, gzipOptIn bool) (*sdkdynamodb.Client, *echoTransport) {
	t.Helper()

	rt := &echoTransport{e: e}

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(&http.Client{Transport: rt}),
	)
	require.NoError(t, err)

	return sdkdynamodb.NewFromConfig(cfg, func(o *sdkdynamodb.Options) {
		o.BaseEndpoint = aws.String("http://gopherstack.test")
		o.EnableAcceptEncodingGzip = gzipOptIn
	}), rt
}

func seedCompressionTable(t *testing.T, c *sdkdynamodb.Client, table string) string {
	t.Helper()

	ctx := t.Context()
	payload := strings.Repeat("gopherstack compresses repetitive item payloads. ", 120)

	_, err := c.CreateTable(ctx, &sdkdynamodb.CreateTableInput{
		TableName: aws.String(table),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = c.PutItem(ctx, &sdkdynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbtypes.AttributeValue{
			"id":   &ddbtypes.AttributeValueMemberS{Value: "k"},
			"body": &ddbtypes.AttributeValueMemberS{Value: payload},
		},
	})
	require.NoError(t, err)

	return payload
}

func TestCompressionDynamoDBSDK(t *testing.T) {
	t.Parallel()

	srv := compressionFixture(t)

	t.Run("gzip opt-in with CRC validation", func(t *testing.T) {
		t.Parallel()

		c, rt := ddbClient(t, srv.on, true)
		payload := seedCompressionTable(t, c, "gz-table")
		ctx := t.Context()

		get, err := c.GetItem(ctx, &sdkdynamodb.GetItemInput{
			TableName: aws.String("gz-table"),
			Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "k"}},
		})
		require.NoError(t, err)
		assert.Equal(t, payload, get.Item["body"].(*ddbtypes.AttributeValueMemberS).Value)

		q, err := c.Query(ctx, &sdkdynamodb.QueryInput{
			TableName:              aws.String("gz-table"),
			KeyConditionExpression: aws.String("id = :k"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":k": &ddbtypes.AttributeValueMemberS{Value: "k"},
			},
		})
		require.NoError(t, err)
		require.Len(t, q.Items, 1)
		assert.Equal(t, payload, q.Items[0]["body"].(*ddbtypes.AttributeValueMemberS).Value)

		scan, err := c.Scan(ctx, &sdkdynamodb.ScanInput{TableName: aws.String("gz-table")})
		require.NoError(t, err)
		require.Len(t, scan.Items, 1)
		assert.Equal(t, payload, scan.Items[0]["body"].(*ddbtypes.AttributeValueMemberS).Value)

		seen := rt.encodings()
		require.GreaterOrEqual(t, len(seen), 5)
		assert.Equal(t, []string{"gzip", "gzip", "gzip"}, seen[len(seen)-3:])
		assert.Empty(t, seen[0], "small CreateTable response stays identity")
	})

	t.Run("default client is byte-identical to compression off", func(t *testing.T) {
		t.Parallel()

		onC, onRT := ddbClient(t, srv.on, false)
		offC, _ := ddbClient(t, srv.off, false)
		seedCompressionTable(t, onC, "plain-table")
		seedCompressionTable(t, offC, "plain-table")

		for _, target := range []string{"GetItem", "Scan"} {
			body := `{"TableName":"plain-table","Key":{"id":{"S":"k"}}}`
			if target == "Scan" {
				body = `{"TableName":"plain-table"}`
			}

			on := rawDDB(srv.on, target, body, "identity")
			off := rawDDB(srv.off, target, body, "identity")

			off.Header().Del("X-Amz-Request-Id")
			on.Header().Del("X-Amz-Request-Id")

			assert.Equal(t, off.Header(), on.Header(), target)
			assert.Equal(t, off.Body.Bytes(), on.Body.Bytes(), target)
			assert.Empty(t, on.Header().Get("Content-Encoding"))
		}

		for _, enc := range onRT.encodings() {
			assert.Empty(t, enc)
		}
	})

	t.Run("crc32 covers compressed bytes", func(t *testing.T) {
		t.Parallel()

		c, _ := ddbClient(t, srv.on, true)
		seedCompressionTable(t, c, "crc-table")

		rec := rawDDB(srv.on, "Scan", `{"TableName":"crc-table"}`, "gzip")
		require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
		assert.Equal(
			t,
			strconv.FormatUint(uint64(crc32.ChecksumIEEE(rec.Body.Bytes())), 10),
			rec.Header().Get("X-Amz-Crc32"),
		)

		zr, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
		require.NoError(t, err)

		plain, err := io.ReadAll(zr)
		require.NoError(t, err)
		assert.Contains(t, string(plain), `"Items"`)
	})
}

func rawDDB(e *echo.Echo, target, body, ae string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", "dynamodb", 1))
	req.Header.Set("X-Amz-Date", "20260101T000000Z")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+target)
	req.Header.Set("Accept-Encoding", ae)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func getRaw(e *echo.Echo, path, ae string, extra map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if ae != "" {
		req.Header.Set("Accept-Encoding", ae)
	}

	for k, v := range extra {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestCompressionDashboardAndAWSUnchanged(t *testing.T) {
	t.Parallel()

	srv := compressionFixture(t)

	t.Run("dashboard static br", func(t *testing.T) {
		t.Parallel()

		off := getRaw(srv.off, "/dashboard/static/app.js", "", nil)
		require.Equal(t, http.StatusOK, off.Code)

		on := getRaw(srv.on, "/dashboard/static/app.js", "br, gzip", nil)
		assert.Equal(t, "br", on.Header().Get("Content-Encoding"))
		assert.Contains(t, on.Header().Values("Vary"), "Accept-Encoding")
		assert.Less(t, on.Body.Len(), off.Body.Len())

		plain, err := io.ReadAll(brotli.NewReader(bytes.NewReader(on.Body.Bytes())))
		require.NoError(t, err)
		assert.Equal(t, off.Body.Bytes(), plain)

		again := getRaw(srv.on, "/dashboard/static/app.js", "br", nil)
		assert.Equal(t, on.Body.Bytes(), again.Body.Bytes())
	})

	t.Run("compression off ignores accept-encoding", func(t *testing.T) {
		t.Parallel()

		rec := getRaw(srv.off, "/dashboard/static/app.js", "br, gzip, zstd", nil)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))

		dd := rawDDB(srv.off, "ListTables", `{}`, "gzip")
		assert.Empty(t, dd.Header().Get("Content-Encoding"))
	})

	t.Run("dashboard without accept-encoding unchanged", func(t *testing.T) {
		t.Parallel()

		paths := []string{"/dashboard/static/app.js", "/dashboard/static/styles.css", "/dashboard/api/system/state"}

		for _, p := range paths {
			off := getRaw(srv.off, p, "", nil)
			on := getRaw(srv.on, p, "", nil)

			assert.Equal(t, off.Code, on.Code, p)
			assert.Empty(t, on.Header().Get("Content-Encoding"), p)
			assert.Equal(t, off.Body.Bytes(), on.Body.Bytes(), p)
		}
	})

	t.Run("dashboard range request not compressed", func(t *testing.T) {
		t.Parallel()

		rec := getRaw(srv.on, "/dashboard/static/app.js", "gzip", map[string]string{"Range": "bytes=0-9"})
		assert.Equal(t, http.StatusPartialContent, rec.Code)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, 10, rec.Body.Len())
	})

	t.Run("other aws services never compress", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct{ svc, body string }{
			{"sts", "Action=GetCallerIdentity&Version=2011-06-15"},
			{"sqs", "Action=ListQueues&Version=2012-11-05"},
			{"sns", "Action=ListTopics&Version=2010-03-31"},
		} {
			for _, e := range []*echo.Echo{srv.on, srv.off} {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
				req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", tc.svc, 1))
				req.Header.Set("X-Amz-Date", "20260101T000000Z")
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.Header.Set("Accept-Encoding", "gzip, br, zstd")

				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, req)
				assert.Empty(t, rec.Header().Get("Content-Encoding"), tc.svc)
				assert.Empty(t, rec.Header().Get("Vary"), tc.svc)
			}
		}
	})
}
