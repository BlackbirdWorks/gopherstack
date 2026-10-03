package cloudwatch_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

const cwPutMetricDataPath = "/service/GraniteServiceVersion20100801/operation/PutMetricData"

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(b)
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func newCompressionServer(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()

	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))

	encodings := &[]string{}
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			*encodings = append(*encodings, c.Request().Header.Get("Content-Encoding"))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv, encodings
}

func TestPutMetricData_RequestCompression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		count       int
		disable     bool
		wantGzipped bool
	}{
		{name: "below threshold", count: 5, wantGzipped: false},
		{name: "above threshold gzipped", count: 400, wantGzipped: true},
		{name: "above threshold compression disabled", count: 400, disable: true, wantGzipped: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, encodings := newCompressionServer(t)
			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
			)
			require.NoError(t, err)

			client := cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) {
				o.BaseEndpoint = aws.String(srv.URL)
				o.DisableRequestCompression = tt.disable
			})

			data := make([]types.MetricDatum, 0, tt.count)
			for i := range tt.count {
				data = append(data, types.MetricDatum{
					MetricName: aws.String(fmt.Sprintf("metric-%04d-with-a-reasonably-long-name", i)),
					Value:      aws.Float64(float64(i)),
					Unit:       types.StandardUnitCount,
				})
			}

			_, err = client.PutMetricData(t.Context(), &cwsdk.PutMetricDataInput{
				Namespace: aws.String("Compress/Test"), MetricData: data,
			})
			require.NoError(t, err)

			if tt.wantGzipped {
				assert.Contains(t, *encodings, "gzip")
			} else {
				assert.NotContains(t, *encodings, "gzip")
			}

			var names []string
			p := cwsdk.NewListMetricsPaginator(client, &cwsdk.ListMetricsInput{Namespace: aws.String("Compress/Test")})
			for p.HasMorePages() {
				page, pErr := p.NextPage(t.Context())
				require.NoError(t, pErr)
				for _, m := range page.Metrics {
					names = append(names, aws.ToString(m.MetricName))
				}
			}
			assert.Len(t, names, tt.count)
		})
	}
}

func TestPutMetricData_RawEncodings(t *testing.T) {
	t.Parallel()

	valid := cbor.Encode(cbor.Map{
		"Namespace": cbor.String("Raw/Test"),
		"MetricData": cbor.List{cbor.Map{
			"MetricName": cbor.String("m"), "Value": cbor.Float64(1),
		}},
	})
	oversized := gzipBytes(t, bytes.Repeat([]byte{0}, 2<<20))

	tests := []struct {
		name     string
		encoding string
		body     []byte
		wantCode int
	}{
		{name: "valid gzip", encoding: "gzip", body: gzipBytes(t, valid), wantCode: http.StatusOK},
		{name: "uppercase gzip token", encoding: "GZIP", body: gzipBytes(t, valid), wantCode: http.StatusOK},
		{name: "identity", encoding: "identity", body: valid, wantCode: http.StatusOK},
		{name: "oversized decompressed", encoding: "gzip", body: oversized, wantCode: http.StatusBadRequest},
		{name: "corrupt gzip", encoding: "gzip", body: []byte("not gzip at all"), wantCode: http.StatusBadRequest},
		{name: "truncated gzip", encoding: "gzip", body: gzipBytes(t, valid)[:12], wantCode: http.StatusBadRequest},
		{name: "unknown encoding", encoding: "br", body: valid, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, _ := newCompressionServer(t)
			req, err := http.NewRequestWithContext(
				t.Context(), http.MethodPost, srv.URL+cwPutMetricDataPath, bytes.NewReader(tt.body),
			)
			require.NoError(t, err)
			req.Header.Set("Smithy-Protocol", "rpc-v2-cbor")
			req.Header.Set("Content-Type", "application/cbor")
			req.Header.Set("Content-Encoding", tt.encoding)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestPutMetricData_FormGzipRoutes(t *testing.T) {
	t.Parallel()

	form := "Action=PutMetricData&Version=2010-08-01&Namespace=Form%2FTest" +
		"&MetricData.member.1.MetricName=m&MetricData.member.1.Value=1"

	tests := []struct {
		name     string
		encoding string
		body     []byte
		wantCode int
	}{
		{name: "gzip form", encoding: "gzip", body: gzipBytes(t, []byte(form)), wantCode: http.StatusOK},
		{name: "plain form", body: []byte(form), wantCode: http.StatusOK},
		{name: "corrupt gzip form", encoding: "gzip", body: []byte("garbage"), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, _ := newCompressionServer(t)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/", bytes.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("User-Agent", "aws-sdk-go2/1.30.0 api/cloudwatch#1.66.3")

			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}
