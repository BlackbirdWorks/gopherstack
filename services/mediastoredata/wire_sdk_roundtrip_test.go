//go:build !integration

package mediastoredata_test

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	msdsdk "github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	msdtypes "github.com/aws/aws-sdk-go-v2/service/mediastoredata/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

// newTestMSDSDKClient stands up the real aws-sdk-go-v2 mediastoredata client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- so every shape
// below is verified by the real client's own deserializer (headers, status
// codes, JSON body), not gopherstack's own header-writing code.
func newTestMSDSDKClient(t *testing.T, h *mediastoredata.Handler) *msdsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return msdsdk.NewFromConfig(cfg, func(o *msdsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestPutGetObject_MultiSegmentPath_SDKRoundTrip proves the greedy
// "/{Path+}" URI label (serializers.go's SplitURI("/{Path+}") for
// PutObject/GetObject) survives gopherstack's own router, which has no Echo
// route pattern at all -- pkgs/service.Router matches on User-Agent and
// dispatches on raw r.URL.Path (router.go), so a multi-segment path like
// "premium/canada/mlaw.avi" must round-trip byte-for-byte through both the
// PUT and the GET.
func TestPutGetObject_MultiSegmentPath_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mediastoredata.NewInMemoryBackend("us-east-1")
	h := mediastoredata.NewHandler(backend)
	client := newTestMSDSDKClient(t, h)

	const path = "premium/canada/mlaw.avi"

	body := []byte("multi-segment-path-body")

	putOut, err := client.PutObject(t.Context(), &msdsdk.PutObjectInput{
		Path:        aws.String(path),
		Body:        bytesReader(body),
		ContentType: aws.String("video/x-msvideo"),
	})
	require.NoError(t, err)
	assert.Equal(t, msdtypes.StorageClassTemporal, putOut.StorageClass,
		"StorageClass defaults to the real SDK's sole enum member TEMPORAL")
	require.NotNil(t, putOut.ETag)
	require.NotNil(t, putOut.ContentSHA256)

	getOut, err := client.GetObject(t.Context(), &msdsdk.GetObjectInput{Path: aws.String(path)})
	require.NoError(t, err)

	defer getOut.Body.Close()

	gotBody, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	assert.Equal(
		t,
		body,
		gotBody,
		"multi-segment path must resolve to the exact object stored under it",
	)
	assert.EqualValues(
		t,
		200,
		getOut.StatusCode,
		"a non-ranged GetObject must report HTTP 200, not 206",
	)
	assert.Equal(t, "video/x-msvideo", aws.ToString(getOut.ContentType))
	assert.Equal(t, aws.ToString(putOut.ETag), aws.ToString(getOut.ETag))
	require.NotNil(t, getOut.ContentLength)
	assert.EqualValues(t, len(body), *getOut.ContentLength)
}

// TestGetObject_Range_SDKRoundTrip proves a ranged GetObject reports the real
// 206 Partial Content status (GetObjectOutput.StatusCode is populated
// straight from response.StatusCode in deserializers.go, not hardcoded) and
// carries a Content-Range header, and that an unsatisfiable range surfaces as
// the real modeled types.RequestedRangeNotSatisfiableException, not a
// fabricated exception name or a bare HTTP 416 with no typed error.
func TestGetObject_Range_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mediastoredata.NewInMemoryBackend("us-east-1")
	h := mediastoredata.NewHandler(backend)
	client := newTestMSDSDKClient(t, h)

	const path = "range-object"

	body := []byte("0123456789")

	_, err := client.PutObject(t.Context(), &msdsdk.PutObjectInput{
		Path: aws.String(path),
		Body: bytesReader(body),
	})
	require.NoError(t, err)

	rangeOut, err := client.GetObject(t.Context(), &msdsdk.GetObjectInput{
		Path:  aws.String(path),
		Range: aws.String("bytes=2-4"),
	})
	require.NoError(t, err)

	defer rangeOut.Body.Close()

	gotRange, err := io.ReadAll(rangeOut.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("234"), gotRange)
	assert.EqualValues(
		t,
		206,
		rangeOut.StatusCode,
		"a satisfiable ranged GetObject must report HTTP 206",
	)
	assert.Equal(t, "bytes 2-4/10", aws.ToString(rangeOut.ContentRange))

	_, err = client.GetObject(t.Context(), &msdsdk.GetObjectInput{
		Path:  aws.String(path),
		Range: aws.String("bytes=100-200"),
	})
	require.Error(t, err)

	var rangeErr *msdtypes.RequestedRangeNotSatisfiableException
	require.ErrorAs(t, err, &rangeErr,
		"an unsatisfiable range must deserialize into the real typed "+
			"RequestedRangeNotSatisfiableException, not a fabricated __type or a generic API error")
}

// TestDescribeObject_SDKRoundTrip proves DescribeObject's headers-only
// response (no HTTP body, no StatusCode field on DescribeObjectOutput unlike
// GetObjectOutput) round-trips through the real client's HEAD deserializer.
func TestDescribeObject_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mediastoredata.NewInMemoryBackend("us-east-1")
	h := mediastoredata.NewHandler(backend)
	client := newTestMSDSDKClient(t, h)

	const path = "describe-object"

	body := []byte("describe-me")

	putOut, err := client.PutObject(t.Context(), &msdsdk.PutObjectInput{
		Path:        aws.String(path),
		Body:        bytesReader(body),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeObject(
		t.Context(),
		&msdsdk.DescribeObjectInput{Path: aws.String(path)},
	)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", aws.ToString(descOut.ContentType))
	assert.Equal(t, aws.ToString(putOut.ETag), aws.ToString(descOut.ETag))
	require.NotNil(t, descOut.ContentLength)
	assert.EqualValues(t, len(body), *descOut.ContentLength)
	require.NotNil(t, descOut.LastModified)

	_, err = client.DescribeObject(
		t.Context(),
		&msdsdk.DescribeObjectInput{Path: aws.String("missing")},
	)
	require.Error(t, err)

	var notFound *msdtypes.ObjectNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestListItems_SDKRoundTrip proves ListItems' JSON body (Items[].{Name,
// Type, ContentLength, ContentType, ETag, LastModified}, NextToken) matches
// deserializeDocumentItem field-for-field, including LastModified as an
// epoch-seconds JSON number (smithytime.ParseEpochSeconds) and Type as the
// real ItemType enum (OBJECT/FOLDER), and that ListItems is always GET "/"
// with Path/MaxResults/NextToken as query params -- never a GET on the
// folder path itself.
func TestListItems_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mediastoredata.NewInMemoryBackend("us-east-1")
	h := mediastoredata.NewHandler(backend)
	client := newTestMSDSDKClient(t, h)

	for _, p := range []string{"movies/mlaw.avi", "movies/premium/other.mov"} {
		_, err := client.PutObject(t.Context(), &msdsdk.PutObjectInput{
			Path: aws.String(p),
			Body: bytesReader([]byte("x")),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListItems(
		t.Context(),
		&msdsdk.ListItemsInput{Path: aws.String("movies")},
	)
	require.NoError(t, err)
	require.Len(t, listOut.Items, 2, "one direct object, one deduped folder entry")

	byName := make(map[string]msdtypes.Item, len(listOut.Items))
	for _, item := range listOut.Items {
		byName[aws.ToString(item.Name)] = item
	}

	obj, ok := byName["mlaw.avi"]
	require.True(t, ok, "expected the direct object entry")
	assert.Equal(t, msdtypes.ItemTypeObject, obj.Type)
	require.NotNil(t, obj.LastModified)
	require.NotNil(t, obj.ContentLength)
	assert.EqualValues(t, 1, *obj.ContentLength)

	folder, ok := byName["premium"]
	require.True(t, ok, "expected the deduped folder entry")
	assert.Equal(t, msdtypes.ItemTypeFolder, folder.Type)
}

func bytesReader(b []byte) io.Reader { return &staticReader{b: b} }

// staticReader is a minimal io.Reader (not io.ReadSeeker) so the SDK's
// unsigned-payload PutObject path (addUnsignedPayload in
// addOperationPutObjectMiddlewares) is exercised the same way a real
// streaming caller would use it.
type staticReader struct {
	b   []byte
	pos int
}

func (r *staticReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}

	n := copy(p, r.b[r.pos:])
	r.pos += n

	return n, nil
}
