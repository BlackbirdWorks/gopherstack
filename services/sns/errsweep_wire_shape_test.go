package sns_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestSDK_GetTopicAttributes_NonExistentTopic_TypedError drives the real
// aws-sdk-go-v2 sns client (pinned v1.42.4, awsAwsquery/XML protocol) and
// asserts errors.As decodes the specific *types.NotFoundException, not
// merely that an error occurred.
//
// All 42 of sns@v1.42.4/deserializers.go's awsAwsquery_deserializeOpError
// functions call awsxml.GetErrorResponseComponents(errorBody, false) --
// noErrorWrapping=false selects wrappedErrorResponse (Code/Message read from
// the "Error>Code"/"Error>Message" XML path), i.e. the response body must
// carry <ErrorResponse><Error><Code>...</Code><Message>...</Message></Error>
// (aws-sdk-go-v2@v1.43.4/aws/protocol/xml/error_utils.go). A bare
// <Error><Code> root (no wrapping ErrorResponse/Error nesting) would not
// decode into this shape.
func TestSDK_GetTopicAttributes_NonExistentTopic_TypedError(t *testing.T) {
	t.Parallel()

	h := sns.NewHandler(sns.NewInMemoryBackend())
	client := newTestSNSClient(t, h)

	_, err := client.GetTopicAttributes(t.Context(), &snssdk.GetTopicAttributesInput{
		TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:does-not-exist"),
	})
	require.Error(t, err)

	var target *snstypes.NotFoundException
	require.ErrorAs(t, err, &target,
		"expected a real NotFoundException from the SDK deserializer, got %T: %v", err, err)

	// Raw-bytes check: the body must be wrapped (<ErrorResponse><Error><Code>),
	// not the bare <Error><Code> shape some AWS XML APIs (e.g. S3's data
	// plane) use -- a lenient client-side XML decode can tolerate a root
	// mismatch and still resolve Code/Message via path matching, masking a
	// shape bug that a stricter client (or botocore) would reject.
	rawURL, cleanup := rawSNSTestServer(t)
	defer cleanup()

	body := rawSNSErrorBody(t, rawURL, "GetTopicAttributes",
		"TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3Adoes-not-exist")

	require.Contains(t, string(body), "<ErrorResponse", "root must be the wrapped ErrorResponse shape: %s", body)
	require.Contains(t, string(body), "<Error>", "Code/Message must be nested under <Error>: %s", body)
	require.Contains(t, string(body), "<Code>NotFound</Code>", "raw body: %s", body)
}

func rawSNSTestServer(t *testing.T) (string, func()) {
	t.Helper()

	h := sns.NewHandler(sns.NewInMemoryBackend())
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)

	return srv.URL, srv.Close
}

// rawSNSErrorBody sends a raw form-urlencoded Query-protocol POST matching
// what the pinned SDK sends, and returns the raw response bytes, bypassing
// SDK-side decoding entirely.
func rawSNSErrorBody(t *testing.T, url, action, formBody string) []byte {
	t.Helper()

	body := "Action=" + action + "&Version=2010-03-31&" + formBody

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, url+"/", bytes.NewReader([]byte(body)),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "aws-sdk-go2/1.30.0 api/sns#1.42.4")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.NotEqual(t, http.StatusOK, resp.StatusCode)

	return respBody
}
