package awserr_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func TestClassifyMatchesWrappedSentinel(t *testing.T) {
	t.Parallel()

	table := map[error]awserr.APIError{
		awserr.ErrNotFound:      {Code: "ResourceNotFoundException", Message: "not found", HTTPStatus: 404},
		awserr.ErrAlreadyExists: {Code: "ResourceInUseException", Message: "exists", HTTPStatus: 409},
	}
	fallback := awserr.APIError{Code: "InternalFailure", Message: "boom", HTTPStatus: 500}

	got := awserr.Classify(awserr.New("table missing", awserr.ErrNotFound), table, fallback)
	assert.Equal(t, "ResourceNotFoundException", got.Code)
	assert.Equal(t, 404, got.HTTPStatus)
	// The concrete message overrides the table default.
	assert.Equal(t, "table missing", got.Message)
}

func TestClassifyFallback(t *testing.T) {
	t.Parallel()

	fallback := awserr.APIError{Code: "InternalFailure", Message: "boom", HTTPStatus: 500}

	assert.Equal(t, fallback, awserr.Classify(nil, nil, fallback))
	assert.Equal(t, fallback, awserr.Classify(awserr.New("x", awserr.ErrConflict), nil, fallback))
}

func newCtx(t *testing.T) (*echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amzn-Requestid", "req-123")
	rec := httptest.NewRecorder()

	return e.NewContext(req, rec), rec
}

func TestWriteJSON10(t *testing.T) {
	t.Parallel()

	c, rec := newCtx(t)
	err := awserr.Write(c, awserr.ProtocolJSON10, awserr.APIError{
		Code: "ResourceNotFoundException", Message: "nope", HTTPStatus: 400,
	})
	require.NoError(t, err)

	assert.Equal(t, 400, rec.Code)
	assert.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Header().Get("X-Amz-Crc32"))

	var env struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "ResourceNotFoundException", env.Type)
	assert.Equal(t, "nope", env.Message)
}

func TestWriteJSON11ContentType(t *testing.T) {
	t.Parallel()

	c, rec := newCtx(t)
	require.NoError(t, awserr.Write(c, awserr.ProtocolJSON11, awserr.APIError{
		Code: "ValidationException", Message: "bad", HTTPStatus: 400,
	}))
	assert.Equal(t, "application/x-amz-json-1.1", rec.Header().Get("Content-Type"))
}

func TestWriteRestXML(t *testing.T) {
	t.Parallel()

	c, rec := newCtx(t)
	require.NoError(t, awserr.Write(c, awserr.ProtocolRestXML, awserr.APIError{
		Code: "NoSuchBucket", Message: "missing", HTTPStatus: 404,
	}))

	assert.Equal(t, 404, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "xml")

	var env struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		RequestID string   `xml:"RequestId"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "NoSuchBucket", env.Code)
	assert.Equal(t, "missing", env.Message)
	assert.Equal(t, "req-123", env.RequestID)
}

func TestWriteQueryXML(t *testing.T) {
	t.Parallel()

	c, rec := newCtx(t)
	require.NoError(t, awserr.Write(c, awserr.ProtocolQueryXML, awserr.APIError{
		Code: "InvalidParameterValue", Message: "bad param", HTTPStatus: 400,
	}))

	var env struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Type    string   `xml:"Error>Type"`
		Code    string   `xml:"Error>Code"`
		Message string   `xml:"Error>Message"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "Sender", env.Type)
	assert.Equal(t, "InvalidParameterValue", env.Code)
	assert.Equal(t, "bad param", env.Message)
}

func TestAPIErrorImplementsError(t *testing.T) {
	t.Parallel()

	var err error = awserr.APIError{Code: "X", Message: "y", HTTPStatus: 400}
	assert.Equal(t, "X: y", err.Error())
}
