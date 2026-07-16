package sns_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

// errReadErr is the sentinel error returned by errReader.
var errRead = errors.New("read error")

// errBackendErr is the sentinel error returned by errBackend.
var errBackend2 = errors.New("unexpected internal backend failure")

// errReader is a helper that always fails on read.
type errReader struct{}

func (errReader) Read(_ []byte) (int, error) { return 0, errRead }

func (errReader) Close() error { return nil }

// errBackend wraps InMemoryBackend and overrides CreateTopic/CreateTopicInRegion to return a custom error.
type errBackend struct {
	*sns.InMemoryBackend
}

func (b *errBackend) CreateTopic(_ string, _ map[string]string) (*sns.Topic, error) {
	return nil, errBackend2
}

func (b *errBackend) CreateTopicInRegion(_ string, _ string, _ map[string]string) (*sns.Topic, error) {
	return nil, errBackend2
}

// newErrContext builds an echo context whose request body always fails to read.
func newErrContext() *echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", errReader{})
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

// TestSNSBodyReadErrors covers error paths triggered when the request body cannot be read.
func TestSNSBodyReadErrors(t *testing.T) {
	t.Parallel()

	newSNSHandler := func() *sns.Handler {
		return sns.NewHandler(sns.NewInMemoryBackend())
	}

	t.Run("RouteMatcher_BodyReadError", func(t *testing.T) {
		t.Parallel()
		assert.False(t, newSNSHandler().RouteMatcher()(newErrContext()))
	})

	t.Run("ExtractOperation_BodyReadError", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Unknown", newSNSHandler().ExtractOperation(newErrContext()))
	})

	t.Run("ExtractResource_BodyReadError", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, newSNSHandler().ExtractResource(newErrContext()))
	})
}

// TestSNSHandlerInternalError covers the handleBackendError default case using a mock backend.
func TestSNSHandlerInternalError(t *testing.T) {
	t.Parallel()

	b := &errBackend{sns.NewInMemoryBackend()}
	h := sns.NewHandler(b)

	// CreateTopic will call handleBackendError with an unexpected (non-sentinel) error,
	// exercising the default branch in handleBackendError and errorCode.
	rec := snsPost(t, h, url.Values{
		"Action":  {"CreateTopic"},
		"Version": {"2010-03-31"},
		"Name":    {"test"},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "InternalError")
}
