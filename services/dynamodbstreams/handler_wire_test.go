package dynamodbstreams_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// Generic wire-format assertions (headers, content type) that apply to every
// successful DynamoDB Streams response, exercised here via ListStreams.

func TestHandler_WireFormat_CRC32Header(t *testing.T) {
	t.Parallel()

	db, _ := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "ListStreams", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get("X-Amz-Crc32"),
		"all successful responses must include X-Amz-Crc32 header")
}

func TestHandler_WireFormat_ContentType(t *testing.T) {
	t.Parallel()

	db, _ := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "ListStreams", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/x-amz-json-1.0",
		w.Header().Get("Content-Type"),
		"DynamoDB Streams responses must use application/x-amz-json-1.0")
}
