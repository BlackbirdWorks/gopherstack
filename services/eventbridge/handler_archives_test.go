package eventbridge_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestHandler_ArchiveCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "h-archive-bus"})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "CreateArchive", map[string]any{
		"ArchiveName":    "h-archive",
		"EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/h-archive-bus",
		"RetentionDays":  30,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeArchive", map[string]any{"ArchiveName": "h-archive"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-archive")

	rec = auditMakeRequest(t, h, e, "UpdateArchive", map[string]any{
		"ArchiveName":   "h-archive",
		"Description":   "handler updated",
		"RetentionDays": 60,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListArchives", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-archive")

	rec = auditMakeRequest(t, h, e, "DeleteArchive", map[string]any{"ArchiveName": "h-archive"})
	assert.Equal(t, http.StatusOK, rec.Code)
}
