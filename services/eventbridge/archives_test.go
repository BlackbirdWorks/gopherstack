package eventbridge_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestArchiveJanitor_PrunesArchivedEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "my-bus"})
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/my-bus"
	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: busARN,
		RetentionDays:  1,
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "test", DetailType: "Test", Detail: `{}`, EventBusName: "my-bus"},
	})

	// Make the archive look old enough to expire.
	err = b.SetArchiveCreationTimeForTest("my-archive", time.Now().Add(-48*time.Hour))
	require.NoError(t, err)

	janitor := eventbridge.NewArchiveJanitor(b, time.Hour)
	janitor.SetNow(time.Now())
	janitor.SweepOnce(context.Background())

	_, err = b.DescribeArchive(context.Background(), "my-archive")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	assert.Equal(t, 0, b.ArchivedEventCount("my-archive"))
}

func TestArchiveJanitor_RetentionDaysZeroNeverExpires(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "bus2"})
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/bus2"
	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "forever-archive",
		EventSourceArn: busARN,
		RetentionDays:  0, // 0 = forever
	})
	require.NoError(t, err)

	err = b.SetArchiveCreationTimeForTest("forever-archive", time.Now().Add(-365*24*time.Hour))
	require.NoError(t, err)

	janitor := eventbridge.NewArchiveJanitor(b, time.Hour)
	janitor.SetNow(time.Now())
	janitor.SweepOnce(context.Background())

	_, err = b.DescribeArchive(context.Background(), "forever-archive")
	require.NoError(t, err, "archive with RetentionDays=0 should never expire")
}

func TestTags_Archive(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "tagged-bus"})
	require.NoError(t, err)

	archive, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "tagged-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/tagged-bus",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": archive.ArchiveArn,
		"Tags":        []map[string]string{{"Key": "retention", "Value": "30d"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": archive.ArchiveArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "30d")
}

func TestArchive_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "src-bus"})
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/src-bus"

	archive, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: busARN,
		RetentionDays:  7,
		EventPattern:   `{"source":["important.app"]}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "my-archive", archive.ArchiveName)
	assert.Equal(t, 7, archive.RetentionDays)

	described, err := b.DescribeArchive(context.Background(), "my-archive")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", described.State)

	updated, err := b.UpdateArchive(context.Background(), eventbridge.UpdateArchiveInput{
		ArchiveName:   "my-archive",
		Description:   "important events",
		RetentionDays: 14,
	})
	require.NoError(t, err)
	assert.Equal(t, 14, updated.RetentionDays)
	assert.Equal(t, "important events", updated.Description)

	archives, _, err := b.ListArchives(context.Background(), "my-", "", "", "")
	require.NoError(t, err)
	assert.Len(t, archives, 1)

	err = b.DeleteArchive(context.Background(), "my-archive")
	require.NoError(t, err)

	_, err = b.DescribeArchive(context.Background(), "my-archive")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestArchive_RejectsLongName(t *testing.T) {
	t.Parallel()
	b := newBackend()

	longName := strings.Repeat("a", 49)
	_, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    longName,
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestArchive_CapturesMatchingEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "capture-bus"})
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/capture-bus"
	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "capture-archive",
		EventSourceArn: busARN,
		EventPattern:   `{"source":["audit.app"]}`,
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "audit.app", DetailType: "T", Detail: `{}`, EventBusName: "capture-bus"},
		{Source: "other.app", DetailType: "T", Detail: `{}`, EventBusName: "capture-bus"},
	})

	count := b.ArchivedEventCount("capture-archive")
	assert.Equal(t, 1, count, "only matching events should be archived")
}

func TestCreateArchive_NameLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		archiveName string
	}{
		{name: "normal name ok", archiveName: "my-archive"},
		{name: "exactly 48 chars ok", archiveName: "12345678901234567890123456789012345678901234567a"},
		{
			name:        "49 chars rejected",
			archiveName: "1234567890123456789012345678901234567890123456789",
			wantErr:     eventbridge.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
				ArchiveName:    tt.archiveName,
				EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateArchive_NegativeRetentionRejects(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "bad-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
		RetentionDays:  -1,
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestUpdateArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(*eventbridge.InMemoryBackend)
		name     string
		wantDesc string
		input    eventbridge.UpdateArchiveInput
	}{
		{
			name:    "not found returns error",
			input:   eventbridge.UpdateArchiveInput{ArchiveName: "no-such"},
			wantErr: eventbridge.ErrNotFound,
		},
		{
			name: "updates description",
			setup: func(b *eventbridge.InMemoryBackend) {
				b.AddArchiveInternal(&eventbridge.Archive{
					ArchiveName:    "a1",
					EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
					State:          "ENABLED",
				})
			},
			input:    eventbridge.UpdateArchiveInput{ArchiveName: "a1", Description: "new desc"},
			wantDesc: "new desc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			got, err := b.UpdateArchive(context.Background(), tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, got.Description)
		})
	}
}

func TestArchiveCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeArchive(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeleteArchive(context.Background(), "missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
			ArchiveName:    "rt-archive",
			EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
		})
		require.NoError(t, err)

		got, err := b.DescribeArchive(context.Background(), "rt-archive")
		require.NoError(t, err)
		assert.Equal(t, "rt-archive", got.ArchiveName)

		archives, _, err := b.ListArchives(context.Background(), "rt-", "", "", "")
		require.NoError(t, err)
		assert.Len(t, archives, 1)

		require.NoError(t, b.DeleteArchive(context.Background(), "rt-archive"))
		_, err = b.DescribeArchive(context.Background(), "rt-archive")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// TestCreateArchive_RequiresName verifies name and source ARN validation.
func TestCreateArchive_RequiresName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input eventbridge.CreateArchiveInput
	}{
		{
			name:  "empty_archive_name",
			input: eventbridge.CreateArchiveInput{EventSourceArn: "arn:x"},
		},
		{
			name:  "empty_event_source_arn",
			input: eventbridge.CreateArchiveInput{ArchiveName: "a1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			_, err := b.CreateArchive(context.Background(), tt.input)
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

// TestCreateArchive_RoundTrip verifies archive creation state is ENABLED.
func TestCreateArchive_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	input := eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
	}

	archive, err := b.CreateArchive(context.Background(), input)
	require.NoError(t, err)

	assert.Equal(t, "my-archive", archive.ArchiveName)
	assert.Equal(t, "ENABLED", archive.State)
	assert.NotEmpty(t, archive.ArchiveArn)
	assert.Contains(t, archive.ArchiveArn, "archive/my-archive")
	assert.Equal(t, 1, b.ArchiveCount())
}

// TestCaptureEventInArchives_UsesPatternCache confirms archive pattern matching
// reuses the compiled-pattern cache: firing many events against an archive
// compiles its pattern at most once, and events are still captured.
func TestCaptureEventInArchives_UsesPatternCache(t *testing.T) {
	t.Parallel()

	b := newBackend()

	bus, err := b.DescribeEventBus(t.Context(), "default")
	require.NoError(t, err)

	_, err = b.CreateArchive(t.Context(), eventbridge.CreateArchiveInput{
		ArchiveName:    "cache-archive",
		EventSourceArn: bus.Arn,
		EventPattern:   `{"source":["cache.svc"]}`,
	})
	require.NoError(t, err)

	const events = 25
	for range events {
		b.PutEvents(t.Context(), []eventbridge.EventEntry{
			{Source: "cache.svc", DetailType: "Evt", Detail: `{}`},
		})
	}

	assert.Equal(t, events, b.ArchivedEventCount("cache-archive"))
	// Only the single archive pattern should have been compiled and cached,
	// regardless of how many events were processed.
	assert.LessOrEqual(t, b.PatternCacheSize(), 1,
		"archive pattern should be compiled at most once via the cache")
}
