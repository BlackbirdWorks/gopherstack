package eventbridge_test

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestArchiveJanitor_PrunesEventsPastRetention(t *testing.T) {
	t.Parallel()

	const day = 24 * time.Hour

	tests := []struct {
		name          string
		ages          []time.Duration
		retentionDays int
		wantKept      int
	}{
		{name: "older than retention pruned", retentionDays: 1, ages: []time.Duration{2 * day}, wantKept: 0},
		{name: "within retention kept", retentionDays: 3, ages: []time.Duration{day}, wantKept: 1},
		{name: "zero retention keeps all", retentionDays: 0, ages: []time.Duration{365 * day, day}, wantKept: 2},
		{
			name:          "mixed ages prune only old",
			retentionDays: 2,
			ages:          []time.Duration{3 * day, day, 4 * day},
			wantKept:      1,
		},
		{name: "exactly at retention pruned", retentionDays: 1, ages: []time.Duration{day}, wantKept: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				b := newBackend()
				t.Cleanup(b.Close)

				_, err := b.CreateEventBus(ctx, eventbridge.CreateEventBusParams{Name: "bus"})
				require.NoError(t, err)

				_, err = b.CreateArchive(ctx, eventbridge.CreateArchiveInput{
					ArchiveName:    "arc",
					EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/bus",
					RetentionDays:  tt.retentionDays,
				})
				require.NoError(t, err)

				start := time.Now()
				maxAge := time.Duration(0)
				for _, a := range tt.ages {
					maxAge = max(maxAge, a)
				}

				// Each event is captured at sweep time minus its age.
				ordered := slices.Clone(tt.ages)
				slices.SortFunc(ordered, func(a, b time.Duration) int { return cmp.Compare(b, a) })
				for _, age := range ordered {
					time.Sleep(maxAge - age - time.Since(start))
					b.PutEvents(ctx, []eventbridge.EventEntry{
						{Source: "s", DetailType: "T", Detail: `{}`, EventBusName: "bus"},
					})
				}
				time.Sleep(maxAge - time.Since(start))

				before, err := b.DescribeArchive(ctx, "arc")
				require.NoError(t, err)
				require.Equal(t, int64(len(tt.ages)), before.EventCount)
				perEvent := before.SizeBytes / before.EventCount
				require.Positive(t, perEvent)

				eventbridge.NewArchiveJanitor(b, time.Hour).SweepOnce(ctx)

				after, err := b.DescribeArchive(ctx, "arc")
				require.NoError(t, err, "an archive itself never expires")
				assert.Equal(t, tt.wantKept, b.ArchivedEventCount("arc"))
				assert.Equal(t, int64(tt.wantKept), after.EventCount)
				assert.Equal(t, int64(tt.wantKept)*perEvent, after.SizeBytes)
			})
		})
	}
}
