package eventbridge //nolint:testpackage // needs buildDeliveryPlan.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func benchPlanBackend(b *testing.B, rules int) *InMemoryBackend {
	b.Helper()

	be := NewInMemoryBackend()
	b.Cleanup(be.Close)

	ctx := context.Background()
	for i := range rules {
		name := fmt.Sprintf("rule-%d", i)
		pattern := fmt.Sprintf(
			`{"source":["bench.app"],"detail":{"state":["running"],"id":[{"numeric":[">=",%d]}]}}`, i%3,
		)
		_, err := be.PutRule(ctx, PutRuleInput{Name: name, EventPattern: pattern})
		require.NoError(b, err)

		_, err = be.PutTargets(ctx, name, "", []Target{{ID: "t", Arn: "arn:aws:sqs:us-east-1:000000000000:q"}})
		require.NoError(b, err)
	}

	return be
}

func BenchmarkBuildDeliveryPlan(b *testing.B) {
	for _, n := range []int{10, 100, 290} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			be := benchPlanBackend(b, n)
			entries := []EventEntry{{
				Source: "bench.app", DetailType: "T", Detail: `{"state":"running","id":5,"extra":{"a":[1,2,3]}}`,
			}}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_ = be.buildDeliveryPlan(be.region, entries, nil)
			}
		})
	}
}

func BenchmarkFilterArchivedEvents(b *testing.B) {
	be := NewInMemoryBackend()
	b.Cleanup(be.Close)

	region := be.region
	events := make([]EventEntry, 1000)
	for i := range events {
		events[i] = EventEntry{
			Source: "bench.app", DetailType: "T", Detail: fmt.Sprintf(`{"state":"running","id":%d}`, i),
		}
	}

	be.archivedEventsStore(region)["a"] = events
	pattern := `{"source":["bench.app"],"detail":{"id":[{"numeric":[">=",500]}]}}`

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_ = be.filterArchivedEvents(region, "a", pattern, time.Time{}, time.Time{})
	}
}
