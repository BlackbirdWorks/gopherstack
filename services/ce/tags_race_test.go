package ce_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestTaggedResourceConcurrentWithUntag proves Describe/List for cost
// categories and anomaly monitors must not alias a Tags map UntagResource mutates.
func TestTaggedResourceConcurrentWithUntag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *ce.InMemoryBackend) (arn string)
		reader func(b *ce.InMemoryBackend, arn string)
		name   string
	}{
		{
			name: "DescribeCostCategoryDefinition races UntagResource",
			setup: func(t *testing.T, b *ce.InMemoryBackend) string {
				t.Helper()

				cat, err := b.CreateCostCategoryDefinition(
					"race-cat", "CostCategoryExpression.v1", "unassigned",
					nil, map[string]string{"env": "prod"}, nil, "",
				)
				require.NoError(t, err)

				return cat.ARN
			},
			reader: func(b *ce.InMemoryBackend, arn string) {
				cat, err := b.DescribeCostCategoryDefinition(arn)
				if err != nil {
					return
				}

				for k := range cat.Tags {
					_ = k
				}
			},
		},
		{
			name: "GetAnomalyMonitors races UntagResource",
			setup: func(t *testing.T, b *ce.InMemoryBackend) string {
				t.Helper()

				mon, err := b.CreateAnomalyMonitor(
					"race-mon", "DIMENSIONAL", "SERVICE", nil, map[string]string{"env": "prod"},
				)
				require.NoError(t, err)

				return mon.MonitorARN
			},
			reader: func(b *ce.InMemoryBackend, arn string) {
				mons, _, err := b.GetAnomalyMonitors([]string{arn}, 0, "")
				if err != nil {
					return
				}

				for _, m := range mons {
					for k := range m.Tags {
						_ = k
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")
			arn := tt.setup(t, b)

			const iterations = 500

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(b, arn)
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					_ = b.TagResource(arn, map[string]string{"env": "prod"})
					_ = b.UntagResource(arn, []string{"env"})
				}
			}()

			wg.Wait()
		})
	}
}
