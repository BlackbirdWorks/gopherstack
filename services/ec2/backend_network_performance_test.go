package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestNetworkPerformanceSubscriptions_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("enable requires source and destination", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.EnableAwsNetworkPerformanceMetricSubscription("", "us-east-2", "", "")
		require.Error(t, err)
	})

	t.Run("enable defaults metric and statistic", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ok, err := b.EnableAwsNetworkPerformanceMetricSubscription("us-east-1", "us-east-2", "", "")
		require.NoError(t, err)
		assert.True(t, ok)

		subs := b.DescribeAwsNetworkPerformanceMetricSubscriptions()
		require.Len(t, subs, 1)
		assert.Equal(t, "aggregate-latency", subs[0].Metric)
		assert.Equal(t, "p50", subs[0].Statistic)
	})

	t.Run("disable removes the subscription", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ok, err := b.DisableAwsNetworkPerformanceMetricSubscription("us-east-1", "us-east-2", "", "")
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Empty(t, b.DescribeAwsNetworkPerformanceMetricSubscriptions())
	})

	t.Run("disable unknown subscription still succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ok, err := b.DisableAwsNetworkPerformanceMetricSubscription("us-west-1", "us-west-2", "", "")
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestGetAwsNetworkPerformanceData(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("requires at least one query", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.GetAwsNetworkPerformanceData(nil, time.Time{}, time.Time{})
		require.Error(t, err)
	})

	t.Run("data points are deterministic per pair", func(t *testing.T) { //nolint:paralleltest // existing issue.
		queries := []ec2.NetworkPerformanceDataQuery{
			{ID: "q1", Source: "us-east-1", Destination: "us-east-2"},
		}

		first, err := b.GetAwsNetworkPerformanceData(queries, time.Time{}, time.Time{})
		require.NoError(t, err)
		require.Len(t, first, 1)
		require.Len(t, first[0].MetricPoints, 1)

		second, err := b.GetAwsNetworkPerformanceData(queries, time.Time{}, time.Time{})
		require.NoError(t, err)
		require.Len(t, second, 1)
		require.Len(t, second[0].MetricPoints, 1)

		assert.InDelta(t, first[0].MetricPoints[0].Value, second[0].MetricPoints[0].Value, 0.0001)
		assert.Equal(t, "q1", first[0].ID)
		assert.Equal(t, "aggregate-latency", first[0].Metric)
	})

	t.Run("rejects a query missing source or destination", func(t *testing.T) { //nolint:paralleltest // existing issue.
		queries := []ec2.NetworkPerformanceDataQuery{{ID: "q2", Source: "us-east-1"}}
		_, err := b.GetAwsNetworkPerformanceData(queries, time.Time{}, time.Time{})
		require.Error(t, err)
	})
}
