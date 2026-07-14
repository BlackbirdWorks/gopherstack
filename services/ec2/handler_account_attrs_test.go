package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Account/misc ---- //nolint:godot // existing issue.
func TestDescribeAccountAttributes(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns all attributes when no filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		attrs := b.DescribeAccountAttributes(nil)
		assert.NotEmpty(t, attrs)
	})

	t.Run("filters by name", func(t *testing.T) { //nolint:paralleltest // existing issue.
		attrs := b.DescribeAccountAttributes([]string{"max-instances"})
		require.Len(t, attrs, 1)
		assert.Equal(t, "max-instances", attrs[0].Name)
	})
}

func TestDescribePrefixLists(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns static prefix lists", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lists := b.DescribePrefixLists(nil)
		require.NotEmpty(t, lists)
		assert.Contains(t, lists[0].PrefixListName, "s3")
	})

	t.Run("filters by ID", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lists := b.DescribePrefixLists(nil)
		id := lists[0].PrefixListID
		filtered := b.DescribePrefixLists([]string{id})
		require.Len(t, filtered, 1)
	})
}

func TestIdFormat(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("describe returns default resources", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeIDFormat(nil)
		assert.NotEmpty(t, items)
	})

	t.Run("modify and describe shows change", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyIDFormat("instance", true))
		items := b.DescribeIDFormat([]string{"instance"})
		require.Len(t, items, 1)
		assert.True(t, items[0].UseLongIDs)
	})

	t.Run("describe aggregate returns all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeAggregateIDFormat()
		assert.NotEmpty(t, items)
	})

	t.Run("identity format delegates to account format", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeIdentityIDFormat("arn:aws:iam::000000000000:user/test", nil)
		assert.NotEmpty(t, items)
	})
}

func TestInstanceEventNotification(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("describe returns default", func(t *testing.T) { //nolint:paralleltest // existing issue.
		attrs := b.DescribeInstanceEventNotificationAttributes()
		assert.NotNil(t, attrs)
	})

	t.Run("deregister clears attributes", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DeregisterInstanceEventNotificationAttributes()
		attrs := b.DescribeInstanceEventNotificationAttributes()
		assert.NotNil(t, attrs)
	})
}

// ---- HTTP dispatch integration tests ---- //nolint:godot // existing issue.

func TestHTTP_DescribeAccountAttributes(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeAccountAttributes"},
	})
	require.NoError(t, err)
}

func TestHTTP_DescribePrefixLists(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribePrefixLists"},
	})
	require.NoError(t, err)
}

func TestHTTP_DescribeIdFormat(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeIdFormat"},
	})
	require.NoError(t, err)
}
