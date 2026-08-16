package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
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

// TestManagedResourceVisibility verifies GetManagedResourceVisibility and
// ModifyManagedResourceVisibility (parity-4): the account default starts
// "hidden" (the real AWS default) and Modify mutates real, persisted state.
func TestManagedResourceVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vals     url.Values
		wantBody string
		wantErr  bool
	}{
		{
			name:     "get_default_is_hidden",
			vals:     url.Values{"Action": {"GetManagedResourceVisibility"}},
			wantBody: "<defaultVisibility>hidden</defaultVisibility>",
		},
		{
			name: "modify_to_visible",
			vals: url.Values{
				"Action":            {"ModifyManagedResourceVisibility"},
				"DefaultVisibility": {"visible"},
			},
			wantBody: "<defaultVisibility>visible</defaultVisibility>",
		},
		{
			name: "modify_invalid_value_fails",
			vals: url.Values{
				"Action":            {"ModifyManagedResourceVisibility"},
				"DefaultVisibility": {"bogus"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			resp, err := dispatchHandler(h, tt.vals)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, resp, tt.wantBody)
		})
	}
}

// TestManagedResourceVisibility_ModifyPersistsAcrossGets verifies a Modify
// call's value is visible on a subsequent Get (real mutated account state).
func TestManagedResourceVisibility_ModifyPersistsAcrossGets(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action":            {"ModifyManagedResourceVisibility"},
		"DefaultVisibility": {"visible"},
	})
	require.NoError(t, err)

	resp, err := dispatchHandler(h, url.Values{"Action": {"GetManagedResourceVisibility"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<defaultVisibility>visible</defaultVisibility>")
}
