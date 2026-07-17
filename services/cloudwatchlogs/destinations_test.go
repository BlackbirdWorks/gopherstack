package cloudwatchlogs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDestination_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_describe_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dest, err := b.PutDestination("my-dest", "arn:aws:kinesis:::stream/s", "arn:aws:iam:::role/r")
				require.NoError(t, err)
				assert.Equal(t, "my-dest", dest.DestinationName)
				assert.NotEmpty(t, dest.Arn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)

				err := b.DeleteDestination("my-dest")
				require.NoError(t, err)

				dests = b.DescribeDestinations("")
				assert.Empty(t, dests)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("dest1", "arn:old", "arn:role-old")
				require.NoError(t, err)
				_, err = b.PutDestination("dest1", "arn:new", "arn:role-new")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)
				assert.Equal(t, "arn:new", dests[0].TargetArn)
				assert.Equal(t, "arn:role-new", dests[0].RoleArn)
			},
		},
		{
			name: "put_destination_policy",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("my-dest", "arn:aws:kinesis:::stream/s", "arn:aws:iam:::role/r")
				require.NoError(t, err)
				err = b.PutDestinationPolicy("my-dest", `{"Statement":[]}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)
				assert.JSONEq(t, `{"Statement":[]}`, dests[0].AccessPolicy)
			},
		},
		{
			name: "describe_prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("prod-dest", "arn:a", "arn:r")
				require.NoError(t, err)
				_, err = b.PutDestination("dev-dest", "arn:b", "arn:r")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				all := b.DescribeDestinations("")
				assert.Len(t, all, 2)

				prod := b.DescribeDestinations("prod")
				require.Len(t, prod, 1)
				assert.Equal(t, "prod-dest", prod[0].DestinationName)

				none := b.DescribeDestinations("nonexistent")
				assert.Empty(t, none)
			},
		},
		{
			name: "put_policy_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutDestinationPolicy("ghost", `{}`)
				require.ErrorIs(t, err, cloudwatchlogs.ErrDestinationNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteDestination("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrDestinationNotFound)
			},
		},
		{
			name: "put_empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("", "arn:a", "arn:r")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}
