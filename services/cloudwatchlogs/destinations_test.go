package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
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
				dests, _ := b.DescribeDestinations("", 0, "")
				require.Len(t, dests, 1)

				err := b.DeleteDestination("my-dest")
				require.NoError(t, err)

				dests, _ = b.DescribeDestinations("", 0, "")
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
				dests, _ := b.DescribeDestinations("", 0, "")
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
				dests, _ := b.DescribeDestinations("", 0, "")
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
				all, _ := b.DescribeDestinations("", 0, "")
				assert.Len(t, all, 2)

				prod, _ := b.DescribeDestinations("prod", 0, "")
				require.Len(t, prod, 1)
				assert.Equal(t, "prod-dest", prod[0].DestinationName)

				none, _ := b.DescribeDestinations("nonexistent", 0, "")
				assert.Empty(t, none)
			},
		},
		{
			name: "put_policy_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutDestinationPolicy("ghost", `{}`)
				// PutDestinationPolicy's own deserializer declares
				// InvalidParameterException/OperationAbortedException/
				// ServiceUnavailableException, not ResourceNotFoundException --
				// unlike DeleteDestination below, which does declare it.
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
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
		{
			// Real DescribeDestinationsInput/Output support limit/nextToken
			// pagination (see api_op_DescribeDestinations.go) -- a previous
			// revision had no way to page through results at all.
			name: "describe_paginates",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"dest-a", "dest-b", "dest-c"} {
					_, err := b.PutDestination(n, "arn:a", "arn:r")
					require.NoError(t, err)
				}
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				page1, next1 := b.DescribeDestinations("", 2, "")
				require.Len(t, page1, 2)
				require.NotEmpty(t, next1)
				assert.Equal(t, "dest-a", page1[0].DestinationName)
				assert.Equal(t, "dest-b", page1[1].DestinationName)

				page2, next2 := b.DescribeDestinations("", 2, next1)
				require.Len(t, page2, 1)
				assert.Empty(t, next2)
				assert.Equal(t, "dest-c", page2[0].DestinationName)
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
