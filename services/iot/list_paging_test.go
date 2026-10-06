package iot_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iotsdktypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

type pageFn func(ctx context.Context, c *iotsdk.Client, size *int32, token, prefix *string) (int, *string, error)

// TestListOps_PageAndFilter covers maxResults/nextToken (and pageSize/marker) plus
// name-prefix filters; bindings per iot@v1.77.4 serializers.go.
func TestListOps_PageAndFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list   pageFn
		seed   func(t *testing.T, c *iotsdk.Client, b *iot.InMemoryBackend)
		name   string
		prefix string
		total  int
	}{
		{
			name:   "billing_groups",
			prefix: "pg-",
			total:  3,
			seed: func(t *testing.T, c *iotsdk.Client, _ *iot.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"pg-a", "pg-b", "pg-c", "zz"} {
					_, err := c.CreateBillingGroup(
						t.Context(),
						&iotsdk.CreateBillingGroupInput{BillingGroupName: aws.String(n)},
					)
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *iotsdk.Client, sz *int32, tok, pfx *string) (int, *string, error) {
				out, err := c.ListBillingGroups(ctx, &iotsdk.ListBillingGroupsInput{
					MaxResults: sz, NextToken: tok, NamePrefixFilter: pfx,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.BillingGroups), out.NextToken, nil
			},
		},
		{
			name:   "thing_groups",
			prefix: "pg-",
			total:  3,
			seed: func(t *testing.T, c *iotsdk.Client, _ *iot.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"pg-a", "pg-b", "pg-c", "zz"} {
					_, err := c.CreateThingGroup(
						t.Context(),
						&iotsdk.CreateThingGroupInput{ThingGroupName: aws.String(n)},
					)
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *iotsdk.Client, sz *int32, tok, pfx *string) (int, *string, error) {
				out, err := c.ListThingGroups(ctx, &iotsdk.ListThingGroupsInput{
					MaxResults: sz, NextToken: tok, NamePrefixFilter: pfx,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ThingGroups), out.NextToken, nil
			},
		},
		{
			name:  "thing_types",
			total: 3,
			seed: func(t *testing.T, c *iotsdk.Client, _ *iot.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"pg-a", "pg-b", "pg-c"} {
					_, err := c.CreateThingType(t.Context(), &iotsdk.CreateThingTypeInput{ThingTypeName: aws.String(n)})
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *iotsdk.Client, sz *int32, tok, _ *string) (int, *string, error) {
				out, err := c.ListThingTypes(ctx, &iotsdk.ListThingTypesInput{MaxResults: sz, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ThingTypes), out.NextToken, nil
			},
		},
		{
			name:  "dimensions",
			total: 3,
			seed: func(t *testing.T, c *iotsdk.Client, _ *iot.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"pg-a", "pg-b", "pg-c"} {
					_, err := c.CreateDimension(t.Context(), &iotsdk.CreateDimensionInput{
						Name: aws.String(n), Type: iotsdktypes.DimensionTypeTopicFilter, StringValues: []string{"a/b"},
					})
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *iotsdk.Client, sz *int32, tok, _ *string) (int, *string, error) {
				out, err := c.ListDimensions(ctx, &iotsdk.ListDimensionsInput{MaxResults: sz, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(out.DimensionNames), out.NextToken, nil
			},
		},
		{
			name:  "attached_policies_pagesize_marker",
			total: 3,
			seed: func(t *testing.T, _ *iotsdk.Client, b *iot.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"pg-a", "pg-b", "pg-c"} {
					b.AddPolicyInternal(iot.Policy{PolicyName: n, CreatedAt: time.Now()})
					require.NoError(
						t,
						b.AttachPolicy(
							&iot.AttachPolicyInput{PolicyName: n, Target: "arn:aws:iot:us-east-1:123456789012:cert/x"},
						),
					)
				}
			},
			list: func(ctx context.Context, c *iotsdk.Client, sz *int32, tok, _ *string) (int, *string, error) {
				out, err := c.ListAttachedPolicies(ctx, &iotsdk.ListAttachedPoliciesInput{
					Target: aws.String("arn:aws:iot:us-east-1:123456789012:cert/x"), PageSize: sz, Marker: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.Policies), out.NextMarker, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, b := newIoTSDKClient(t)
			tt.seed(t, c, b)

			var pfx *string
			if tt.prefix != "" {
				pfx = aws.String(tt.prefix)
			}

			n, next, err := tt.list(t.Context(), c, nil, nil, pfx)
			require.NoError(t, err)
			assert.Equal(t, tt.total, n)
			assert.Nil(t, next, "no token when the page is not truncated")

			n, next, err = tt.list(t.Context(), c, aws.Int32(2), nil, pfx)
			require.NoError(t, err)
			assert.Equal(t, 2, n)
			require.NotNil(t, next)

			n, next, err = tt.list(t.Context(), c, aws.Int32(2), next, pfx)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			assert.Nil(t, next)

			_, _, err = tt.list(t.Context(), c, nil, aws.String("bogus"), pfx)
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidRequestException", apiErr.ErrorCode())
		})
	}
}

// TestListAuditTasks_StatusAndTimeFilter covers the RFC3339 startTime/endTime wire format
// (serializers.go:13088 FormatDateTime) and taskStatus.
func TestListAuditTasks_StatusAndTimeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status iotsdktypes.AuditTaskStatus
		start  time.Duration
		end    time.Duration
		want   int
	}{
		{name: "window_includes", start: -time.Hour, end: time.Hour, want: 1},
		{name: "window_in_future", start: time.Hour, end: 2 * time.Hour, want: 0},
		{name: "window_in_past", start: -2 * time.Hour, end: -time.Hour, want: 0},
		{
			name:   "status_match",
			status: iotsdktypes.AuditTaskStatusInProgress,
			start:  -time.Hour,
			end:    time.Hour,
			want:   1,
		},
		{name: "status_miss", status: iotsdktypes.AuditTaskStatusCompleted, start: -time.Hour, end: time.Hour, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, b := newIoTSDKClient(t)
			_, err := b.StartOnDemandAuditTask(nil)
			require.NoError(t, err)

			now := time.Now()
			out, err := c.ListAuditTasks(t.Context(), &iotsdk.ListAuditTasksInput{
				StartTime:  aws.Time(now.Add(tt.start)),
				EndTime:    aws.Time(now.Add(tt.end)),
				TaskStatus: tt.status,
			})
			require.NoError(t, err)
			assert.Len(t, out.Tasks, tt.want)
		})
	}
}
