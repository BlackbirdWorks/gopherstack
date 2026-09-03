package autoscaling_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// assertPaginatesAllRecords drives list across pages of size pageSize until NextToken is nil,
// and asserts: the first page is full, a cursor comes back when more records remain, every
// record is seen, and no record is seen twice. Before the pagination fix, every listing under
// test here ignored MaxRecords/NextToken and returned all `total` records on page one with no
// NextToken -- so require.Len(page1, pageSize) alone already fails against the old code; the
// no-duplicate/exactly-once checks additionally catch a broken cursor (e.g. a non-unique sort
// key, or an unsorted map-derived slice) that a naive fix could introduce.
func assertPaginatesAllRecords[T any](
	t *testing.T,
	total, pageSize int,
	list func(nextToken *string, maxRecords int32) (page []T, next *string),
	keyOf func(T) string,
) {
	t.Helper()

	seen := make(map[string]bool, total)

	var token *string

	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination did not terminate")

		page, next := list(token, int32(pageSize))
		if pages == 0 {
			require.Len(t, page, pageSize, "first page should be full")
			require.NotNil(t, next, "first page should report a cursor")
		}

		for _, item := range page {
			k := keyOf(item)
			require.False(t, seen[k], "record %q seen twice across pages", k)
			seen[k] = true
		}

		if next == nil {
			break
		}

		token = next
	}

	require.Len(t, seen, total, "did not see every record exactly once")
}

func TestDescribeLaunchConfigurations_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	for i := range total {
		_, err := backend.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
			LaunchConfigurationName: fmt.Sprintf("pg-lc-%02d", i),
			ImageID:                 "ami-pg",
			InstanceType:            "t3.micro",
		})
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.LaunchConfiguration, *string) {
			out, err := client.DescribeLaunchConfigurations(t.Context(), &assdk.DescribeLaunchConfigurationsInput{
				NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, err)

			return out.LaunchConfigurations, out.NextToken
		},
		func(lc types.LaunchConfiguration) string { return aws.ToString(lc.LaunchConfigurationName) },
	)
}

func TestDescribeAutoScalingInstances_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-asi-group",
		MinSize:              total,
		MaxSize:              total,
		DesiredCapacity:      total,
	})
	require.NoError(t, err)

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.AutoScalingInstanceDetails, *string) {
			out, listErr := client.DescribeAutoScalingInstances(t.Context(), &assdk.DescribeAutoScalingInstancesInput{
				NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.AutoScalingInstances, out.NextToken
		},
		func(inst types.AutoScalingInstanceDetails) string { return aws.ToString(inst.InstanceId) },
	)
}

func TestDescribeScheduledActions_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-sa-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	base := time.Now().Add(time.Hour).UTC()
	for i := range total {
		require.NoError(t, backend.PutScheduledUpdateGroupAction("pg-sa-group", autoscaling.ScheduledUpdateGroupAction{
			ScheduledActionName: fmt.Sprintf("pg-sa-%02d", i),
			StartTime:           base.Add(time.Duration(i) * time.Minute),
		}))
	}

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.ScheduledUpdateGroupAction, *string) {
			out, listErr := client.DescribeScheduledActions(t.Context(), &assdk.DescribeScheduledActionsInput{
				AutoScalingGroupName: aws.String("pg-sa-group"), NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.ScheduledUpdateGroupActions, out.NextToken
		},
		func(a types.ScheduledUpdateGroupAction) string { return aws.ToString(a.ScheduledActionName) },
	)
}

func TestDescribeTags_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-tags-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	tags := make([]autoscaling.ResourceTag, 0, total)
	for i := range total {
		tags = append(tags, autoscaling.ResourceTag{
			ResourceID: "pg-tags-group", ResourceType: "auto-scaling-group",
			Key: fmt.Sprintf("pg-tag-key-%02d", i), Value: "v",
		})
	}
	require.NoError(t, backend.CreateOrUpdateTags(tags))

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.TagDescription, *string) {
			out, listErr := client.DescribeTags(t.Context(), &assdk.DescribeTagsInput{
				NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.Tags, out.NextToken
		},
		func(tag types.TagDescription) string { return aws.ToString(tag.Key) },
	)
}

func TestDescribeLoadBalancers_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-lb-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	names := make([]string, 0, total)
	for i := range total {
		names = append(names, fmt.Sprintf("pg-lb-%02d", i))
	}
	require.NoError(t, backend.AttachLoadBalancers("pg-lb-group", names))

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.LoadBalancerState, *string) {
			out, listErr := client.DescribeLoadBalancers(t.Context(), &assdk.DescribeLoadBalancersInput{
				AutoScalingGroupName: aws.String("pg-lb-group"), NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.LoadBalancers, out.NextToken
		},
		func(lb types.LoadBalancerState) string { return aws.ToString(lb.LoadBalancerName) },
	)
}

func TestDescribeLoadBalancerTargetGroups_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-tg-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	arns := make([]string, 0, total)
	for i := range total {
		arns = append(arns, fmt.Sprintf(
			"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/pg-tg-%02d/abc123", i,
		))
	}
	require.NoError(t, backend.AttachLoadBalancerTargetGroups("pg-tg-group", arns))

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.LoadBalancerTargetGroupState, *string) {
			out, listErr := client.DescribeLoadBalancerTargetGroups(
				t.Context(), &assdk.DescribeLoadBalancerTargetGroupsInput{
					AutoScalingGroupName: aws.String(
						"pg-tg-group",
					), NextToken: token, MaxRecords: aws.Int32(maxRecords),
				},
			)
			require.NoError(t, listErr)

			return out.LoadBalancerTargetGroups, out.NextToken
		},
		func(tg types.LoadBalancerTargetGroupState) string {
			return aws.ToString(tg.LoadBalancerTargetGroupARN)
		},
	)
}

// TestDescribeNotificationConfigurations_SDKRoundTrip_Pagination also proves
// DescribeNotificationConfigurations (notifications.go) sorts its account-wide result --
// before the fix it ranged a map with zero sort calls, so a paginated cursor over that order
// could drop or duplicate records across a page boundary.
func TestDescribeNotificationConfigurations_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-nc-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	for i := range total {
		topicARN := fmt.Sprintf("arn:aws:sns:us-east-1:123456789012:pg-topic-%02d", i)
		require.NoError(t, backend.PutNotificationConfiguration(
			"pg-nc-group", topicARN, []string{"autoscaling:EC2_INSTANCE_LAUNCH"},
		))
	}

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.NotificationConfiguration, *string) {
			out, listErr := client.DescribeNotificationConfigurations(
				t.Context(), &assdk.DescribeNotificationConfigurationsInput{
					NextToken: token, MaxRecords: aws.Int32(maxRecords),
				},
			)
			require.NoError(t, listErr)

			return out.NotificationConfigurations, out.NextToken
		},
		func(c types.NotificationConfiguration) string { return aws.ToString(c.TopicARN) },
	)
}

func TestDescribeTrafficSources_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-ts-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	sources := make([]autoscaling.TrafficSource, 0, total)
	for i := range total {
		sources = append(sources, autoscaling.TrafficSource{
			Identifier: fmt.Sprintf(
				"arn:aws:vpc-lattice:us-east-1:123456789012:targetgroup/pg-ts-%02d", i,
			),
			Type: "vpc-lattice",
		})
	}
	require.NoError(t, backend.AttachTrafficSources("pg-ts-group", sources))

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.TrafficSourceState, *string) {
			out, listErr := client.DescribeTrafficSources(t.Context(), &assdk.DescribeTrafficSourcesInput{
				AutoScalingGroupName: aws.String("pg-ts-group"), NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.TrafficSources, out.NextToken
		},
		func(ts types.TrafficSourceState) string { return aws.ToString(ts.Identifier) },
	)
}

// TestDescribeWarmPool_MaxRecordsNextToken_Wired proves DescribeWarmPool reads MaxRecords and
// NextToken without erroring and returns them wired into the response. This emulator doesn't
// model individual warm-pool instances (PutWarmPool only tracks pool-level config), so
// Instances is always empty and there is no >page-size collection to actually paginate --
// unlike the other nine listings in this file, this test cannot exercise a real page boundary.
func TestDescribeWarmPool_MaxRecordsNextToken_Wired(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-wp-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	require.NoError(t, backend.PutWarmPool(autoscaling.WarmPoolInput{
		AutoScalingGroupName: "pg-wp-group", MinSize: 1, MaxGroupPreparedCapacity: 5,
	}))

	out, err := client.DescribeWarmPool(t.Context(), &assdk.DescribeWarmPoolInput{
		AutoScalingGroupName: aws.String("pg-wp-group"),
		MaxRecords:           aws.Int32(1),
		NextToken:            aws.String(""),
	})
	require.NoError(t, err, "MaxRecords/NextToken must not error even though Instances is unmodeled")
	require.Empty(t, out.Instances)
	require.Nil(t, out.NextToken)
	require.NotNil(t, out.WarmPoolConfiguration)
}

// TestDescribeInstanceRefreshes_SDKRoundTrip_Pagination drives real MaxRecords/NextToken
// pagination for a single group. Real DescribeInstanceRefreshesInput requires
// AutoScalingGroupName (confirmed via `go doc` -- "This member is required"), so the SDK client
// itself refuses to build the account-wide request (empty AutoScalingGroupName) that exercises
// the map-ranging branch in instance_refreshes.go; that branch's sort fix is covered separately
// by TestDescribeInstanceRefreshes_AccountWide_SortIsDeterministic below, which calls the
// backend directly. AddInstanceRefresh (an existing test-only backend helper) seeds refreshes
// without the "only one InProgress/Pending refresh per group" restriction StartInstanceRefresh
// enforces.
func TestDescribeInstanceRefreshes_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pg-ir-group", MinSize: 0, MaxSize: 1,
	})
	require.NoError(t, err)

	for i := range total {
		require.NoError(t, backend.AddInstanceRefresh(autoscaling.InstanceRefresh{
			InstanceRefreshID:    fmt.Sprintf("pg-ir-%02d", i),
			AutoScalingGroupName: "pg-ir-group",
			Status:               "Successful",
			StartTime:            time.Now(),
		}))
	}

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.InstanceRefresh, *string) {
			out, listErr := client.DescribeInstanceRefreshes(t.Context(), &assdk.DescribeInstanceRefreshesInput{
				AutoScalingGroupName: aws.String("pg-ir-group"), NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.InstanceRefreshes, out.NextToken
		},
		func(r types.InstanceRefresh) string { return aws.ToString(r.InstanceRefreshId) },
	)
}

// TestDescribeInstanceRefreshes_AccountWide_SortIsDeterministic covers the map-ranging branch
// of DescribeInstanceRefreshes (groupName == "") that the real SDK client cannot reach (see the
// test above): before the fix it ranged b.instanceRefreshes (a map) with zero sort calls, so
// repeated calls against the same state could return the records in a different order --
// exactly the failure mode that drops or duplicates records across a pagination page boundary.
func TestDescribeInstanceRefreshes_AccountWide_SortIsDeterministic(t *testing.T) {
	t.Parallel()

	backend := autoscaling.NewInMemoryBackend()

	for i := range 10 {
		name := fmt.Sprintf("pg-irs-group-%02d", i)
		_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
			AutoScalingGroupName: name, MinSize: 0, MaxSize: 1,
		})
		require.NoError(t, err)

		require.NoError(t, backend.AddInstanceRefresh(autoscaling.InstanceRefresh{
			InstanceRefreshID:    fmt.Sprintf("pg-irs-%02d", i),
			AutoScalingGroupName: name,
			Status:               "Successful",
			StartTime:            time.Now(),
		}))
	}

	first, err := backend.DescribeInstanceRefreshes("", nil)
	require.NoError(t, err)
	require.Len(t, first, 10)

	for range 20 {
		again, describeErr := backend.DescribeInstanceRefreshes("", nil)
		require.NoError(t, describeErr)
		require.Equal(t, first, again, "account-wide DescribeInstanceRefreshes order must be stable across calls")
	}
}

// TestDescribePolicies_SDKRoundTrip_Pagination also proves DescribePolicies (scaling_policies.go)
// tiebreaks its PolicyName-only sort with AutoScalingGroupName -- PolicyName is unique only
// within a group (scalingPolicies is keyed by scopedKey(groupName, PolicyName)), so an
// account-wide query (groupName empty) can see the same PolicyName on different groups; without
// the tiebreak, sort order (and therefore the pagination cursor) would be nondeterministic
// across ties. This seeds every policy on a distinct group with the SAME PolicyName to force
// that tie.
func TestDescribePolicies_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)

	const total = 25
	for i := range total {
		name := fmt.Sprintf("pg-pol-group-%02d", i)
		_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
			AutoScalingGroupName: name, MinSize: 0, MaxSize: 1,
		})
		require.NoError(t, err)

		_, err = backend.PutScalingPolicy(autoscaling.ScalingPolicyInput{
			AutoScalingGroupName: name,
			PolicyName:           "tied-policy-name",
			PolicyType:           "SimpleScaling",
			AdjustmentType:       "ChangeInCapacity",
			ScalingAdjustment:    1,
		})
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(token *string, maxRecords int32) ([]types.ScalingPolicy, *string) {
			out, listErr := client.DescribePolicies(t.Context(), &assdk.DescribePoliciesInput{
				NextToken: token, MaxRecords: aws.Int32(maxRecords),
			})
			require.NoError(t, listErr)

			return out.ScalingPolicies, out.NextToken
		},
		func(p types.ScalingPolicy) string { return aws.ToString(p.AutoScalingGroupName) },
	)
}
