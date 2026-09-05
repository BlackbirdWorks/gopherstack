package autoscaling_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// newTestBackendAndClient is newTestHandlerAndClient (sdk_roundtrip_helper_test.go)
// plus a handle on the backend, needed here to seed fixtures the SDK's own
// input validation makes awkward to reach (e.g. a scaling activity with a
// non-default StatusCode).
func newTestBackendAndClient(t *testing.T) (*autoscaling.InMemoryBackend, *assdk.Client) {
	t.Helper()

	backend := autoscaling.NewInMemoryBackend()
	h := autoscaling.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := assdk.NewFromConfig(cfg, func(o *assdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return backend, client
}

// TestDescribePolicies_PolicyTypesFilter proves the PolicyTypes request
// member is applied -- previously only PolicyNames/AutoScalingGroupName were
// read, so a PolicyTypes filter silently matched every policy type.
func TestDescribePolicies_PolicyTypesFilter(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	ctx := t.Context()

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("policy-types-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.PutScalingPolicy(ctx, &assdk.PutScalingPolicyInput{
		AutoScalingGroupName: aws.String("policy-types-asg"),
		PolicyName:           aws.String("simple-policy"),
		PolicyType:           aws.String("SimpleScaling"),
		AdjustmentType:       aws.String("ChangeInCapacity"),
		ScalingAdjustment:    aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.PutScalingPolicy(ctx, &assdk.PutScalingPolicyInput{
		AutoScalingGroupName: aws.String("policy-types-asg"),
		PolicyName:           aws.String("step-policy"),
		PolicyType:           aws.String("StepScaling"),
		AdjustmentType:       aws.String("ChangeInCapacity"),
		StepAdjustments: []types.StepAdjustment{
			{ScalingAdjustment: aws.Int32(1), MetricIntervalLowerBound: aws.Float64(0)},
		},
	})
	require.NoError(t, err)

	stepOnly, err := client.DescribePolicies(ctx, &assdk.DescribePoliciesInput{
		AutoScalingGroupName: aws.String("policy-types-asg"),
		PolicyTypes:          []string{"StepScaling"},
	})
	require.NoError(t, err)
	require.Len(t, stepOnly.ScalingPolicies, 1, "PolicyTypes filter must exclude non-matching policy types")
	assert.Equal(t, "step-policy", aws.ToString(stepOnly.ScalingPolicies[0].PolicyName))

	both, err := client.DescribePolicies(ctx, &assdk.DescribePoliciesInput{
		AutoScalingGroupName: aws.String("policy-types-asg"),
	})
	require.NoError(t, err)
	assert.Len(t, both.ScalingPolicies, 2)
}

// TestDescribeScalingActivities_StatusFilterAndPagination proves the
// "Status" Filter and MaxRecords/NextToken are applied. Previously Filters
// were never read at all, and MaxRecords truncated the result with no
// NextToken -- silently dropping the remainder rather than paginating it.
func TestDescribeScalingActivities_StatusFilterAndPagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)
	ctx := t.Context()

	group, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "activities-asg",
		MinSize:              1,
		MaxSize:              1,
		DesiredCapacity:      1,
	})
	require.NoError(t, err)
	require.Len(t, group.Instances, 1)

	require.NoError(t, backend.PutLifecycleHook(autoscaling.LifecycleHook{
		LifecycleHookName:    "term-hook",
		AutoScalingGroupName: "activities-asg",
		LifecycleTransition:  "autoscaling:EC2_INSTANCE_TERMINATING",
		DefaultResult:        "CONTINUE",
	}))

	_, err = backend.TerminateInstanceInAutoScalingGroup(group.Instances[0].InstanceID, false)
	require.NoError(t, err)

	// One "Successful" activity (group creation) and one "InProgress"
	// activity (termination waiting on the lifecycle hook) now exist.
	inProgressOnly, err := client.DescribeScalingActivities(ctx, &assdk.DescribeScalingActivitiesInput{
		AutoScalingGroupName: aws.String("activities-asg"),
		Filters: []types.Filter{
			{Name: aws.String("Status"), Values: []string{"InProgress"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, inProgressOnly.Activities, 1, "Status filter must exclude non-matching activities")
	assert.Equal(t, "InProgress", string(inProgressOnly.Activities[0].StatusCode))

	page1, err := client.DescribeScalingActivities(ctx, &assdk.DescribeScalingActivitiesInput{
		AutoScalingGroupName: aws.String("activities-asg"),
		MaxRecords:           aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Activities, 1, "MaxRecords must cap the page size")
	require.NotNil(t, page1.NextToken, "a truncated result must carry a NextToken, not silently drop the rest")

	page2, err := client.DescribeScalingActivities(ctx, &assdk.DescribeScalingActivitiesInput{
		AutoScalingGroupName: aws.String("activities-asg"),
		MaxRecords:           aws.Int32(1),
		NextToken:            page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Activities, 1, "the second page must return the remainder")
	assert.NotEqual(t, aws.ToString(page1.Activities[0].ActivityId), aws.ToString(page2.Activities[0].ActivityId))
}

// TestDescribeScheduledActions_TimeRangeFilter proves the StartTime/EndTime
// request members are applied against each action's StartTime -- previously
// both were accepted but never read.
func TestDescribeScheduledActions_TimeRangeFilter(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sched-time-asg",
		MinSize:              0,
		MaxSize:              1,
	})
	require.NoError(t, err)

	early := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2030, 6, 1, 0, 0, 0, 0, time.UTC)

	require.NoError(t, backend.PutScheduledUpdateGroupAction("sched-time-asg", autoscaling.ScheduledUpdateGroupAction{
		ScheduledActionName: "early-action",
		StartTime:           early,
		MinSize:             aws.Int32(0),
		MaxSize:             aws.Int32(1),
	}))
	require.NoError(t, backend.PutScheduledUpdateGroupAction("sched-time-asg", autoscaling.ScheduledUpdateGroupAction{
		ScheduledActionName: "late-action",
		StartTime:           late,
		MinSize:             aws.Int32(0),
		MaxSize:             aws.Int32(1),
	}))

	inRange, err := client.DescribeScheduledActions(ctx, &assdk.DescribeScheduledActionsInput{
		AutoScalingGroupName: aws.String("sched-time-asg"),
		StartTime:            aws.Time(time.Date(2030, 2, 1, 0, 0, 0, 0, time.UTC)),
		EndTime:              aws.Time(time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC)),
	})
	require.NoError(t, err)
	require.Len(t, inRange.ScheduledUpdateGroupActions, 1, "StartTime/EndTime must exclude actions outside the range")
	assert.Equal(t, "late-action", aws.ToString(inRange.ScheduledUpdateGroupActions[0].ScheduledActionName))

	all, err := client.DescribeScheduledActions(ctx, &assdk.DescribeScheduledActionsInput{
		AutoScalingGroupName: aws.String("sched-time-asg"),
	})
	require.NoError(t, err)
	assert.Len(t, all.ScheduledUpdateGroupActions, 2)
}

// TestDescribeAutoScalingGroups_TagFilters proves the Filters request member
// (tag-key/tag-value/tag:<key>, API_DescribeAutoScalingGroups.html Examples
// 2-3) is applied -- previously Filters was not even part of the backend
// method signature, so every DescribeAutoScalingGroups call ignored it.
func TestDescribeAutoScalingGroups_TagFilters(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	ctx := t.Context()

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("prod-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		Tags: []types.Tag{
			{Key: aws.String("environment"), Value: aws.String("production")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("dev-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		Tags: []types.Tag{
			{Key: aws.String("environment"), Value: aws.String("development")},
		},
	})
	require.NoError(t, err)

	prodOnly, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		Filters: []types.Filter{
			{Name: aws.String("tag:environment"), Values: []string{"production"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, prodOnly.AutoScalingGroups, 1, "tag:environment=production filter must exclude the dev group")
	assert.Equal(t, "prod-asg", aws.ToString(prodOnly.AutoScalingGroups[0].AutoScalingGroupName))

	byKey, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		Filters: []types.Filter{
			{Name: aws.String("tag-key"), Values: []string{"environment"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, byKey.AutoScalingGroups, 2, "tag-key filter must match both groups")
}

// TestDescribeTrafficSources_TrafficSourceTypeFilter proves the
// TrafficSourceType request member is applied -- previously the handler
// never read it and returned every traffic source regardless of type.
func TestDescribeTrafficSources_TrafficSourceTypeFilter(t *testing.T) {
	t.Parallel()

	backend, client := newTestBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "ts-filter-asg",
		MinSize:              0,
		MaxSize:              1,
	})
	require.NoError(t, err)

	require.NoError(t, backend.AttachTrafficSources("ts-filter-asg", []autoscaling.TrafficSource{
		{Identifier: "arn:aws:elasticloadbalancing:tg/elbv2-tg", Type: "elbv2"},
		{Identifier: "arn:aws:vpc-lattice:tg/lattice-tg", Type: "vpc-lattice"},
	}))

	elbv2Only, err := client.DescribeTrafficSources(ctx, &assdk.DescribeTrafficSourcesInput{
		AutoScalingGroupName: aws.String("ts-filter-asg"),
		TrafficSourceType:    aws.String("elbv2"),
	})
	require.NoError(t, err)
	require.Len(t, elbv2Only.TrafficSources, 1, "TrafficSourceType filter must exclude non-matching sources")
	assert.Equal(t, "elbv2", aws.ToString(elbv2Only.TrafficSources[0].Type))

	all, err := client.DescribeTrafficSources(ctx, &assdk.DescribeTrafficSourcesInput{
		AutoScalingGroupName: aws.String("ts-filter-asg"),
	})
	require.NoError(t, err)
	assert.Len(t, all.TrafficSources, 2)
}
