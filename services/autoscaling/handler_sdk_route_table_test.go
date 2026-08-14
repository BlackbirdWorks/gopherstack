package autoscaling_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative Action value for every real Auto Scaling
// operation, extracted from autoscaling@v1.70.4 serializers.go: each op's
// awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- Auto Scaling is
// AWS Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// form field. ExtractOperation and Handler() both read r.Form.Get("Action")
// directly, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case) -- not a
// route-template mismatch. Query protocol is case-insensitive for XML field
// names on the wire, but gopherstack's own dispatch is a Go string switch
// (via buildDispatchTable's map), which is always exact-match regardless of
// protocol.
//
// This table covers all 66 real Auto Scaling ops (autoscaling@v1.70.4)
// -- confirmed by diffing GetSupportedOperations() and the dispatchTable()
// map's 66 keys against this exact list: zero mismatches in either
// direction, and no dead or excluded keys found.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AttachInstances",
		"AttachLoadBalancers",
		"AttachLoadBalancerTargetGroups",
		"AttachTrafficSources",
		"BatchDeleteScheduledAction",
		"BatchPutScheduledUpdateGroupAction",
		"CancelInstanceRefresh",
		"CompleteLifecycleAction",
		"CreateAutoScalingGroup",
		"CreateLaunchConfiguration",
		"CreateOrUpdateTags",
		"DeleteAutoScalingGroup",
		"DeleteLaunchConfiguration",
		"DeleteLifecycleHook",
		"DeleteNotificationConfiguration",
		"DeletePolicy",
		"DeleteScheduledAction",
		"DeleteTags",
		"DeleteWarmPool",
		"DescribeAccountLimits",
		"DescribeAdjustmentTypes",
		"DescribeAutoScalingGroups",
		"DescribeAutoScalingInstances",
		"DescribeAutoScalingNotificationTypes",
		"DescribeInstanceRefreshes",
		"DescribeLaunchConfigurations",
		"DescribeLifecycleHooks",
		"DescribeLifecycleHookTypes",
		"DescribeLoadBalancers",
		"DescribeLoadBalancerTargetGroups",
		"DescribeMetricCollectionTypes",
		"DescribeNotificationConfigurations",
		"DescribePolicies",
		"DescribeScalingActivities",
		"DescribeScalingProcessTypes",
		"DescribeScheduledActions",
		"DescribeTags",
		"DescribeTerminationPolicyTypes",
		"DescribeTrafficSources",
		"DescribeWarmPool",
		"DetachInstances",
		"DetachLoadBalancers",
		"DetachLoadBalancerTargetGroups",
		"DetachTrafficSources",
		"DisableMetricsCollection",
		"EnableMetricsCollection",
		"EnterStandby",
		"ExecutePolicy",
		"ExitStandby",
		"GetPredictiveScalingForecast",
		"LaunchInstances",
		"PutLifecycleHook",
		"PutNotificationConfiguration",
		"PutScalingPolicy",
		"PutScheduledUpdateGroupAction",
		"PutWarmPool",
		"RecordLifecycleActionHeartbeat",
		"ResumeProcesses",
		"RollbackInstanceRefresh",
		"SetDesiredCapacity",
		"SetInstanceHealth",
		"SetInstanceProtection",
		"StartInstanceRefresh",
		"SuspendProcesses",
		"TerminateInstanceInAutoScalingGroup",
		"UpdateAutoScalingGroup",
	}
}

// TestExtractOperation_SDKRouteTable drives every real Auto Scaling
// operation's authoritative Action value through ExtractOperation and
// Handler(), asserting the form field resolves to the right op name and that
// Handler() does not fall through to the "InvalidAction" sentinel (the
// ErrUnknownAction wire code, errors.go:15) that a dispatch-table key
// mismatch would produce. ErrUnknownAction has exactly one production call
// site -- the dispatch() miss in h.dispatchTable() -- and no other sentinel
// in this handler maps to the "InvalidAction" wire code, so it cannot
// collide with a legitimate error on this all-default-body table.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
