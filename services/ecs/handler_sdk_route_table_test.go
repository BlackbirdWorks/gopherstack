package ecs_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real ECS
// operation, extracted from ecs@v1.90.0 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonEC2ContainerServiceV20141113.<Op>")
// and always request.Request.Method = "POST" against path "/" -- ECS is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same
// way (strip the fixed prefix / split on "."), so the class of bug this
// table can catch is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- ECS is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 77 real ECS ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 77/77) as of ecs@v1.90.0 --
// confirmed by diffing the dispatch-table keys against this exact list,
// zero mismatches either direction.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonEC2ContainerServiceV20141113.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"ContinueServiceDeployment", "AmazonEC2ContainerServiceV20141113.ContinueServiceDeployment"},
		{"CreateCapacityProvider", "AmazonEC2ContainerServiceV20141113.CreateCapacityProvider"},
		{"CreateCluster", "AmazonEC2ContainerServiceV20141113.CreateCluster"},
		{"CreateDaemon", "AmazonEC2ContainerServiceV20141113.CreateDaemon"},
		{"CreateExpressGatewayService", "AmazonEC2ContainerServiceV20141113.CreateExpressGatewayService"},
		{"CreateService", "AmazonEC2ContainerServiceV20141113.CreateService"},
		{"CreateTaskSet", "AmazonEC2ContainerServiceV20141113.CreateTaskSet"},
		{"DeleteAccountSetting", "AmazonEC2ContainerServiceV20141113.DeleteAccountSetting"},
		{"DeleteAttributes", "AmazonEC2ContainerServiceV20141113.DeleteAttributes"},
		{"DeleteCapacityProvider", "AmazonEC2ContainerServiceV20141113.DeleteCapacityProvider"},
		{"DeleteCluster", "AmazonEC2ContainerServiceV20141113.DeleteCluster"},
		{"DeleteDaemon", "AmazonEC2ContainerServiceV20141113.DeleteDaemon"},
		{"DeleteDaemonTaskDefinition", "AmazonEC2ContainerServiceV20141113.DeleteDaemonTaskDefinition"},
		{"DeleteExpressGatewayService", "AmazonEC2ContainerServiceV20141113.DeleteExpressGatewayService"},
		{"DeleteService", "AmazonEC2ContainerServiceV20141113.DeleteService"},
		{"DeleteTaskDefinitions", "AmazonEC2ContainerServiceV20141113.DeleteTaskDefinitions"},
		{"DeleteTaskSet", "AmazonEC2ContainerServiceV20141113.DeleteTaskSet"},
		{"DeregisterContainerInstance", "AmazonEC2ContainerServiceV20141113.DeregisterContainerInstance"},
		{"DeregisterTaskDefinition", "AmazonEC2ContainerServiceV20141113.DeregisterTaskDefinition"},
		{"DescribeCapacityProviders", "AmazonEC2ContainerServiceV20141113.DescribeCapacityProviders"},
		{"DescribeClusters", "AmazonEC2ContainerServiceV20141113.DescribeClusters"},
		{"DescribeContainerInstances", "AmazonEC2ContainerServiceV20141113.DescribeContainerInstances"},
		{"DescribeDaemon", "AmazonEC2ContainerServiceV20141113.DescribeDaemon"},
		{"DescribeDaemonDeployments", "AmazonEC2ContainerServiceV20141113.DescribeDaemonDeployments"},
		{"DescribeDaemonRevisions", "AmazonEC2ContainerServiceV20141113.DescribeDaemonRevisions"},
		{"DescribeDaemonTaskDefinition", "AmazonEC2ContainerServiceV20141113.DescribeDaemonTaskDefinition"},
		{"DescribeExpressGatewayService", "AmazonEC2ContainerServiceV20141113.DescribeExpressGatewayService"},
		{"DescribeServiceDeployments", "AmazonEC2ContainerServiceV20141113.DescribeServiceDeployments"},
		{"DescribeServiceRevisions", "AmazonEC2ContainerServiceV20141113.DescribeServiceRevisions"},
		{"DescribeServices", "AmazonEC2ContainerServiceV20141113.DescribeServices"},
		{"DescribeTaskDefinition", "AmazonEC2ContainerServiceV20141113.DescribeTaskDefinition"},
		{"DescribeTasks", "AmazonEC2ContainerServiceV20141113.DescribeTasks"},
		{"DescribeTaskSets", "AmazonEC2ContainerServiceV20141113.DescribeTaskSets"},
		{"DiscoverPollEndpoint", "AmazonEC2ContainerServiceV20141113.DiscoverPollEndpoint"},
		{"ExecuteCommand", "AmazonEC2ContainerServiceV20141113.ExecuteCommand"},
		{"GetTaskProtection", "AmazonEC2ContainerServiceV20141113.GetTaskProtection"},
		{"ListAccountSettings", "AmazonEC2ContainerServiceV20141113.ListAccountSettings"},
		{"ListAttributes", "AmazonEC2ContainerServiceV20141113.ListAttributes"},
		{"ListClusters", "AmazonEC2ContainerServiceV20141113.ListClusters"},
		{"ListContainerInstances", "AmazonEC2ContainerServiceV20141113.ListContainerInstances"},
		{"ListDaemonDeployments", "AmazonEC2ContainerServiceV20141113.ListDaemonDeployments"},
		{"ListDaemons", "AmazonEC2ContainerServiceV20141113.ListDaemons"},
		{"ListDaemonTaskDefinitions", "AmazonEC2ContainerServiceV20141113.ListDaemonTaskDefinitions"},
		{"ListServiceDeployments", "AmazonEC2ContainerServiceV20141113.ListServiceDeployments"},
		{"ListServices", "AmazonEC2ContainerServiceV20141113.ListServices"},
		{"ListServicesByNamespace", "AmazonEC2ContainerServiceV20141113.ListServicesByNamespace"},
		{"ListTagsForResource", "AmazonEC2ContainerServiceV20141113.ListTagsForResource"},
		{"ListTaskDefinitionFamilies", "AmazonEC2ContainerServiceV20141113.ListTaskDefinitionFamilies"},
		{"ListTaskDefinitions", "AmazonEC2ContainerServiceV20141113.ListTaskDefinitions"},
		{"ListTasks", "AmazonEC2ContainerServiceV20141113.ListTasks"},
		{"PutAccountSetting", "AmazonEC2ContainerServiceV20141113.PutAccountSetting"},
		{"PutAccountSettingDefault", "AmazonEC2ContainerServiceV20141113.PutAccountSettingDefault"},
		{"PutAttributes", "AmazonEC2ContainerServiceV20141113.PutAttributes"},
		{"PutClusterCapacityProviders", "AmazonEC2ContainerServiceV20141113.PutClusterCapacityProviders"},
		{"RegisterContainerInstance", "AmazonEC2ContainerServiceV20141113.RegisterContainerInstance"},
		{"RegisterDaemonTaskDefinition", "AmazonEC2ContainerServiceV20141113.RegisterDaemonTaskDefinition"},
		{"RegisterTaskDefinition", "AmazonEC2ContainerServiceV20141113.RegisterTaskDefinition"},
		{"RunTask", "AmazonEC2ContainerServiceV20141113.RunTask"},
		{"StartTask", "AmazonEC2ContainerServiceV20141113.StartTask"},
		{"StopServiceDeployment", "AmazonEC2ContainerServiceV20141113.StopServiceDeployment"},
		{"StopTask", "AmazonEC2ContainerServiceV20141113.StopTask"},
		{"SubmitAttachmentStateChanges", "AmazonEC2ContainerServiceV20141113.SubmitAttachmentStateChanges"},
		{"SubmitContainerStateChange", "AmazonEC2ContainerServiceV20141113.SubmitContainerStateChange"},
		{"SubmitTaskStateChange", "AmazonEC2ContainerServiceV20141113.SubmitTaskStateChange"},
		{"TagResource", "AmazonEC2ContainerServiceV20141113.TagResource"},
		{"UntagResource", "AmazonEC2ContainerServiceV20141113.UntagResource"},
		{"UpdateCapacityProvider", "AmazonEC2ContainerServiceV20141113.UpdateCapacityProvider"},
		{"UpdateCluster", "AmazonEC2ContainerServiceV20141113.UpdateCluster"},
		{"UpdateClusterSettings", "AmazonEC2ContainerServiceV20141113.UpdateClusterSettings"},
		{"UpdateContainerAgent", "AmazonEC2ContainerServiceV20141113.UpdateContainerAgent"},
		{"UpdateContainerInstancesState", "AmazonEC2ContainerServiceV20141113.UpdateContainerInstancesState"},
		{"UpdateDaemon", "AmazonEC2ContainerServiceV20141113.UpdateDaemon"},
		{"UpdateExpressGatewayService", "AmazonEC2ContainerServiceV20141113.UpdateExpressGatewayService"},
		{"UpdateService", "AmazonEC2ContainerServiceV20141113.UpdateService"},
		{"UpdateServicePrimaryTaskSet", "AmazonEC2ContainerServiceV20141113.UpdateServicePrimaryTaskSet"},
		{"UpdateTaskProtection", "AmazonEC2ContainerServiceV20141113.UpdateTaskProtection"},
		{"UpdateTaskSet", "AmazonEC2ContainerServiceV20141113.UpdateTaskSet"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real ECS operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
			h := ecs.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
