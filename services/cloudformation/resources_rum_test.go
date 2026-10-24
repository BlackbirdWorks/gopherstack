package cloudformation_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	rumsdk "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	rumbackend "github.com/blackbirdworks/gopherstack/services/rum"
)

type rumStackEnv struct {
	cfn *cfnsdk.Client
	rum *rumsdk.Client
}

func newRUMStackEnv(t *testing.T) rumStackEnv {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	handler := rumbackend.NewHandler(rumbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.RUM = handler

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	url := sdktest.Serve(t, handler)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("000000000000", "test", "")),
	)
	require.NoError(t, err)

	return rumStackEnv{
		cfn: newTestClientForBackend(t, backend),
		rum: rumsdk.NewFromConfig(cfg, func(o *rumsdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

const rumMonitorTemplate = `{
"Resources": {"Mon": {"Type": "AWS::RUM::AppMonitor", "Properties": {
  "Name": "%s",
  "Domain": "%s",
  "CwLogEnabled": %s,
  "CustomEvents": {"Status": "%s"},
  "AppMonitorConfiguration": {
    "SessionSampleRate": 0.5,
    "Telemetries": ["errors", "performance"],
    "MetricDestinations": [{
      "Destination": "CloudWatch",
      "MetricDefinitions": [{"Name": "%s", "ValueKey": "event_details.duration", "UnitLabel": "ms",
        "EventPattern": "{\"event_type\":[\"%s\"]}"}]
    }]
  },
  "ResourcePolicy": {"PolicyDocument": %s},
  "Tags": [{"Key": "Team", "Value": "%s"}]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Mon"}},
  "Id": {"Value": {"Fn::GetAtt": ["Mon", "Id"]}}
}
}`

const (
	rumNavMetric = "PerformanceNavigationDuration"
	rumResMetric = "PerformanceResourceDuration"
)

const rumPolicyDocument = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
	`"Action":"rum:PutRumEvents","Resource":"*"}]}`

func rumTemplate(name, domain, cwLog, custom, metric, team string) string {
	eventType := map[string]string{
		rumNavMetric: "com.amazon.rum.performance_navigation_event",
		rumResMetric: "com.amazon.rum.performance_resource_event",
	}[metric]

	policy, _ := json.Marshal(rumPolicyDocument)

	return fmt.Sprintf(rumMonitorTemplate, name, domain, cwLog, custom, metric, eventType, policy, team)
}

func TestCreateStack_RUMAppMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, env rumStackEnv)
		name string
	}{
		{name: "create ref and getatt", run: testRUMCreate},
		{name: "update in place", run: testRUMUpdateInPlace},
		{name: "rename replaces", run: testRUMRename},
		{name: "delete stack", run: testRUMDelete},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newRUMStackEnv(t))
		})
	}
}

func testRUMCreate(t *testing.T, env rumStackEnv) {
	t.Helper()

	outputs := createStackAndGetOutputs(t, env.cfn, "rum-create",
		rumTemplate("mon-a", "example.com", "true", "ENABLED", rumNavMetric, "emu"))

	assert.Equal(t, "mon-a", outputs["Ref"])

	got, err := env.rum.GetAppMonitor(t.Context(), &rumsdk.GetAppMonitorInput{Name: aws.String("mon-a")})
	require.NoError(t, err)

	assert.Equal(t, outputs["Id"], aws.ToString(got.AppMonitor.Id))
	assert.Equal(t, "example.com", aws.ToString(got.AppMonitor.Domain))
	assert.True(t, aws.ToBool(got.AppMonitor.DataStorage.CwLog.CwLogEnabled))
	assert.Equal(t, "ENABLED", string(got.AppMonitor.CustomEvents.Status))
	assert.InDelta(t, 0.5, got.AppMonitor.AppMonitorConfiguration.SessionSampleRate, 0.0001)
	assert.Equal(t, map[string]string{"Team": "emu"}, got.AppMonitor.Tags)

	dests, err := env.rum.ListRumMetricsDestinations(t.Context(), &rumsdk.ListRumMetricsDestinationsInput{
		AppMonitorName: aws.String("mon-a"),
	})
	require.NoError(t, err)
	require.Len(t, dests.Destinations, 1)

	defs, err := env.rum.BatchGetRumMetricDefinitions(t.Context(), &rumsdk.BatchGetRumMetricDefinitionsInput{
		AppMonitorName: aws.String("mon-a"), Destination: dests.Destinations[0].Destination,
	})
	require.NoError(t, err)
	require.Len(t, defs.MetricDefinitions, 1)
	assert.Equal(t, rumNavMetric, aws.ToString(defs.MetricDefinitions[0].Name))

	policy, err := env.rum.GetResourcePolicy(t.Context(), &rumsdk.GetResourcePolicyInput{Name: aws.String("mon-a")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(policy.PolicyDocument), "rum:PutRumEvents")
}

func updateRUMStack(t *testing.T, env rumStackEnv, template string) {
	t.Helper()

	_, err := env.cfn.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("rum-update"), TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	desc, err := env.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("rum-update")})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	stack := desc.Stacks[0]
	require.Equal(t, cfntypes.StackStatusUpdateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))
}

func testRUMUpdateInPlace(t *testing.T, env rumStackEnv) {
	t.Helper()

	outputs := createStackAndGetOutputs(t, env.cfn, "rum-update",
		rumTemplate("mon-u", "example.com", "false", "DISABLED", rumNavMetric, "emu"))

	updateRUMStack(t, env, rumTemplate("mon-u", "other.example.com", "true", "ENABLED", rumResMetric, "dev"))

	got, err := env.rum.GetAppMonitor(t.Context(), &rumsdk.GetAppMonitorInput{Name: aws.String("mon-u")})
	require.NoError(t, err)

	assert.Equal(t, outputs["Id"], aws.ToString(got.AppMonitor.Id), "monitor was updated, not replaced")
	assert.Equal(t, "other.example.com", aws.ToString(got.AppMonitor.Domain))
	assert.True(t, aws.ToBool(got.AppMonitor.DataStorage.CwLog.CwLogEnabled))
	assert.Equal(t, "ENABLED", string(got.AppMonitor.CustomEvents.Status))
	assert.Equal(t, map[string]string{"Team": "dev"}, got.AppMonitor.Tags)

	defs, err := env.rum.BatchGetRumMetricDefinitions(t.Context(), &rumsdk.BatchGetRumMetricDefinitionsInput{
		AppMonitorName: aws.String("mon-u"), Destination: "CloudWatch",
	})
	require.NoError(t, err)
	require.Len(t, defs.MetricDefinitions, 1)
	assert.Equal(t, rumResMetric, aws.ToString(defs.MetricDefinitions[0].Name))
}

func testRUMRename(t *testing.T, env rumStackEnv) {
	t.Helper()

	createStackAndGetOutputs(t, env.cfn, "rum-update",
		rumTemplate("mon-old", "example.com", "false", "DISABLED", rumNavMetric, "emu"))

	updateRUMStack(t, env, rumTemplate("mon-new", "example.com", "false", "DISABLED", rumNavMetric, "emu"))

	_, err := env.rum.GetAppMonitor(t.Context(), &rumsdk.GetAppMonitorInput{Name: aws.String("mon-new")})
	require.NoError(t, err)

	_, err = env.rum.GetAppMonitor(t.Context(), &rumsdk.GetAppMonitorInput{Name: aws.String("mon-old")})
	require.Error(t, err)
}

func testRUMDelete(t *testing.T, env rumStackEnv) {
	t.Helper()

	createStackAndGetOutputs(t, env.cfn, "rum-delete",
		rumTemplate("mon-d", "example.com", "false", "DISABLED", rumNavMetric, "emu"))

	_, err := env.cfn.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rum-delete")})
	require.NoError(t, err)

	list, err := env.rum.ListAppMonitors(t.Context(), &rumsdk.ListAppMonitorsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.AppMonitorSummaries)
}
