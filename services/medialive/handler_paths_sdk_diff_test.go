package medialive_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

// sdkRouteCases is the authoritative method+path for every real medialive
// operation, extracted from medialive@v1.101.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptInputDeviceTransfer", "POST", "/prod/inputDevices/PLACEHOLDER/accept"},
		{"BatchDelete", "POST", "/prod/batch/delete"},
		{"BatchStart", "POST", "/prod/batch/start"},
		{"BatchStop", "POST", "/prod/batch/stop"},
		{"BatchUpdateSchedule", "PUT", "/prod/channels/PLACEHOLDER/schedule"},
		{"CancelInputDeviceTransfer", "POST", "/prod/inputDevices/PLACEHOLDER/cancel"},
		{"ClaimDevice", "POST", "/prod/claimDevice"},
		{"CreateChannel", "POST", "/prod/channels"},
		{"CreateChannelPlacementGroup", "POST", "/prod/clusters/PLACEHOLDER/channelplacementgroups"},
		{"CreateCloudWatchAlarmTemplate", "POST", "/prod/cloudwatch-alarm-templates"},
		{"CreateCloudWatchAlarmTemplateGroup", "POST", "/prod/cloudwatch-alarm-template-groups"},
		{"CreateCluster", "POST", "/prod/clusters"},
		{"CreateEventBridgeRuleTemplate", "POST", "/prod/eventbridge-rule-templates"},
		{"CreateEventBridgeRuleTemplateGroup", "POST", "/prod/eventbridge-rule-template-groups"},
		{"CreateInput", "POST", "/prod/inputs"},
		{"CreateInputSecurityGroup", "POST", "/prod/inputSecurityGroups"},
		{"CreateMultiplex", "POST", "/prod/multiplexes"},
		{"CreateMultiplexProgram", "POST", "/prod/multiplexes/PLACEHOLDER/programs"},
		{"CreateNetwork", "POST", "/prod/networks"},
		{"CreateNode", "POST", "/prod/clusters/PLACEHOLDER/nodes"},
		{"CreateNodeRegistrationScript", "POST", "/prod/clusters/PLACEHOLDER/nodeRegistrationScript"},
		{"CreatePartnerInput", "POST", "/prod/inputs/PLACEHOLDER/partners"},
		{"CreateSdiSource", "POST", "/prod/sdiSources"},
		{"CreateSignalMap", "POST", "/prod/signal-maps"},
		{"CreateTags", "POST", "/prod/tags/PLACEHOLDER"},
		{"DeleteChannel", "DELETE", "/prod/channels/PLACEHOLDER"},
		{"DeleteChannelPlacementGroup", "DELETE", "/prod/clusters/PLACEHOLDER/channelplacementgroups/PLACEHOLDER"},
		{"DeleteCloudWatchAlarmTemplate", "DELETE", "/prod/cloudwatch-alarm-templates/PLACEHOLDER"},
		{
			"DeleteCloudWatchAlarmTemplateGroup", "DELETE",
			"/prod/cloudwatch-alarm-template-groups/PLACEHOLDER",
		},
		{"DeleteCluster", "DELETE", "/prod/clusters/PLACEHOLDER"},
		{"DeleteEventBridgeRuleTemplate", "DELETE", "/prod/eventbridge-rule-templates/PLACEHOLDER"},
		{
			"DeleteEventBridgeRuleTemplateGroup", "DELETE",
			"/prod/eventbridge-rule-template-groups/PLACEHOLDER",
		},
		{"DeleteInput", "DELETE", "/prod/inputs/PLACEHOLDER"},
		{"DeleteInputSecurityGroup", "DELETE", "/prod/inputSecurityGroups/PLACEHOLDER"},
		{"DeleteMultiplex", "DELETE", "/prod/multiplexes/PLACEHOLDER"},
		{"DeleteMultiplexProgram", "DELETE", "/prod/multiplexes/PLACEHOLDER/programs/PLACEHOLDER"},
		{"DeleteNetwork", "DELETE", "/prod/networks/PLACEHOLDER"},
		{"DeleteNode", "DELETE", "/prod/clusters/PLACEHOLDER/nodes/PLACEHOLDER"},
		{"DeleteReservation", "DELETE", "/prod/reservations/PLACEHOLDER"},
		{"DeleteSchedule", "DELETE", "/prod/channels/PLACEHOLDER/schedule"},
		{"DeleteSdiSource", "DELETE", "/prod/sdiSources/PLACEHOLDER"},
		{"DeleteSignalMap", "DELETE", "/prod/signal-maps/PLACEHOLDER"},
		{"DeleteTags", "DELETE", "/prod/tags/PLACEHOLDER"},
		{"DescribeAccountConfiguration", "GET", "/prod/accountConfiguration"},
		{"DescribeChannel", "GET", "/prod/channels/PLACEHOLDER"},
		{"DescribeChannelPlacementGroup", "GET", "/prod/clusters/PLACEHOLDER/channelplacementgroups/PLACEHOLDER"},
		{"DescribeCluster", "GET", "/prod/clusters/PLACEHOLDER"},
		{"DescribeInput", "GET", "/prod/inputs/PLACEHOLDER"},
		{"DescribeInputDevice", "GET", "/prod/inputDevices/PLACEHOLDER"},
		{"DescribeInputDeviceThumbnail", "GET", "/prod/inputDevices/PLACEHOLDER/thumbnailData"},
		{"DescribeInputSecurityGroup", "GET", "/prod/inputSecurityGroups/PLACEHOLDER"},
		{"DescribeMultiplex", "GET", "/prod/multiplexes/PLACEHOLDER"},
		{"DescribeMultiplexProgram", "GET", "/prod/multiplexes/PLACEHOLDER/programs/PLACEHOLDER"},
		{"DescribeNetwork", "GET", "/prod/networks/PLACEHOLDER"},
		{"DescribeNode", "GET", "/prod/clusters/PLACEHOLDER/nodes/PLACEHOLDER"},
		{"DescribeOffering", "GET", "/prod/offerings/PLACEHOLDER"},
		{"DescribeReservation", "GET", "/prod/reservations/PLACEHOLDER"},
		{"DescribeSchedule", "GET", "/prod/channels/PLACEHOLDER/schedule"},
		{"DescribeSdiSource", "GET", "/prod/sdiSources/PLACEHOLDER"},
		{"DescribeThumbnails", "GET", "/prod/channels/PLACEHOLDER/thumbnails"},
		{"GetCloudWatchAlarmTemplate", "GET", "/prod/cloudwatch-alarm-templates/PLACEHOLDER"},
		{"GetCloudWatchAlarmTemplateGroup", "GET", "/prod/cloudwatch-alarm-template-groups/PLACEHOLDER"},
		{"GetEventBridgeRuleTemplate", "GET", "/prod/eventbridge-rule-templates/PLACEHOLDER"},
		{"GetEventBridgeRuleTemplateGroup", "GET", "/prod/eventbridge-rule-template-groups/PLACEHOLDER"},
		{"GetSignalMap", "GET", "/prod/signal-maps/PLACEHOLDER"},
		{"ListAlerts", "GET", "/prod/channels/PLACEHOLDER/alerts"},
		{"ListChannelPlacementGroups", "GET", "/prod/clusters/PLACEHOLDER/channelplacementgroups"},
		{"ListChannels", "GET", "/prod/channels"},
		{"ListCloudWatchAlarmTemplateGroups", "GET", "/prod/cloudwatch-alarm-template-groups"},
		{"ListCloudWatchAlarmTemplates", "GET", "/prod/cloudwatch-alarm-templates"},
		{"ListClusterAlerts", "GET", "/prod/clusters/PLACEHOLDER/alerts"},
		{"ListClusters", "GET", "/prod/clusters"},
		{"ListEventBridgeRuleTemplateGroups", "GET", "/prod/eventbridge-rule-template-groups"},
		{"ListEventBridgeRuleTemplates", "GET", "/prod/eventbridge-rule-templates"},
		{"ListInputDeviceTransfers", "GET", "/prod/inputDeviceTransfers"},
		{"ListInputDevices", "GET", "/prod/inputDevices"},
		{"ListInputSecurityGroups", "GET", "/prod/inputSecurityGroups"},
		{"ListInputs", "GET", "/prod/inputs"},
		{"ListMultiplexAlerts", "GET", "/prod/multiplexes/PLACEHOLDER/alerts"},
		{"ListMultiplexPrograms", "GET", "/prod/multiplexes/PLACEHOLDER/programs"},
		{"ListMultiplexes", "GET", "/prod/multiplexes"},
		{"ListNetworks", "GET", "/prod/networks"},
		{"ListNodes", "GET", "/prod/clusters/PLACEHOLDER/nodes"},
		{"ListOfferings", "GET", "/prod/offerings"},
		{"ListReservations", "GET", "/prod/reservations"},
		{"ListSdiSources", "GET", "/prod/sdiSources"},
		{"ListSignalMaps", "GET", "/prod/signal-maps"},
		{"ListTagsForResource", "GET", "/prod/tags/PLACEHOLDER"},
		{"ListVersions", "GET", "/prod/versions"},
		{"PurchaseOffering", "POST", "/prod/offerings/PLACEHOLDER/purchase"},
		{"RebootInputDevice", "POST", "/prod/inputDevices/PLACEHOLDER/reboot"},
		{"RejectInputDeviceTransfer", "POST", "/prod/inputDevices/PLACEHOLDER/reject"},
		{"RestartChannelPipelines", "POST", "/prod/channels/PLACEHOLDER/restartChannelPipelines"},
		{"StartChannel", "POST", "/prod/channels/PLACEHOLDER/start"},
		{"StartDeleteMonitorDeployment", "DELETE", "/prod/signal-maps/PLACEHOLDER/monitor-deployment"},
		{"StartInputDevice", "POST", "/prod/inputDevices/PLACEHOLDER/start"},
		{
			"StartInputDeviceMaintenanceWindow", "POST",
			"/prod/inputDevices/PLACEHOLDER/startInputDeviceMaintenanceWindow",
		},
		{"StartMonitorDeployment", "POST", "/prod/signal-maps/PLACEHOLDER/monitor-deployment"},
		{"StartMultiplex", "POST", "/prod/multiplexes/PLACEHOLDER/start"},
		{"StartUpdateSignalMap", "PATCH", "/prod/signal-maps/PLACEHOLDER"},
		{"StopChannel", "POST", "/prod/channels/PLACEHOLDER/stop"},
		{"StopInputDevice", "POST", "/prod/inputDevices/PLACEHOLDER/stop"},
		{"StopMultiplex", "POST", "/prod/multiplexes/PLACEHOLDER/stop"},
		{"TransferInputDevice", "POST", "/prod/inputDevices/PLACEHOLDER/transfer"},
		{"UpdateAccountConfiguration", "PUT", "/prod/accountConfiguration"},
		{"UpdateChannel", "PUT", "/prod/channels/PLACEHOLDER"},
		{"UpdateChannelClass", "PUT", "/prod/channels/PLACEHOLDER/channelClass"},
		{"UpdateChannelPlacementGroup", "PUT", "/prod/clusters/PLACEHOLDER/channelplacementgroups/PLACEHOLDER"},
		{"UpdateCloudWatchAlarmTemplate", "PATCH", "/prod/cloudwatch-alarm-templates/PLACEHOLDER"},
		{
			"UpdateCloudWatchAlarmTemplateGroup", "PATCH",
			"/prod/cloudwatch-alarm-template-groups/PLACEHOLDER",
		},
		{"UpdateCluster", "PUT", "/prod/clusters/PLACEHOLDER"},
		{"UpdateEventBridgeRuleTemplate", "PATCH", "/prod/eventbridge-rule-templates/PLACEHOLDER"},
		{
			"UpdateEventBridgeRuleTemplateGroup", "PATCH",
			"/prod/eventbridge-rule-template-groups/PLACEHOLDER",
		},
		{"UpdateInput", "PUT", "/prod/inputs/PLACEHOLDER"},
		{"UpdateInputDevice", "PUT", "/prod/inputDevices/PLACEHOLDER"},
		{"UpdateInputSecurityGroup", "PUT", "/prod/inputSecurityGroups/PLACEHOLDER"},
		{"UpdateMultiplex", "PUT", "/prod/multiplexes/PLACEHOLDER"},
		{"UpdateMultiplexProgram", "PUT", "/prod/multiplexes/PLACEHOLDER/programs/PLACEHOLDER"},
		{"UpdateNetwork", "PUT", "/prod/networks/PLACEHOLDER"},
		{"UpdateNode", "PUT", "/prod/clusters/PLACEHOLDER/nodes/PLACEHOLDER"},
		{"UpdateNodeState", "PUT", "/prod/clusters/PLACEHOLDER/nodes/PLACEHOLDER/state"},
		{"UpdateReservation", "PUT", "/prod/reservations/PLACEHOLDER"},
		{"UpdateSdiSource", "PUT", "/prod/sdiSources/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real medialive op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}
		})
	}
}
