package kafkaconnect

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseKafkaConnectPath(t *testing.T) {
	t.Parallel()

	const connArn = "arn:aws:kafkaconnect:us-east-1:000000000000:connector/test/uuid-1"

	tests := []struct {
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
	}{
		{name: "create_connector", method: http.MethodPost, path: "/v1/connectors", wantOp: opCreateConnector},
		{name: "list_connectors", method: http.MethodGet, path: "/v1/connectors", wantOp: opListConnectors},
		{
			name: "describe_connector", method: http.MethodGet, path: "/v1/connectors/" + connArn,
			wantOp: opDescribeConnector, wantResource: connArn,
		},
		{
			name: "update_connector", method: http.MethodPut, path: "/v1/connectors/" + connArn,
			wantOp: opUpdateConnector, wantResource: connArn,
		},
		{
			name: "delete_connector", method: http.MethodDelete, path: "/v1/connectors/" + connArn,
			wantOp: opDeleteConnector, wantResource: connArn,
		},
		{
			name: "restart_connector", method: http.MethodPost, path: "/v1/connectors/" + connArn + "/restart",
			wantOp: opRestartConnector, wantResource: connArn,
		},
		{
			name:         "list_connector_operations",
			method:       http.MethodGet,
			path:         "/v1/connectors/" + connArn + "/operations",
			wantOp:       opListConnectorOperations,
			wantResource: connArn,
		},
		{
			name: "describe_connector_operation", method: http.MethodGet,
			path:   "/v1/connectorOperations/" + connArn + "/operation/op-1",
			wantOp: opDescribeConnectorOperation, wantResource: connArn + "/operation/op-1",
		},
		{
			name:   "create_custom_plugin",
			method: http.MethodPost,
			path:   "/v1/custom-plugins",
			wantOp: opCreateCustomPlugin,
		},
		{name: "list_custom_plugins", method: http.MethodGet, path: "/v1/custom-plugins", wantOp: opListCustomPlugins},
		{
			name: "describe_custom_plugin", method: http.MethodGet, path: "/v1/custom-plugins/plugin-arn",
			wantOp: opDescribeCustomPlugin, wantResource: "plugin-arn",
		},
		{
			name: "delete_custom_plugin", method: http.MethodDelete, path: "/v1/custom-plugins/plugin-arn",
			wantOp: opDeleteCustomPlugin, wantResource: "plugin-arn",
		},
		{
			name: "create_worker_configuration", method: http.MethodPost, path: "/v1/worker-configurations",
			wantOp: opCreateWorkerConfiguration,
		},
		{
			name: "list_worker_configurations", method: http.MethodGet, path: "/v1/worker-configurations",
			wantOp: opListWorkerConfigurations,
		},
		{
			name: "describe_worker_configuration", method: http.MethodGet, path: "/v1/worker-configurations/wc-arn",
			wantOp: opDescribeWorkerConfiguration, wantResource: "wc-arn",
		},
		{
			name: "delete_worker_configuration", method: http.MethodDelete, path: "/v1/worker-configurations/wc-arn",
			wantOp: opDeleteWorkerConfiguration, wantResource: "wc-arn",
		},
		{
			name: "tag_resource", method: http.MethodPost, path: "/v1/tags/" + connArn,
			wantOp: opTagResource, wantResource: connArn,
		},
		{
			name: "untag_resource", method: http.MethodDelete, path: "/v1/tags/" + connArn,
			wantOp: opUntagResource, wantResource: connArn,
		},
		{
			name: "list_tags_for_resource", method: http.MethodGet, path: "/v1/tags/" + connArn,
			wantOp: opListTagsForResource, wantResource: connArn,
		},
		{name: "unknown_path", method: http.MethodGet, path: "/v1/unknown", wantOp: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, resource := parseKafkaConnectPath(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestIsKafkaConnectTagsPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "kafkaconnect arn matches",
			path: "/v1/tags/arn:aws:kafkaconnect:us-east-1:000000000000:connector/test/uuid-1",
			want: true,
		},
		{
			name: "kafka msk arn does not match",
			path: "/v1/tags/arn:aws:kafka:us-east-1:000000000000:cluster/test/uuid-1",
			want: false,
		},
		{name: "empty resource does not match", path: "/v1/tags/", want: false},
		{name: "non-arn resource does not match", path: "/v1/tags/not-an-arn", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isKafkaConnectTagsPath(tt.path))
		})
	}
}
