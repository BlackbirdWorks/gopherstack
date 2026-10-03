package kafkaconnect

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	connectorsPath           = "/v1/connectors"
	connectorsPrefix         = "/v1/connectors/"
	connectorOperationsPath  = "/v1/connectorOperations/"
	customPluginsPath        = "/v1/custom-plugins"
	customPluginsPrefix      = "/v1/custom-plugins/"
	workerConfigurationsPath = "/v1/worker-configurations"
	workerConfigurationsPre  = "/v1/worker-configurations/"
	tagsPrefix               = "/v1/tags/"

	operationsSuffix = "/operations"
	restartSuffix    = "/restart"
)

// parseKafkaConnectPath parses an HTTP method + path into an operation name
// and its resource ARN (the empty string for operations with no resource in
// the path, e.g. CreateConnector).
func parseKafkaConnectPath(method, path string) (string, string) {
	switch {
	case path == connectorsPath:
		return parseConnectorsRoot(method)
	case strings.HasPrefix(path, connectorsPrefix):
		return parseConnectorResource(method, path[len(connectorsPrefix):])
	case strings.HasPrefix(path, connectorOperationsPath):
		return parseConnectorOperationResource(method, path[len(connectorOperationsPath):])
	case path == customPluginsPath:
		return parseCustomPluginsRoot(method)
	case strings.HasPrefix(path, customPluginsPrefix):
		return parseCustomPluginResource(method, path[len(customPluginsPrefix):])
	case path == workerConfigurationsPath:
		return parseWorkerConfigurationsRoot(method)
	case strings.HasPrefix(path, workerConfigurationsPre):
		return parseWorkerConfigurationResource(method, path[len(workerConfigurationsPre):])
	case strings.HasPrefix(path, tagsPrefix):
		return parseTagsResource(method, path[len(tagsPrefix):])
	}

	return "", ""
}

func parseConnectorsRoot(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateConnector, ""
	case http.MethodGet:
		return opListConnectors, ""
	}

	return "", ""
}

func parseConnectorResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if connectorArn, ok := strings.CutSuffix(decoded, operationsSuffix); ok {
		if method == http.MethodGet {
			return opListConnectorOperations, connectorArn
		}

		return "", ""
	}

	if connectorArn, ok := strings.CutSuffix(decoded, restartSuffix); ok {
		if method == http.MethodPost {
			return opRestartConnector, connectorArn
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opDescribeConnector, decoded
	case http.MethodPut:
		return opUpdateConnector, decoded
	case http.MethodDelete:
		return opDeleteConnector, decoded
	}

	return "", ""
}

func parseConnectorOperationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	if method == http.MethodGet {
		return opDescribeConnectorOperation, decoded
	}

	return "", ""
}

func parseCustomPluginsRoot(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateCustomPlugin, ""
	case http.MethodGet:
		return opListCustomPlugins, ""
	}

	return "", ""
}

func parseCustomPluginResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return opDescribeCustomPlugin, decoded
	case http.MethodDelete:
		return opDeleteCustomPlugin, decoded
	}

	return "", ""
}

func parseWorkerConfigurationsRoot(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateWorkerConfiguration, ""
	case http.MethodGet:
		return opListWorkerConfigurations, ""
	}

	return "", ""
}

func parseWorkerConfigurationResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return opDescribeWorkerConfiguration, decoded
	case http.MethodDelete:
		return opDeleteWorkerConfiguration, decoded
	}

	return "", ""
}

func parseTagsResource(method, remainder string) (string, string) {
	decoded, _ := url.PathUnescape(remainder)

	switch method {
	case http.MethodGet:
		return opListTagsForResource, decoded
	case http.MethodPost:
		return opTagResource, decoded
	case http.MethodDelete:
		return opUntagResource, decoded
	}

	return "", ""
}
