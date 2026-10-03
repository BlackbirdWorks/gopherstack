package kafkaconnect

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildConnectorOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateConnector: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreateConnector(c, body)
		},
		opDescribeConnector: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeConnector(c, resource)
		},
		opListConnectors: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListConnectors(c)
		},
		opUpdateConnector: func(c *echo.Context, resource string, body []byte) error {
			return h.handleUpdateConnector(c, resource, body)
		},
		opDeleteConnector: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteConnector(c, resource)
		},
		opRestartConnector: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleRestartConnector(c, resource)
		},
		opDescribeConnectorOperation: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeConnectorOperation(c, resource)
		},
		opListConnectorOperations: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleListConnectorOperations(c, resource)
		},
	}
}

func (h *Handler) handleCreateConnector(c *echo.Context, body []byte) error {
	var req createConnectorRequest
	if err := decodeBody(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if req.ConnectorName == "" || req.ServiceExecutionRoleArn == "" || req.KafkaConnectVersion == "" {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "missing required field")
	}

	spec := ConnectorSpec{
		Name:                             req.ConnectorName,
		Description:                      req.ConnectorDescription,
		ConnectorConfiguration:           req.ConnectorConfiguration,
		Capacity:                         capacityFromDTO(req.Capacity),
		ApacheKafkaCluster:               apacheKafkaClusterFromDTO(req.KafkaCluster),
		KafkaClusterClientAuthentication: req.KafkaClusterClientAuthentication.AuthenticationType,
		KafkaClusterEncryptionInTransit:  req.KafkaClusterEncryptionInTransit.EncryptionType,
		KafkaConnectVersion:              req.KafkaConnectVersion,
		ServiceExecutionRoleArn:          req.ServiceExecutionRoleArn,
		NetworkType:                      req.NetworkType,
		Plugins:                          pluginsFromDTO(req.Plugins),
		WorkerLogDelivery:                workerLogDeliveryFromDTO(req.LogDelivery),
		Tags:                             req.Tags,
	}

	if req.WorkerConfiguration != nil {
		spec.WorkerConfiguration = &WorkerConfigRef{
			Arn:      req.WorkerConfiguration.WorkerConfigurationArn,
			Revision: req.WorkerConfiguration.Revision,
		}
	}

	connector, err := h.Backend.CreateConnector(h.AccountID, h.DefaultRegion, spec)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, createConnectorResponse{
		ConnectorArn:   connector.ARN,
		ConnectorName:  connector.Name,
		ConnectorState: connector.State,
	})
}

func (h *Handler) handleDescribeConnector(c *echo.Context, connectorArn string) error {
	connector, err := h.Backend.DescribeConnector(connectorArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeConnectorResponse{connectorSummaryDTO: connectorToDTO(connector)})
}

func (h *Handler) handleListConnectors(c *echo.Context) error {
	q := c.Request().URL.Query()
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	connectors, next, err := h.Backend.ListConnectors(q.Get("connectorNamePrefix"), q.Get("nextToken"), maxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	dtos := make([]connectorSummaryDTO, 0, len(connectors))
	for _, connector := range connectors {
		dtos = append(dtos, connectorToDTO(connector))
	}

	return h.writeJSON(c, listConnectorsResponse{Connectors: dtos, NextToken: next})
}

func (h *Handler) handleUpdateConnector(c *echo.Context, connectorArn string, body []byte) error {
	var req updateConnectorRequest
	if err := decodeBody(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	currentVersion := c.Request().URL.Query().Get("currentVersion")

	update := ConnectorUpdate{ConnectorConfiguration: req.ConnectorConfiguration}
	if req.Capacity != nil {
		capUpdate := capacityFromDTO(*req.Capacity)
		update.Capacity = &capUpdate
	}

	connector, op, err := h.Backend.UpdateConnector(connectorArn, currentVersion, update)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, updateConnectorResponse{
		ConnectorArn:          connector.ARN,
		ConnectorOperationArn: op.ARN,
		ConnectorState:        connector.State,
	})
}

func (h *Handler) handleDeleteConnector(c *echo.Context, connectorArn string) error {
	currentVersion := c.Request().URL.Query().Get("currentVersion")

	connector, err := h.Backend.DeleteConnector(connectorArn, currentVersion)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, deleteConnectorResponse{ConnectorArn: connector.ARN, ConnectorState: connector.State})
}

func (h *Handler) handleRestartConnector(c *echo.Context, connectorArn string) error {
	onlyFailedTasks := c.Request().URL.Query().Get("onlyFailedTasks") == "true"

	_, op, err := h.Backend.RestartConnector(connectorArn, onlyFailedTasks)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, restartConnectorResponse{ConnectorArn: op.ConnectorArn, ConnectorOperationArn: op.ARN})
}

func (h *Handler) handleDescribeConnectorOperation(c *echo.Context, operationArn string) error {
	op, err := h.Backend.DescribeConnectorOperation(operationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, connectorOperationToDescribeDTO(op))
}

func (h *Handler) handleListConnectorOperations(c *echo.Context, connectorArn string) error {
	q := c.Request().URL.Query()
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	ops, next, err := h.Backend.ListConnectorOperations(connectorArn, q.Get("nextToken"), maxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	dtos := make([]connectorOperationSummaryDTO, 0, len(ops))
	for _, op := range ops {
		dtos = append(dtos, connectorOperationToSummaryDTO(op))
	}

	return h.writeJSON(c, listConnectorOperationsResponse{ConnectorOperations: dtos, NextToken: next})
}
