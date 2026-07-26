package awsconfig

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// listConnectorsPageSize bounds a single ListConnectors page, mirroring
// describeConfigRulesPageSize's role for DescribeConfigRules.
const listConnectorsPageSize = 100

// Operation name constants for the connector family.
const (
	opDeleteConnector = "DeleteConnector"
	opGetConnector    = "GetConnector"
	opListConnectors  = "ListConnectors"
	opPutConnector    = "PutConnector"
)

// connectorSupportedOps returns the operation names this family handles.
func connectorSupportedOps() []string {
	return []string{
		opPutConnector,
		opGetConnector,
		opListConnectors,
		opDeleteConnector,
	}
}

// azureConnectorConfigurationBody mirrors AzureConnectorConfiguration on the wire.
type azureConnectorConfigurationBody struct {
	ClientIdentifier string `json:"clientIdentifier"`
	TenantIdentifier string `json:"tenantIdentifier"`
}

// connectorConfigurationBody mirrors ConnectorConfiguration on the wire.
type connectorConfigurationBody struct {
	Azure *azureConnectorConfigurationBody `json:"azure,omitempty"`
}

// toModel converts the wire body to the backend's ConnectorConfiguration. A
// nil receiver (an absent "ConnectorConfiguration" in the request) returns
// nil, letting PutConnector's own required-field validation report it.
func (c *connectorConfigurationBody) toModel() *ConnectorConfiguration {
	if c == nil {
		return nil
	}

	out := &ConnectorConfiguration{}
	if c.Azure != nil {
		out.Azure = &AzureConnectorConfiguration{
			ClientIdentifier: c.Azure.ClientIdentifier,
			TenantIdentifier: c.Azure.TenantIdentifier,
		}
	}

	return out
}

type putConnectorInput struct {
	ConnectorConfiguration *connectorConfigurationBody `json:"ConnectorConfiguration"`
	Tags                   []Tag                       `json:"Tags,omitempty"`
}

type putConnectorOutput struct {
	Arn string `json:"Arn"`
}

func (h *Handler) handlePutConnector(_ context.Context, in *putConnectorInput) (*putConnectorOutput, error) {
	arn, err := h.Backend.PutConnector(in.ConnectorConfiguration.toModel(), in.Tags)
	if err != nil {
		return nil, err
	}

	return &putConnectorOutput{Arn: arn}, nil
}

type getConnectorInput struct {
	Arn string `json:"Arn"`
}

type getConnectorOutput struct {
	Connector *Connector `json:"Connector"`
}

func (h *Handler) handleGetConnector(_ context.Context, in *getConnectorInput) (*getConnectorOutput, error) {
	c, err := h.Backend.GetConnector(in.Arn)
	if err != nil {
		return nil, err
	}

	return &getConnectorOutput{Connector: c}, nil
}

type deleteConnectorInput struct {
	Arn string `json:"Arn"`
}

func (h *Handler) handleDeleteConnector(_ context.Context, in *deleteConnectorInput) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteConnector(in.Arn)
}

// connectorFilterBody mirrors ConnectorFilter on the wire.
type connectorFilterBody struct {
	FilterName   string   `json:"filterName,omitempty"`
	FilterValues []string `json:"filterValues,omitempty"`
}

type listConnectorsInput struct {
	NextToken  string                `json:"NextToken,omitempty"`
	Filters    []connectorFilterBody `json:"Filters,omitempty"`
	MaxResults int32                 `json:"MaxResults,omitempty"`
}

type listConnectorsOutput struct {
	NextToken          string             `json:"NextToken,omitempty"`
	ConnectorSummaries []ConnectorSummary `json:"ConnectorSummaries"`
}

func (h *Handler) handleListConnectors(
	_ context.Context, in *listConnectorsInput,
) (*listConnectorsOutput, error) {
	if err := page.ValidateToken(in.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}

	filters := make([]ConnectorFilter, 0, len(in.Filters))
	for _, f := range in.Filters {
		filters = append(filters, ConnectorFilter(f))
	}

	all := h.Backend.ListConnectors(filters)
	p := page.New(all, in.NextToken, int(in.MaxResults), listConnectorsPageSize)

	return &listConnectorsOutput{ConnectorSummaries: p.Data, NextToken: p.Next}, nil
}

// buildConnectorDispatch returns dispatch entries for connector ops.
func (h *Handler) buildConnectorDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opPutConnector:    service.WrapOp(h.handlePutConnector),
		opGetConnector:    service.WrapOp(h.handleGetConnector),
		opListConnectors:  service.WrapOp(h.handleListConnectors),
		opDeleteConnector: service.WrapOp(h.handleDeleteConnector),
	}
}
