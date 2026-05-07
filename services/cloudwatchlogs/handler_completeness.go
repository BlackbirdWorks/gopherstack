package cloudwatchlogs

import (
	"encoding/json"
	"fmt"
)

const (
	completenessKeyLogGroupIdentifier = "logGroupIdentifier"
	completenessKeyPolicyDocument     = "policyDocument"
	completenessStatusActive          = "ACTIVE"
)

// completenessActions returns stub dispatch entries for all previously
// notImplemented CloudWatch Logs operations.
func (h *Handler) completenessActions() map[string]actionFn {
	return map[string]actionFn{
		"DeleteDataProtectionPolicy":               h.handleDeleteDataProtectionPolicy,
		"DeleteDeliveryDestination":                h.handleDeleteDeliveryDestination,
		"DeleteDeliveryDestinationPolicy":          h.handleDeleteDeliveryDestinationPolicy,
		"DeleteDeliverySource":                     h.handleDeleteDeliverySource,
		"DeleteDestination":                        h.handleDeleteDestination,
		"DeleteIndexPolicy":                        h.handleDeleteIndexPolicy,
		"DeleteIntegration":                        h.handleDeleteIntegration,
		"DeleteResourcePolicy":                     h.handleDeleteResourcePolicy,
		"DeleteTransformer":                        h.handleDeleteTransformer,
		"DescribeConfigurationTemplates":           h.handleDescribeConfigurationTemplates,
		"DescribeDeliveryDestinations":             h.handleDescribeDeliveryDestinations,
		"DescribeDeliverySources":                  h.handleDescribeDeliverySources,
		"DescribeDestinations":                     h.handleDescribeDestinations,
		"DescribeFieldIndexes":                     h.handleDescribeFieldIndexes,
		"DescribeImportTaskBatches":                h.handleDescribeImportTaskBatches,
		"DescribeIndexPolicies":                    h.handleDescribeIndexPolicies,
		"DescribeResourcePolicies":                 h.handleDescribeResourcePolicies,
		"DisassociateSourceFromS3TableIntegration": h.handleDisassociateSourceFromS3TableIntegration,
		"GetDataProtectionPolicy":                  h.handleGetDataProtectionPolicy,
		"GetDeliveryDestination":                   h.handleGetDeliveryDestination,
		"GetDeliveryDestinationPolicy":             h.handleGetDeliveryDestinationPolicy,
		"GetDeliverySource":                        h.handleGetDeliverySource,
		"GetIntegration":                           h.handleGetIntegration,
		"GetLogFields":                             h.handleGetLogFields,
		"GetLogObject":                             h.handleGetLogObject,
		"GetTransformer":                           h.handleGetTransformer,
		"ListAggregateLogGroupSummaries":           h.handleListAggregateLogGroupSummaries,
		"ListIntegrations":                         h.handleListIntegrations,
		"ListSourcesForS3TableIntegration":         h.handleListSourcesForS3TableIntegration,
		"PutBearerTokenAuthentication":             h.handlePutBearerTokenAuthentication,
		"PutDataProtectionPolicy":                  h.handlePutDataProtectionPolicy,
		"PutDeliveryDestination":                   h.handlePutDeliveryDestination,
		"PutDeliveryDestinationPolicy":             h.handlePutDeliveryDestinationPolicy,
		"PutDeliverySource":                        h.handlePutDeliverySource,
		"PutDestination":                           h.handlePutDestination,
		"PutDestinationPolicy":                     h.handlePutDestinationPolicy,
		"PutIndexPolicy":                           h.handlePutIndexPolicy,
		"PutIntegration":                           h.handlePutIntegration,
		"PutLogGroupDeletionProtection":            h.handlePutLogGroupDeletionProtection,
		"PutResourcePolicy":                        h.handlePutResourcePolicy,
		"PutTransformer":                           h.handlePutTransformer,
		"StartLiveTail":                            h.handleStartLiveTail,
		"TestTransformer":                          h.handleTestTransformer,
		"UpdateDeliveryConfiguration":              h.handleUpdateDeliveryConfiguration,
	}
}

// ----- Stubs -----

type deleteDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteDataProtectionPolicy(body []byte) (any, error) {
	var in deleteDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if dp, ok := h.Backend.(*InMemoryBackend); ok {
		if err := dp.DeleteDataProtectionPolicy(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

func (h *Handler) handleDeleteDeliveryDestination(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteDeliveryDestinationPolicy(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteDeliverySource(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteDestination(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteIndexPolicy(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteIntegration(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteResourcePolicy(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDeleteTransformer(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDescribeConfigurationTemplates(_ []byte) (any, error) {
	return map[string]any{"configurationTemplates": []any{}}, nil
}

func (h *Handler) handleDescribeDeliveryDestinations(_ []byte) (any, error) {
	return map[string]any{"deliveryDestinations": []any{}}, nil
}

func (h *Handler) handleDescribeDeliverySources(_ []byte) (any, error) {
	return map[string]any{"deliverySources": []any{}}, nil
}

func (h *Handler) handleDescribeDestinations(_ []byte) (any, error) {
	return map[string]any{"destinations": []any{}}, nil
}

func (h *Handler) handleDescribeFieldIndexes(_ []byte) (any, error) {
	return map[string]any{"fieldIndexes": []any{}}, nil
}

func (h *Handler) handleDescribeImportTaskBatches(_ []byte) (any, error) {
	return map[string]any{"importTaskBatches": []any{}}, nil
}

func (h *Handler) handleDescribeIndexPolicies(_ []byte) (any, error) {
	return map[string]any{"indexPolicies": []any{}}, nil
}

func (h *Handler) handleDescribeResourcePolicies(_ []byte) (any, error) {
	return map[string]any{"resourcePolicies": []any{}}, nil
}

func (h *Handler) handleDisassociateSourceFromS3TableIntegration(_ []byte) (any, error) {
	return struct{}{}, nil
}

type getDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleGetDataProtectionPolicy(body []byte) (any, error) {
	var in getDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	policyDoc := "{}"
	if dp, ok := h.Backend.(*InMemoryBackend); ok {
		p, err := dp.GetDataProtectionPolicy(in.LogGroupIdentifier)
		if err != nil {
			return nil, err
		}
		policyDoc = p
	}

	return map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		completenessKeyPolicyDocument:     policyDoc,
	}, nil
}

func (h *Handler) handleGetDeliveryDestination(_ []byte) (any, error) {
	return map[string]any{"deliveryDestination": map[string]any{}}, nil
}

func (h *Handler) handleGetDeliveryDestinationPolicy(_ []byte) (any, error) {
	return map[string]any{"policy": map[string]any{}}, nil
}

func (h *Handler) handleGetDeliverySource(_ []byte) (any, error) {
	return map[string]any{"deliverySource": map[string]any{}}, nil
}

func (h *Handler) handleGetIntegration(_ []byte) (any, error) {
	return map[string]any{
		"integrationName":   "",
		"integrationType":   "",
		"integrationStatus": completenessStatusActive,
	}, nil
}

func (h *Handler) handleGetLogFields(_ []byte) (any, error) {
	return map[string]any{"logRecordPointer": "", "logRecord": map[string]any{}}, nil
}

func (h *Handler) handleGetLogObject(_ []byte) (any, error) {
	return map[string]any{}, nil
}

func (h *Handler) handleGetTransformer(_ []byte) (any, error) {
	return map[string]any{
		completenessKeyLogGroupIdentifier: "",
		"processors":                      []any{},
	}, nil
}

func (h *Handler) handleListAggregateLogGroupSummaries(_ []byte) (any, error) {
	return map[string]any{"logGroupSummaries": []any{}}, nil
}

func (h *Handler) handleListIntegrations(_ []byte) (any, error) {
	return map[string]any{"integrationSummaries": []any{}}, nil
}

func (h *Handler) handleListSourcesForS3TableIntegration(_ []byte) (any, error) {
	return map[string]any{"sources": []any{}}, nil
}

func (h *Handler) handlePutBearerTokenAuthentication(_ []byte) (any, error) {
	return struct{}{}, nil
}

type putDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	PolicyDocument     string `json:"policyDocument"`
}

func (h *Handler) handlePutDataProtectionPolicy(body []byte) (any, error) {
	var in putDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if dp, ok := h.Backend.(*InMemoryBackend); ok {
		if err := dp.PutDataProtectionPolicy(in.LogGroupIdentifier, in.PolicyDocument); err != nil {
			return nil, err
		}
	}

	return map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		completenessKeyPolicyDocument:     in.PolicyDocument,
	}, nil
}

func (h *Handler) handlePutDeliveryDestination(_ []byte) (any, error) {
	return map[string]any{"deliveryDestination": map[string]any{}}, nil
}

func (h *Handler) handlePutDeliveryDestinationPolicy(_ []byte) (any, error) {
	return map[string]any{"policy": map[string]any{}}, nil
}

func (h *Handler) handlePutDeliverySource(_ []byte) (any, error) {
	return map[string]any{"deliverySource": map[string]any{}}, nil
}

func (h *Handler) handlePutDestination(_ []byte) (any, error) {
	return map[string]any{"destination": map[string]any{
		"destinationName": "",
		"targetArn":       "",
		"roleArn":         "",
		"arn":             "",
	}}, nil
}

func (h *Handler) handlePutDestinationPolicy(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handlePutIndexPolicy(_ []byte) (any, error) {
	return map[string]any{"indexPolicy": map[string]any{}}, nil
}

func (h *Handler) handlePutIntegration(_ []byte) (any, error) {
	return map[string]any{
		"integrationName":   "",
		"integrationStatus": completenessStatusActive,
	}, nil
}

func (h *Handler) handlePutLogGroupDeletionProtection(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handlePutResourcePolicy(_ []byte) (any, error) {
	return map[string]any{"resourcePolicy": map[string]any{
		"policyName":                  "",
		completenessKeyPolicyDocument: "{}",
	}}, nil
}

func (h *Handler) handlePutTransformer(_ []byte) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleStartLiveTail(_ []byte) (any, error) {
	return map[string]any{"responseStream": map[string]any{}}, nil
}

func (h *Handler) handleTestTransformer(_ []byte) (any, error) {
	return map[string]any{"transformedLogs": []any{}}, nil
}

func (h *Handler) handleUpdateDeliveryConfiguration(_ []byte) (any, error) {
	return struct{}{}, nil
}
