package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	completenessKeyLogGroupIdentifier = "logGroupIdentifier"
	completenessKeyPolicyDocument     = "policyDocument"
	completenessStatusActive          = "ACTIVE"
)

const (
	keyName                = "name"
	keyArn                 = "arn"
	keyPolicyName          = "policyName"
	keyDeliveryDestination = "deliveryDestination"
	keyDeliverySource      = "deliverySource"
	keyDestinationName     = "destinationName"
	keyTargetArn           = "targetArn"
	keyRoleArn             = "roleArn"
	keyIntegrationName     = "integrationName"
	keyIntegrationStatus   = "integrationStatus"
	keyIntegrationType     = "integrationType"
)

// completenessActions returns dispatch entries for all previously notImplemented CloudWatch Logs operations.
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

// cwlBackend returns the InMemoryBackend, or nil if the backend is not an InMemoryBackend.
func cwlBackend(h *Handler) *InMemoryBackend {
	b, _ := h.Backend.(*InMemoryBackend)

	return b
}

// ---- DataProtectionPolicy (real — already existed) ----

type deleteDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteDataProtectionPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDataProtectionPolicy(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type getDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleGetDataProtectionPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	policyDoc := "{}"
	if b := cwlBackend(h); b != nil {
		p, err := b.GetDataProtectionPolicy(in.LogGroupIdentifier)
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

type putDataProtectionPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	PolicyDocument     string `json:"policyDocument"`
}

func (h *Handler) handlePutDataProtectionPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDataProtectionPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutDataProtectionPolicy(in.LogGroupIdentifier, in.PolicyDocument); err != nil {
			return nil, err
		}
	}

	return map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		completenessKeyPolicyDocument:     in.PolicyDocument,
	}, nil
}

// ---- ResourcePolicy ----

type putResourcePolicyInput struct {
	PolicyName     string `json:"policyName"`
	PolicyDocument string `json:"policyDocument"`
}

func (h *Handler) handlePutResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putResourcePolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		p, err := b.PutResourcePolicy(in.PolicyName, in.PolicyDocument)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"resourcePolicy": map[string]any{
				keyPolicyName:                 p.PolicyName,
				completenessKeyPolicyDocument: p.PolicyDocument,
			},
		}, nil
	}

	return map[string]any{"resourcePolicy": map[string]any{
		keyPolicyName:                 in.PolicyName,
		completenessKeyPolicyDocument: in.PolicyDocument,
	}}, nil
}

func (h *Handler) handleDescribeResourcePolicies(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		policies := b.DescribeResourcePolicies()
		out := make([]map[string]any, 0, len(policies))
		for _, p := range policies {
			out = append(out, map[string]any{
				keyPolicyName:                 p.PolicyName,
				completenessKeyPolicyDocument: p.PolicyDocument,
			})
		}

		return map[string]any{"resourcePolicies": out}, nil
	}

	return map[string]any{"resourcePolicies": []any{}}, nil
}

type deleteResourcePolicyInput struct {
	PolicyName string `json:"policyName"`
}

func (h *Handler) handleDeleteResourcePolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteResourcePolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteResourcePolicy(in.PolicyName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- DeliveryDestination ----

type putDeliveryDestinationInputFull struct {
	Name         string            `json:"name"`
	OutputFormat string            `json:"outputFormat,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Config       struct {
		DestinationResourceArn string `json:"destinationResourceArn"`
	} `json:"deliveryDestinationConfiguration"`
}

func (h *Handler) handlePutDeliveryDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDeliveryDestinationInputFull
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		dest, err := b.PutDeliveryDestination(in.Name, in.Config.DestinationResourceArn, in.OutputFormat, in.Tags)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliveryDestination: map[string]any{
			keyName:        dest.Name,
			keyArn:         dest.Arn,
			"outputFormat": dest.OutputFormat,
		}}, nil
	}

	return map[string]any{keyDeliveryDestination: map[string]any{}}, nil
}

type getDeliveryDestinationInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleGetDeliveryDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDeliveryDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		dest, err := b.GetDeliveryDestination(in.Name)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliveryDestination: map[string]any{
			keyName:        dest.Name,
			keyArn:         dest.Arn,
			"outputFormat": dest.OutputFormat,
		}}, nil
	}

	return map[string]any{keyDeliveryDestination: map[string]any{}}, nil
}

func (h *Handler) handleDescribeDeliveryDestinations(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		dests := b.DescribeDeliveryDestinations()
		out := make([]map[string]any, 0, len(dests))
		for _, d := range dests {
			out = append(out, map[string]any{keyName: d.Name, keyArn: d.Arn})
		}

		return map[string]any{"deliveryDestinations": out}, nil
	}

	return map[string]any{"deliveryDestinations": []any{}}, nil
}

type deleteDeliveryDestinationInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeleteDeliveryDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDeliveryDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDeliveryDestination(in.Name); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type putDeliveryDestinationPolicyInput struct {
	DeliveryDestinationName   string `json:"deliveryDestinationName"`
	DeliveryDestinationPolicy string `json:"deliveryDestinationPolicy"`
}

func (h *Handler) handlePutDeliveryDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDeliveryDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutDeliveryDestinationPolicy(in.DeliveryDestinationName, in.DeliveryDestinationPolicy); err != nil {
			return nil, err
		}
	}

	return map[string]any{"policy": map[string]any{
		"deliveryDestinationPolicy": in.DeliveryDestinationPolicy,
	}}, nil
}

type getDeliveryDestinationPolicyInput struct {
	DeliveryDestinationName string `json:"deliveryDestinationName"`
}

func (h *Handler) handleGetDeliveryDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDeliveryDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	policy := ""
	if b := cwlBackend(h); b != nil {
		p, err := b.GetDeliveryDestinationPolicy(in.DeliveryDestinationName)
		if err != nil {
			return nil, err
		}
		policy = p
	}

	return map[string]any{"policy": map[string]any{"deliveryDestinationPolicy": policy}}, nil
}

type deleteDeliveryDestinationPolicyInput struct {
	DeliveryDestinationName string `json:"deliveryDestinationName"`
}

func (h *Handler) handleDeleteDeliveryDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDeliveryDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDeliveryDestinationPolicy(in.DeliveryDestinationName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- DeliverySource ----

type putDeliverySourceInput struct {
	Tags         map[string]string `json:"tags,omitempty"`
	Name         string            `json:"name"`
	LogType      string            `json:"logType,omitempty"`
	ResourceArns []string          `json:"resourceArns,omitempty"`
}

func (h *Handler) handlePutDeliverySource(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDeliverySourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		src, err := b.PutDeliverySource(in.Name, in.LogType, in.ResourceArns, in.Tags)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliverySource: map[string]any{
			keyName: src.Name,
			keyArn:  src.Arn,
		}}, nil
	}

	return map[string]any{keyDeliverySource: map[string]any{}}, nil
}

type getDeliverySourceInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleGetDeliverySource(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDeliverySourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		src, err := b.GetDeliverySource(in.Name)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliverySource: map[string]any{
			keyName: src.Name,
			keyArn:  src.Arn,
		}}, nil
	}

	return map[string]any{keyDeliverySource: map[string]any{}}, nil
}

func (h *Handler) handleDescribeDeliverySources(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		srcs := b.DescribeDeliverySources()
		out := make([]map[string]any, 0, len(srcs))
		for _, s := range srcs {
			out = append(out, map[string]any{keyName: s.Name, keyArn: s.Arn})
		}

		return map[string]any{"deliverySources": out}, nil
	}

	return map[string]any{"deliverySources": []any{}}, nil
}

type deleteDeliverySourceInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeleteDeliverySource(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDeliverySourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDeliverySource(in.Name); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- Destination (log routing) ----

type putDestinationInput struct {
	DestinationName string `json:"destinationName"`
	TargetArn       string `json:"targetArn"`
	RoleArn         string `json:"roleArn"`
}

func (h *Handler) handlePutDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		dest, err := b.PutDestination(in.DestinationName, in.TargetArn, in.RoleArn)
		if err != nil {
			return nil, err
		}

		return map[string]any{"destination": map[string]any{
			keyDestinationName: dest.DestinationName,
			keyTargetArn:       dest.TargetArn,
			keyRoleArn:         dest.RoleArn,
			keyArn:             dest.Arn,
		}}, nil
	}

	return map[string]any{"destination": map[string]any{
		keyDestinationName: in.DestinationName,
		keyTargetArn:       in.TargetArn,
		keyRoleArn:         in.RoleArn,
		keyArn:             "",
	}}, nil
}

type putDestinationPolicyInput struct {
	DestinationName string `json:"destinationName"`
	AccessPolicy    string `json:"accessPolicy"`
}

func (h *Handler) handlePutDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutDestinationPolicy(in.DestinationName, in.AccessPolicy); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type describeDestinationsInput struct {
	DestinationNamePrefix string `json:"DestinationNamePrefix,omitempty"`
}

func (h *Handler) handleDescribeDestinations(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in describeDestinationsInput
	_ = json.Unmarshal(body, &in)

	if b := cwlBackend(h); b != nil {
		dests := b.DescribeDestinations(in.DestinationNamePrefix)
		out := make([]map[string]any, 0, len(dests))
		for _, d := range dests {
			out = append(out, map[string]any{
				keyDestinationName: d.DestinationName,
				keyTargetArn:       d.TargetArn,
				keyRoleArn:         d.RoleArn,
				keyArn:             d.Arn,
			})
		}

		return map[string]any{"destinations": out}, nil
	}

	return map[string]any{"destinations": []any{}}, nil
}

type deleteDestinationInput struct {
	DestinationName string `json:"destinationName"`
}

func (h *Handler) handleDeleteDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDestination(in.DestinationName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- IndexPolicy ----

type putIndexPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	PolicyDocument     string `json:"policyDocument"`
}

func (h *Handler) handlePutIndexPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putIndexPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		p, err := b.PutIndexPolicy(in.LogGroupIdentifier, in.PolicyDocument)
		if err != nil {
			return nil, err
		}

		return map[string]any{"indexPolicy": map[string]any{
			completenessKeyLogGroupIdentifier: p.LogGroupIdentifier,
			completenessKeyPolicyDocument:     p.PolicyDocument,
		}}, nil
	}

	return map[string]any{"indexPolicy": map[string]any{}}, nil
}

func (h *Handler) handleDescribeIndexPolicies(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		policies := b.DescribeIndexPolicies()
		out := make([]map[string]any, 0, len(policies))
		for _, p := range policies {
			out = append(out, map[string]any{
				completenessKeyLogGroupIdentifier: p.LogGroupIdentifier,
				completenessKeyPolicyDocument:     p.PolicyDocument,
			})
		}

		return map[string]any{"indexPolicies": out}, nil
	}

	return map[string]any{"indexPolicies": []any{}}, nil
}

type deleteIndexPolicyInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteIndexPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteIndexPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteIndexPolicy(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- Transformer ----

type putTransformerInput struct {
	LogGroupIdentifier string           `json:"logGroupIdentifier"`
	TransformerConfig  []map[string]any `json:"transformerConfig"`
}

func (h *Handler) handlePutTransformer(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putTransformerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutTransformer(in.LogGroupIdentifier, in.TransformerConfig); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type getTransformerInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleGetTransformer(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getTransformerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		t, err := b.GetTransformer(in.LogGroupIdentifier)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			completenessKeyLogGroupIdentifier: t.LogGroupIdentifier,
			"transformerConfig":               t.Processors,
		}, nil
	}

	return map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		"transformerConfig":               []any{},
	}, nil
}

type deleteTransformerInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteTransformer(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteTransformerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteTransformer(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- Integration ----

type putIntegrationInput struct {
	IntegrationName string `json:"integrationName"`
	IntegrationType string `json:"integrationType"`
}

func (h *Handler) handlePutIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		ig, err := b.PutIntegration(in.IntegrationName, in.IntegrationType)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			keyIntegrationName:   ig.Name,
			keyIntegrationStatus: ig.Status,
		}, nil
	}

	return map[string]any{
		keyIntegrationName:   in.IntegrationName,
		keyIntegrationStatus: completenessStatusActive,
	}, nil
}

type getIntegrationInput struct {
	IntegrationName string `json:"integrationName"`
}

func (h *Handler) handleGetIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		ig, err := b.GetIntegration(in.IntegrationName)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			keyIntegrationName:   ig.Name,
			keyIntegrationType:   ig.Type,
			keyIntegrationStatus: ig.Status,
		}, nil
	}

	return map[string]any{
		keyIntegrationName:   "",
		keyIntegrationType:   "",
		keyIntegrationStatus: completenessStatusActive,
	}, nil
}

func (h *Handler) handleListIntegrations(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		igs := b.ListIntegrations()
		out := make([]map[string]any, 0, len(igs))
		for _, ig := range igs {
			out = append(out, map[string]any{
				keyIntegrationName:   ig.Name,
				keyIntegrationType:   ig.Type,
				keyIntegrationStatus: ig.Status,
			})
		}

		return map[string]any{"integrationSummaries": out}, nil
	}

	return map[string]any{"integrationSummaries": []any{}}, nil
}

type deleteIntegrationInput struct {
	IntegrationName string `json:"integrationName"`
}

func (h *Handler) handleDeleteIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteIntegration(in.IntegrationName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- LogGroupDeletionProtection ----

type putLogGroupDeletionProtectionInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	DeletionProtected  bool   `json:"deletionProtected"`
}

func (h *Handler) handlePutLogGroupDeletionProtection(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putLogGroupDeletionProtectionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.SetLogGroupDeletionProtection(in.LogGroupIdentifier, in.DeletionProtected); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- UpdateDeliveryConfiguration ----

type updateDeliveryConfigurationInput struct {
	ID             string   `json:"id"`
	FieldDelimiter string   `json:"fieldDelimiter,omitempty"`
	RecordFields   []string `json:"recordFields,omitempty"`
}

func (h *Handler) handleUpdateDeliveryConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in updateDeliveryConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.UpdateDeliveryConfiguration(in.ID, in.FieldDelimiter, in.RecordFields); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// ---- Stubs (no meaningful state to track) ----

func (h *Handler) handleDescribeConfigurationTemplates(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"configurationTemplates": []any{}}, nil
}

func (h *Handler) handleDescribeFieldIndexes(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"fieldIndexes": []any{}}, nil
}

func (h *Handler) handleDescribeImportTaskBatches(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"importTaskBatches": []any{}}, nil
}

func (h *Handler) handleDisassociateSourceFromS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleGetLogFields(ctx context.Context, _ []byte) (any, error) { //nolint:revive // existing issue.
	return map[string]any{"logRecordPointer": "", "logRecord": map[string]any{}}, nil
}

func (h *Handler) handleGetLogObject(ctx context.Context, _ []byte) (any, error) { //nolint:revive // existing issue.
	return map[string]any{}, nil
}

func (h *Handler) handleListAggregateLogGroupSummaries(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"logGroupSummaries": []any{}}, nil
}

func (h *Handler) handleListSourcesForS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"sources": []any{}}, nil
}

func (h *Handler) handlePutBearerTokenAuthentication(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleStartLiveTail(ctx context.Context, _ []byte) (any, error) { //nolint:revive // existing issue.
	return map[string]any{"responseStream": map[string]any{}}, nil
}

func (h *Handler) handleTestTransformer(ctx context.Context, _ []byte) (any, error) { //nolint:revive // existing issue.
	return map[string]any{"transformedLogs": []any{}}, nil
}
