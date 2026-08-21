package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// PartnerApp handlers
// ---------------------------------------------------------------------------

// createPartnerAppInput mirrors CreatePartnerAppInput
// (api_op_CreatePartnerApp.go:36-93): AuthType/ExecutionRoleArn/Name/Tier/Type
// are all required. ClientToken is deliberately omitted — see
// CreatePartnerAppOptions' doc comment.
type createPartnerAppInput struct {
	MaintenanceConfig             *PartnerAppMaintenanceConfig `json:"MaintenanceConfig,omitempty"`
	Name                          string                       `json:"Name"`
	Type                          string                       `json:"Type"`
	ExecutionRoleArn              string                       `json:"ExecutionRoleArn,omitempty"`
	AuthType                      string                       `json:"AuthType,omitempty"`
	Tier                          string                       `json:"Tier,omitempty"`
	KmsKeyID                      string                       `json:"KmsKeyId,omitempty"`
	Tags                          []tagObject                  `json:"Tags"`
	ApplicationConfig             json.RawMessage              `json:"ApplicationConfig,omitempty"`
	EnableAutoMinorVersionUpgrade bool                         `json:"EnableAutoMinorVersionUpgrade,omitempty"`
	EnableIamSessionBasedIdentity bool                         `json:"EnableIamSessionBasedIdentity,omitempty"`
}

func (h *Handler) handleCreatePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req createPartnerAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.AuthType == "" {
		return nil, fmt.Errorf("%w: AuthType is required", errInvalidRequest)
	}

	if req.ExecutionRoleArn == "" {
		return nil, fmt.Errorf("%w: ExecutionRoleArn is required", errInvalidRequest)
	}

	if req.Tier == "" {
		return nil, fmt.Errorf("%w: Tier is required", errInvalidRequest)
	}

	if req.Type == "" {
		return nil, fmt.Errorf("%w: Type is required", errInvalidRequest)
	}

	result, err := h.Backend.CreatePartnerApp(ctx, CreatePartnerAppOptions{
		Name:                          req.Name,
		Type:                          req.Type,
		ExecutionRoleArn:              req.ExecutionRoleArn,
		AuthType:                      req.AuthType,
		Tier:                          req.Tier,
		KmsKeyID:                      req.KmsKeyID,
		ApplicationConfig:             req.ApplicationConfig,
		MaintenanceConfig:             req.MaintenanceConfig,
		EnableAutoMinorVersionUpgrade: req.EnableAutoMinorVersionUpgrade,
		EnableIamSessionBasedIdentity: req.EnableIamSessionBasedIdentity,
		Tags:                          fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

// describePartnerAppResponse is the response body for DescribePartnerApp
// (api_op_DescribePartnerApp.go:36-125). AvailableUpgrade,
// CurrentVersionEolDate, and Version are disclosed, not modeled: this
// backend tracks no minor-version-upgrade catalog, so there is never an
// available upgrade, an EOL date, or a version to report. Error
// (types.ErrorInfo) is likewise disclosed: Status never reaches
// Failed/UpdateFailed in this backend, so there is never a failure to
// describe.
type describePartnerAppResponse struct {
	MaintenanceConfig             *PartnerAppMaintenanceConfig `json:"MaintenanceConfig,omitempty"`
	Tier                          string                       `json:"Tier,omitempty"`
	BaseURL                       string                       `json:"BaseUrl,omitempty"`
	Type                          string                       `json:"Type,omitempty"`
	ExecutionRoleArn              string                       `json:"ExecutionRoleArn,omitempty"`
	AuthType                      string                       `json:"AuthType,omitempty"`
	Arn                           string                       `json:"Arn"`
	KmsKeyID                      string                       `json:"KmsKeyId,omitempty"`
	Status                        string                       `json:"Status"`
	Name                          string                       `json:"Name"`
	ApplicationConfig             json.RawMessage              `json:"ApplicationConfig,omitempty"`
	CreationTime                  float64                      `json:"CreationTime"`
	LastModifiedTime              float64                      `json:"LastModifiedTime"`
	EnableAutoMinorVersionUpgrade bool                         `json:"EnableAutoMinorVersionUpgrade,omitempty"`
	EnableIamSessionBasedIdentity bool                         `json:"EnableIamSessionBasedIdentity,omitempty"`
}

// describePartnerAppInput mirrors DescribePartnerAppInput
// (api_op_DescribePartnerApp.go:24-36): Arn is required,
// IncludeAvailableUpgrade optional. IncludeAvailableUpgrade is decoded for
// wire-shape fidelity but is a disclosed no-op — see
// describePartnerAppResponse's doc comment.
type describePartnerAppInput struct {
	Arn                     string `json:"Arn"`
	IncludeAvailableUpgrade bool   `json:"IncludeAvailableUpgrade,omitempty"`
}

func (h *Handler) handleDescribePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req describePartnerAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribePartnerApp(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describePartnerAppResponse{
		Arn:                           result.Arn,
		Name:                          result.Name,
		Status:                        result.Status,
		Type:                          result.Type,
		ExecutionRoleArn:              result.ExecutionRoleArn,
		AuthType:                      result.AuthType,
		Tier:                          result.Tier,
		KmsKeyID:                      result.KmsKeyID,
		BaseURL:                       result.BaseURL,
		ApplicationConfig:             result.ApplicationConfig,
		MaintenanceConfig:             result.MaintenanceConfig,
		EnableAutoMinorVersionUpgrade: result.EnableAutoMinorVersionUpgrade,
		EnableIamSessionBasedIdentity: result.EnableIamSessionBasedIdentity,
		CreationTime:                  epochSeconds(result.CreationTime),
		LastModifiedTime:              epochSeconds(result.LastModifiedTime),
	})
}

// deletePartnerAppInput mirrors DeletePartnerAppInput
// (api_op_DeletePartnerApp.go:24-33): Arn is required. ClientToken is
// omitted — see CreatePartnerAppOptions' doc comment.
type deletePartnerAppInput struct {
	Arn string `json:"Arn"`
}

func (h *Handler) handleDeletePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req deletePartnerAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePartnerApp(ctx, req.Arn); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyGenericArn: req.Arn})
}

// updatePartnerAppInput mirrors UpdatePartnerAppInput
// (api_op_UpdatePartnerApp.go:24-71): Arn is required, every other member
// optional. AppVersion is decoded for wire-shape fidelity but is a disclosed
// no-op — see UpdatePartnerAppOptions' doc comment. ClientToken is omitted
// for the same reason as CreatePartnerApp's.
type updatePartnerAppInput struct {
	MaintenanceConfig             *PartnerAppMaintenanceConfig `json:"MaintenanceConfig,omitempty"`
	EnableAutoMinorVersionUpgrade *bool                        `json:"EnableAutoMinorVersionUpgrade,omitempty"`
	EnableIamSessionBasedIdentity *bool                        `json:"EnableIamSessionBasedIdentity,omitempty"`
	Arn                           string                       `json:"Arn"`
	AppVersion                    string                       `json:"AppVersion,omitempty"`
	Tier                          string                       `json:"Tier,omitempty"`
	Tags                          []tagObject                  `json:"Tags"`
	ApplicationConfig             json.RawMessage              `json:"ApplicationConfig,omitempty"`
}

func (h *Handler) handleUpdatePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req updatePartnerAppInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdatePartnerApp(ctx, UpdatePartnerAppOptions{
		Arn:                           req.Arn,
		Tier:                          req.Tier,
		ApplicationConfig:             req.ApplicationConfig,
		MaintenanceConfig:             req.MaintenanceConfig,
		Tags:                          fromTagObjects(req.Tags),
		EnableAutoMinorVersionUpgrade: req.EnableAutoMinorVersionUpgrade,
		EnableIamSessionBasedIdentity: req.EnableIamSessionBasedIdentity,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

// listPartnerAppsInput mirrors ListPartnerAppsInput
// (api_op_ListPartnerApps.go:24-38), both members optional.
type listPartnerAppsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

func (h *Handler) handleListPartnerApps(ctx context.Context, body []byte) ([]byte, error) {
	var req listPartnerAppsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListPartnerApps(ctx, req.NextToken, req.MaxResults)

	items := make([]map[string]any, 0, len(apps))
	for _, p := range apps {
		items = append(items, map[string]any{
			keyGenericArn:   p.Arn,
			keyGenericName:  p.Name,
			keyStatus:       p.Status,
			"Type":          p.Type,
			keyCreationTime: epochSeconds(p.CreationTime),
		})
	}

	return listResp("Summaries", items, nextToken)
}

// createPartnerAppPresignedURLInput mirrors CreatePartnerAppPresignedUrlInput
// (api_op_CreatePartnerAppPresignedUrl.go:24-38): Arn is required.
// ExpiresInSeconds/SessionExpirationDurationInSeconds are decoded for
// wire-shape fidelity but are a disclosed no-op: the response is a bare
// {Url}, and this backend's synthesized URL
// (CreatePartnerAppPresignedURL) carries no verified real query-parameter
// format to encode an expiry into.
type createPartnerAppPresignedURLInput struct {
	Arn                                string `json:"Arn"`
	ExpiresInSeconds                   int32  `json:"ExpiresInSeconds,omitempty"`
	SessionExpirationDurationInSeconds int32  `json:"SessionExpirationDurationInSeconds,omitempty"`
}

func (h *Handler) handleCreatePartnerAppPresignedURL(ctx context.Context, body []byte) ([]byte, error) {
	var req createPartnerAppPresignedURLInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePartnerAppPresignedURL(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyURL: url})
}
