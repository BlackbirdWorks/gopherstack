package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Domain handlers
// ---------------------------------------------------------------------------

// createDomainInput is the CreateDomain request shape (named, not inline, so
// wire-field-audit tooling that only inspects named types can see it — see
// gopherstack-oc9v). DefaultUserSettings/DefaultSpaceSettings/DomainSettings
// are carried as opaque json.RawMessage passthrough per this file's
// established convention (algorithms.go, ai_workload_configs.go).
type createDomainInput struct {
	DomainName                 string          `json:"DomainName"`
	AuthMode                   string          `json:"AuthMode"`
	AppNetworkAccessType       string          `json:"AppNetworkAccessType"`
	AppSecurityGroupManagement string          `json:"AppSecurityGroupManagement"`
	HomeEfsFileSystemCreation  string          `json:"HomeEfsFileSystemCreation"`
	KmsKeyID                   string          `json:"KmsKeyId"`
	VpcID                      string          `json:"VpcId"`
	TagPropagation             string          `json:"TagPropagation"`
	SubnetIDs                  []string        `json:"SubnetIds"`
	DefaultUserSettings        json.RawMessage `json:"DefaultUserSettings"`
	DefaultSpaceSettings       json.RawMessage `json:"DefaultSpaceSettings"`
	DomainSettings             json.RawMessage `json:"DomainSettings"`
	Tags                       []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req createDomainInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}
	// DefaultUserSettings is "This member is required" on CreateDomainInput
	// in the real API — reject early rather than silently creating a domain
	// with no user-settings baseline at all.
	if len(req.DefaultUserSettings) == 0 {
		return nil, fmt.Errorf("%w: DefaultUserSettings is required", errInvalidRequest)
	}

	d, err := h.Backend.CreateDomain(ctx, req.DomainName, req.AuthMode, fromTagObjects(req.Tags), CreateDomainOptions{
		AppNetworkAccessType:       req.AppNetworkAccessType,
		AppSecurityGroupManagement: req.AppSecurityGroupManagement,
		HomeEfsFileSystemCreation:  req.HomeEfsFileSystemCreation,
		KmsKeyID:                   req.KmsKeyID,
		VpcID:                      req.VpcID,
		TagPropagation:             req.TagPropagation,
		SubnetIDs:                  req.SubnetIDs,
		DefaultUserSettings:        req.DefaultUserSettings,
		DefaultSpaceSettings:       req.DefaultSpaceSettings,
		DomainSettings:             req.DomainSettings,
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created domain", "name", d.DomainName, "id", d.DomainID)

	return json.Marshal(
		map[string]string{keyDomainArn: d.DomainArn, keyDomainID: d.DomainID, keyURL: d.URL},
	)
}

type describeDomainInput struct {
	DomainID string `json:"DomainId"`
}

func (h *Handler) handleDescribeDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req describeDomainInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	d, err := h.Backend.DescribeDomain(ctx, req.DomainID)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyDomainID:         d.DomainID,
		keyDomainArn:        d.DomainArn,
		"DomainName":        d.DomainName,
		keyStatus:           d.Status,
		"AuthMode":          d.AuthMode,
		"Url":               d.URL,
		keyCreationTime:     epochSeconds(d.CreationTime),
		keyLastModifiedTime: epochSeconds(d.LastModifiedTime),
	}

	if d.AppNetworkAccessType != "" {
		resp["AppNetworkAccessType"] = d.AppNetworkAccessType
	}

	if d.AppSecurityGroupManagement != "" {
		resp["AppSecurityGroupManagement"] = d.AppSecurityGroupManagement
	}

	if d.HomeEfsFileSystemCreation != "" {
		resp["HomeEfsFileSystemCreation"] = d.HomeEfsFileSystemCreation
	}

	if d.KmsKeyID != "" {
		resp["KmsKeyId"] = d.KmsKeyID
	}

	if d.VpcID != "" {
		resp["VpcId"] = d.VpcID
	}

	if d.TagPropagation != "" {
		resp["TagPropagation"] = d.TagPropagation
	}

	if len(d.SubnetIDs) > 0 {
		resp["SubnetIds"] = d.SubnetIDs
	}

	if len(d.DefaultUserSettings) > 0 {
		resp["DefaultUserSettings"] = d.DefaultUserSettings
	}

	if len(d.DefaultSpaceSettings) > 0 {
		resp["DefaultSpaceSettings"] = d.DefaultSpaceSettings
	}

	if len(d.DomainSettings) > 0 {
		resp["DomainSettings"] = d.DomainSettings
	}

	return json.Marshal(resp)
}

type domainSummary struct {
	DomainID     string  `json:"DomainId"`
	DomainArn    string  `json:"DomainArn"`
	DomainName   string  `json:"DomainName"`
	Status       string  `json:"Status"`
	CreationTime float64 `json:"CreationTime"`
}

type listDomainsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

func (h *Handler) handleListDomains(ctx context.Context, body []byte) ([]byte, error) {
	var req listDomainsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	domains, nextToken := h.Backend.ListDomains(ctx, req.NextToken, req.MaxResults)
	summaries := make([]domainSummary, 0, len(domains))

	for _, d := range domains {
		summaries = append(summaries, domainSummary{
			DomainID:     d.DomainID,
			DomainArn:    d.DomainArn,
			DomainName:   d.DomainName,
			Status:       d.Status,
			CreationTime: epochSeconds(d.CreationTime),
		})
	}

	resp := map[string]any{"Domains": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

type deleteDomainInput struct {
	DomainID string `json:"DomainId"`
}

func (h *Handler) handleDeleteDomain(ctx context.Context, body []byte) error {
	var req deleteDomainInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteDomain(ctx, req.DomainID); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted domain", "id", req.DomainID)

	return nil
}

// updateDomainInput is the UpdateDomain request shape. Every field besides
// DomainId is optional per UpdateDomainInput's real shape and applied as a
// partial update (see UpdateDomainOptions) — DomainSettingsForUpdate is a
// distinct, separately-shaped type from Create's DomainSettings, so it gets
// its own opaque passthrough field on the wire.
type updateDomainInput struct {
	DomainID                   string          `json:"DomainId"`
	AppNetworkAccessType       string          `json:"AppNetworkAccessType"`
	AppSecurityGroupManagement string          `json:"AppSecurityGroupManagement"`
	HomeEfsFileSystemCreation  string          `json:"HomeEfsFileSystemCreation"`
	TagPropagation             string          `json:"TagPropagation"`
	VpcID                      string          `json:"VpcId"`
	SubnetIDs                  []string        `json:"SubnetIds"`
	DefaultUserSettings        json.RawMessage `json:"DefaultUserSettings"`
	DefaultSpaceSettings       json.RawMessage `json:"DefaultSpaceSettings"`
	DomainSettingsForUpdate    json.RawMessage `json:"DomainSettingsForUpdate"`
}

func (h *Handler) handleUpdateDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req updateDomainInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	d, err := h.Backend.UpdateDomain(ctx, req.DomainID, UpdateDomainOptions{
		AppNetworkAccessType:       req.AppNetworkAccessType,
		AppSecurityGroupManagement: req.AppSecurityGroupManagement,
		HomeEfsFileSystemCreation:  req.HomeEfsFileSystemCreation,
		TagPropagation:             req.TagPropagation,
		VpcID:                      req.VpcID,
		SubnetIDs:                  req.SubnetIDs,
		DefaultUserSettings:        req.DefaultUserSettings,
		DefaultSpaceSettings:       req.DefaultSpaceSettings,
		DomainSettingsForUpdate:    req.DomainSettingsForUpdate,
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated domain", "id", req.DomainID)

	return json.Marshal(map[string]string{keyDomainArn: d.DomainArn})
}
