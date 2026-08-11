package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// dryRunResultsJSON mirrors aws-sdk-go-v2 types.DryRunResults.
type dryRunResultsJSON struct {
	DeploymentType string `json:"DeploymentType"`
	Message        string `json:"Message"`
}

// handleUpdateDomainConfigDryRun serves UpdateDomainConfig requests with
// DryRun=true: it previews the resulting config without mutating the domain,
// matching real AWS's validate-only behavior for dry runs.
func (h *Handler) handleUpdateDomainConfigDryRun(
	w http.ResponseWriter,
	r *http.Request,
	name string,
	input UpdateDomainConfigInput,
) {
	domain, err := h.Backend.PreviewDomainConfig(name, input)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{
		jsonKeyDomainConfig: toDomainConfigJSON(domain),
		"DryRunResults": dryRunResultsJSON{
			DeploymentType: "DynamicUpdate",
			Message:        "Deployment type is not fully validated in this dry run.",
		},
	})
}

// Builds the DescribeDomainConfig / UpdateDomainConfig response body.
func toDomainConfigJSON(d *Domain) domainConfigFields {
	active := opensearchConfigStatus{State: domainStatusActive}
	st := toDomainStatusJSON(d)

	cfg := domainConfigFields{
		EngineVersion:   opensearchConfigValue{Options: d.EngineVersion, Status: active},
		ClusterConfig:   opensearchConfigValue{Options: st.ClusterConfig, Status: active},
		EBSOptions:      opensearchConfigValue{Options: map[string]any{}, Status: active},
		AccessPolicies:  opensearchConfigValue{Options: d.AccessPolicies, Status: active},
		AdvancedOptions: opensearchConfigValue{Options: map[string]any{}, Status: active},
	}

	if st.EBSOptions != nil {
		cfg.EBSOptions = opensearchConfigValue{Options: st.EBSOptions, Status: active}
	}

	if st.SnapshotOptions != nil {
		cfg.SnapshotOptions = opensearchConfigValue{Options: st.SnapshotOptions, Status: active}
	}

	if st.EncryptionAtRestOptions != nil {
		cfg.EncryptionAtRestOptions = opensearchConfigValue{
			Options: st.EncryptionAtRestOptions,
			Status:  active,
		}
	}

	if st.NodeToNodeEncryptionOptions != nil {
		cfg.NodeToNodeEncryptionOptions = opensearchConfigValue{
			Options: st.NodeToNodeEncryptionOptions,
			Status:  active,
		}
	}

	if st.DomainEndpointOptions != nil {
		cfg.DomainEndpointOptions = opensearchConfigValue{
			Options: st.DomainEndpointOptions,
			Status:  active,
		}
	}

	if st.AdvancedSecurityOptions != nil {
		cfg.AdvancedSecurityOptions = opensearchConfigValue{
			Options: st.AdvancedSecurityOptions,
			Status:  active,
		}
	}

	if st.VPCOptions != nil {
		cfg.VPCOptions = opensearchConfigValue{Options: st.VPCOptions, Status: active}
	}

	if st.CognitoOptions != nil {
		cfg.CognitoOptions = opensearchConfigValue{Options: st.CognitoOptions, Status: active}
	}

	if len(st.LogPublishingOptions) > 0 {
		cfg.LogPublishingOptions = opensearchConfigValue{
			Options: st.LogPublishingOptions,
			Status:  active,
		}
	}

	if st.OffPeakWindowOptions != nil {
		cfg.OffPeakWindowOptions = opensearchConfigValue{
			Options: st.OffPeakWindowOptions,
			Status:  active,
		}
	}

	if st.IdentityCenterOptions != nil {
		cfg.IdentityCenterOptions = opensearchConfigValue{
			Options: st.IdentityCenterOptions,
			Status:  active,
		}
	}

	if st.EnableSoftwareUpdateOptions != nil {
		cfg.EnableSoftwareUpdateOptions = opensearchConfigValue{
			Options: st.EnableSoftwareUpdateOptions,
			Status:  active,
		}
	}

	return cfg
}

type opensearchConfigStatus struct {
	State string `json:"State"`
}

type opensearchConfigValue struct {
	Options any                    `json:"Options"`
	Status  opensearchConfigStatus `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct {
	EngineVersion               opensearchConfigValue `json:"EngineVersion"`
	ClusterConfig               opensearchConfigValue `json:"ClusterConfig"`
	EBSOptions                  opensearchConfigValue `json:"EBSOptions"`
	AccessPolicies              opensearchConfigValue `json:"AccessPolicies"`
	AdvancedOptions             opensearchConfigValue `json:"AdvancedOptions"`
	SnapshotOptions             opensearchConfigValue `json:"SnapshotOptions"`
	EncryptionAtRestOptions     opensearchConfigValue `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions opensearchConfigValue `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       opensearchConfigValue `json:"DomainEndpointOptions"`
	AdvancedSecurityOptions     opensearchConfigValue `json:"AdvancedSecurityOptions"`
	VPCOptions                  opensearchConfigValue `json:"VPCOptions"`
	CognitoOptions              opensearchConfigValue `json:"CognitoOptions"`
	LogPublishingOptions        opensearchConfigValue `json:"LogPublishingOptions"`
	OffPeakWindowOptions        opensearchConfigValue `json:"OffPeakWindowOptions"`
	IdentityCenterOptions       opensearchConfigValue `json:"IdentityCenterOptions"`
	EnableSoftwareUpdateOptions opensearchConfigValue `json:"SoftwareUpdateOptions"`
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{jsonKeyDomainConfig: toDomainConfigJSON(domain)})
}

// cancelDomainConfigChangeRequest is the JSON request body for CancelDomainConfigChange.
type cancelDomainConfigChangeRequest struct {
	DryRun bool `json:"DryRun"`
}

// cancelDomainConfigChangeOutput is the JSON response for CancelDomainConfigChange.
type cancelDomainConfigChangeOutput struct {
	CancelledChangeIDs        []string `json:"CancelledChangeIds"`
	CancelledChangeProperties []any    `json:"CancelledChangeProperties"`
	DryRun                    bool     `json:"DryRun"`
}

func (h *Handler) handleCancelDomainConfigChange(
	w http.ResponseWriter,
	r *http.Request,
	domainName string,
) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelDomainConfigChangeRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	ids, dryRun, cancelErr := h.Backend.CancelDomainConfigChange(domainName, req.DryRun)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelDomainConfigChangeOutput{
		CancelledChangeIDs:        ids,
		CancelledChangeProperties: []any{},
		DryRun:                    dryRun,
	})
}
