package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// updateDomainConfigRequest is the request body for UpdateElasticsearchDomainConfig.
type updateDomainConfigRequest struct {
	ClusterConfig        *domainClusterConfig               `json:"ElasticsearchClusterConfig"`
	EBSOptions           *domainEBSOptions                  `json:"EBSOptions"`
	SnapshotOptions      *domainSnapshotOptions             `json:"SnapshotOptions"`
	EncryptionAtRest     *domainEncryptionAtRestOptions     `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption *domainNodeToNodeEncryptionOptions `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts   *domainEndpointOptions             `json:"DomainEndpointOptions"`
	AdvancedOptions      map[string]string                  `json:"AdvancedOptions"`
	AccessPolicies       *string                            `json:"AccessPolicies"`
}

func (h *Handler) handleUpdateDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req updateDomainConfigRequest
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	upd := UpdateConfig{}

	if req.ClusterConfig != nil {
		cfg := clusterConfigFromRequest(req.ClusterConfig)
		upd.ClusterConfig = &cfg
	}

	if req.EBSOptions != nil {
		opts := ebsOptsFromRequest(req.EBSOptions)
		upd.EBSOptions = &opts
	}

	if req.SnapshotOptions != nil {
		so := SnapshotOptions{AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour}
		upd.SnapshotOptions = &so
	}

	if req.EncryptionAtRest != nil {
		upd.EncryptionAtRestEnabled = &req.EncryptionAtRest.Enabled
	}

	if req.NodeToNodeEncryption != nil {
		upd.NodeToNodeEncryptionEnabled = &req.NodeToNodeEncryption.Enabled
	}

	if req.DomainEndpointOpts != nil {
		upd.EnforceHTTPS = &req.DomainEndpointOpts.EnforceHTTPS
		upd.TLSSecurityPolicy = &req.DomainEndpointOpts.TLSSecurityPolicy
	}

	if req.AdvancedOptions != nil {
		upd.AdvancedOptions = req.AdvancedOptions
	}

	upd.AccessPolicies = req.AccessPolicies

	domain, err := h.Backend.UpdateDomainConfig(h.reqContext(r), name, upd)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, buildDomainConfigOutput(domain))
}

// buildDomainConfigOutput builds the DescribeDomainConfig/UpdateDomainConfig response.
func buildDomainConfigOutput(d *Domain) *describeDomainConfigOutput {
	activeStatus := elasticsearchConfigStatus{State: statusActiveCap}
	out := &describeDomainConfigOutput{}
	out.DomainConfig.ElasticsearchVersion = elasticsearchConfigValue{
		Options: d.ElasticsearchVersion,
		Status:  activeStatus,
	}

	clusterOpts := map[string]any{
		keyInstanceType:           d.ClusterConfig.InstanceType,
		keyInstanceCount:          d.ClusterConfig.InstanceCount,
		keyDedicatedMasterEnabled: d.ClusterConfig.DedicatedMasterEnabled,
		keyZoneAwarenessEnabled:   d.ClusterConfig.ZoneAwarenessEnabled,
		keyWarmEnabled:            d.ClusterConfig.WarmEnabled,
		keyColdStorageEnabled:     d.ClusterConfig.ColdStorageEnabled,
	}

	if d.ClusterConfig.DedicatedMasterEnabled {
		clusterOpts[keyDedicatedMasterType] = d.ClusterConfig.DedicatedMasterType
		clusterOpts[keyDedicatedMasterCount] = d.ClusterConfig.DedicatedMasterCount
	}

	if d.ClusterConfig.WarmEnabled {
		clusterOpts[keyWarmType] = d.ClusterConfig.WarmType
		clusterOpts[keyWarmCount] = d.ClusterConfig.WarmCount
	}

	if d.ClusterConfig.ZoneAwarenessEnabled {
		clusterOpts[keyZoneAwarenessConfig] = map[string]any{
			"AvailabilityZoneCount": d.ClusterConfig.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}

	out.DomainConfig.ElasticsearchClusterConfig = elasticsearchConfigValue{Options: clusterOpts, Status: activeStatus}
	out.DomainConfig.EBSOptions = elasticsearchConfigValue{Options: map[string]any{
		keyEBSEnabled: d.EBSOptions.EBSEnabled,
		keyVolumeSize: d.EBSOptions.VolumeSize,
		keyVolumeType: d.EBSOptions.VolumeType,
		keyIops:       d.EBSOptions.Iops,
		keyThroughput: d.EBSOptions.Throughput,
	}, Status: activeStatus}
	out.DomainConfig.AccessPolicies = elasticsearchConfigValue{Options: d.AccessPolicies, Status: activeStatus}

	advOpts := d.AdvancedOptions
	if advOpts == nil {
		advOpts = map[string]string{}
	}

	out.DomainConfig.AdvancedOptions = elasticsearchConfigValue{Options: advOpts, Status: activeStatus}
	out.DomainConfig.SnapshotOptions = elasticsearchConfigValue{
		Options: map[string]any{"AutomatedSnapshotStartHour": d.SnapshotOptions.AutomatedSnapshotStartHour},
		Status:  activeStatus,
	}
	out.DomainConfig.EncryptionAtRestOptions = elasticsearchConfigValue{
		Options: map[string]any{"Enabled": d.EncryptionAtRestEnabled},
		Status:  activeStatus,
	}
	out.DomainConfig.NodeToNodeEncryptionOptions = elasticsearchConfigValue{
		Options: map[string]any{"Enabled": d.NodeToNodeEncryptionEnabled},
		Status:  activeStatus,
	}
	out.DomainConfig.DomainEndpointOptions = elasticsearchConfigValue{
		Options: map[string]any{
			"EnforceHTTPS":      d.EnforceHTTPS,
			"TLSSecurityPolicy": d.TLSSecurityPolicy,
		},
		Status: activeStatus,
	}

	return out
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	d, err := h.Backend.DescribeDomain(h.reqContext(r), name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, buildDomainConfigOutput(d))
}

type elasticsearchConfigStatus struct {
	State string `json:"State"`
}

type elasticsearchConfigValue struct {
	Options any                       `json:"Options"`
	Status  elasticsearchConfigStatus `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct {
	ElasticsearchVersion        elasticsearchConfigValue `json:"ElasticsearchVersion"`
	ElasticsearchClusterConfig  elasticsearchConfigValue `json:"ElasticsearchClusterConfig"`
	EBSOptions                  elasticsearchConfigValue `json:"EBSOptions"`
	AccessPolicies              elasticsearchConfigValue `json:"AccessPolicies"`
	AdvancedOptions             elasticsearchConfigValue `json:"AdvancedOptions"`
	SnapshotOptions             elasticsearchConfigValue `json:"SnapshotOptions"`
	EncryptionAtRestOptions     elasticsearchConfigValue `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions elasticsearchConfigValue `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       elasticsearchConfigValue `json:"DomainEndpointOptions"`
}

type describeDomainConfigOutput struct {
	DomainConfig domainConfigFields `json:"DomainConfig"`
}

func (h *Handler) handleCancelDomainConfigChange(w http.ResponseWriter, r *http.Request, domainName string) {
	d, err := h.Backend.CancelDomainConfigChange(h.reqContext(r), domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, buildDomainConfigOutput(d))
}

func (h *Handler) handleDescribeDomainAutoTunes(w http.ResponseWriter, r *http.Request, domainName string) {
	if err := h.Backend.DescribeDomainAutoTunes(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"AutoTunes": []any{}})
}

func (h *Handler) handleDescribeDomainChangeProgress(w http.ResponseWriter, r *http.Request, domainName string) {
	if err := h.Backend.DescribeDomainChangeProgress(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"ChangeProgressStatus": map[string]any{"Status": "COMPLETED"}})
}
