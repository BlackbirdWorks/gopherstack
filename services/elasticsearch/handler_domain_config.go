package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// updateDomainConfigRequest is the request body for UpdateElasticsearchDomainConfig.
type updateDomainConfigRequest struct {
	ClusterConfig             *domainClusterConfig                `json:"ElasticsearchClusterConfig"`
	EBSOptions                *domainEBSOptions                   `json:"EBSOptions"`
	SnapshotOptions           *domainSnapshotOptions              `json:"SnapshotOptions"`
	EncryptionAtRest          *domainEncryptionAtRestOptions      `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption      *domainNodeToNodeEncryptionOptions  `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts        *domainEndpointOptions              `json:"DomainEndpointOptions"`
	VPCOptions                *vpcOptionsRequestJSON              `json:"VPCOptions"`
	CognitoOptions            *cognitoOptionsJSON                 `json:"CognitoOptions"`
	AdvancedSecurityOptions   *advancedSecurityOptionsRequestJSON `json:"AdvancedSecurityOptions"`
	AutoTuneOptions           *autoTuneOptionsRequestJSON         `json:"AutoTuneOptions"`
	DeploymentStrategyOptions *deploymentStrategyOptionsJSON      `json:"DeploymentStrategyOptions"`
	LogPublishingOptions      map[string]logPublishingOptionJSON  `json:"LogPublishingOptions"`
	AdvancedOptions           map[string]string                   `json:"AdvancedOptions"`
	AccessPolicies            *string                             `json:"AccessPolicies"`
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

	if req.VPCOptions != nil {
		upd.VPCOptions = &VPCOptions{
			SubnetIDs:        req.VPCOptions.SubnetIDs,
			SecurityGroupIDs: req.VPCOptions.SecurityGroupIDs,
		}
	}

	if req.LogPublishingOptions != nil {
		opts := logPublishingOptionsFromRequest(req.LogPublishingOptions)
		upd.LogPublishingOptions = opts
	}

	upd.AccessPolicies = req.AccessPolicies

	if applyErr := applyOptionalSecurityUpdateFields(&upd, &req); applyErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", applyErr.Error())

		return
	}

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

// applyOptionalSecurityUpdateFields validates and applies req's
// CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions onto upd, factored
// out of handleUpdateDomainConfig to keep its cognitive complexity low.
func applyOptionalSecurityUpdateFields(upd *UpdateConfig, req *updateDomainConfigRequest) error {
	if req.CognitoOptions != nil {
		cogOpts, err := cognitoOptionsFromRequest(req.CognitoOptions)
		if err != nil {
			return err
		}

		upd.CognitoOptions = cogOpts
	}

	if req.AdvancedSecurityOptions != nil {
		asOpts, err := advancedSecurityOptionsFromRequest(req.AdvancedSecurityOptions)
		if err != nil {
			return err
		}

		upd.AdvancedSecurityOptions = asOpts
	}

	if req.AutoTuneOptions != nil {
		atOpts, err := autoTuneOptionsFromRequest(req.AutoTuneOptions)
		if err != nil {
			return err
		}

		upd.AutoTuneOptions = atOpts
	}

	if req.DeploymentStrategyOptions != nil {
		dsOpts, err := deploymentStrategyOptionsFromRequest(req.DeploymentStrategyOptions)
		if err != nil {
			return err
		}

		upd.DeploymentStrategyOptions = dsOpts
	}

	return nil
}

// buildDomainConfigOutput builds the DescribeDomainConfig/UpdateDomainConfig response.
func buildDomainConfigOutput(d *Domain) *describeDomainConfigOutput {
	status := domainConfigStatus(d)
	out := &describeDomainConfigOutput{}
	out.DomainConfig.ElasticsearchVersion = elasticsearchConfigValue{
		Options: d.ElasticsearchVersion,
		Status:  status,
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

	out.DomainConfig.ElasticsearchClusterConfig = elasticsearchConfigValue{Options: clusterOpts, Status: status}
	out.DomainConfig.EBSOptions = elasticsearchConfigValue{Options: map[string]any{
		keyEBSEnabled: d.EBSOptions.EBSEnabled,
		keyVolumeSize: d.EBSOptions.VolumeSize,
		keyVolumeType: d.EBSOptions.VolumeType,
		keyIops:       d.EBSOptions.Iops,
		keyThroughput: d.EBSOptions.Throughput,
	}, Status: status}
	out.DomainConfig.AccessPolicies = elasticsearchConfigValue{Options: d.AccessPolicies, Status: status}

	advOpts := d.AdvancedOptions
	if advOpts == nil {
		advOpts = map[string]string{}
	}

	out.DomainConfig.AdvancedOptions = elasticsearchConfigValue{Options: advOpts, Status: status}
	out.DomainConfig.SnapshotOptions = elasticsearchConfigValue{
		Options: map[string]any{"AutomatedSnapshotStartHour": d.SnapshotOptions.AutomatedSnapshotStartHour},
		Status:  status,
	}
	out.DomainConfig.EncryptionAtRestOptions = elasticsearchConfigValue{
		Options: map[string]any{"Enabled": d.EncryptionAtRestEnabled},
		Status:  status,
	}
	out.DomainConfig.NodeToNodeEncryptionOptions = elasticsearchConfigValue{
		Options: map[string]any{"Enabled": d.NodeToNodeEncryptionEnabled},
		Status:  status,
	}
	out.DomainConfig.DomainEndpointOptions = elasticsearchConfigValue{
		Options: map[string]any{
			"EnforceHTTPS":      d.EnforceHTTPS,
			"TLSSecurityPolicy": d.TLSSecurityPolicy,
		},
		Status: status,
	}

	applySecurityConfigFields(out, d, status)

	return out
}

// applySecurityConfigFields fills in the CognitoOptions/AdvancedSecurityOptions/
// AutoTuneOptions/LogPublishingOptions/VPCOptions members of out.DomainConfig,
// factored out of buildDomainConfigOutput to keep its cognitive complexity low.
func applySecurityConfigFields(out *describeDomainConfigOutput, d *Domain, status elasticsearchConfigStatus) {
	out.DomainConfig.CognitoOptions = elasticsearchConfigValue{
		Options: toCognitoOptionsJSON(d.CognitoOptions),
		Status:  status,
	}
	out.DomainConfig.AdvancedSecurityOptions = elasticsearchConfigValue{
		Options: toAdvancedSecurityOptionsJSON(d.AdvancedSecurityOptions), Status: status,
	}
	out.DomainConfig.AutoTuneOptions = autoTuneConfigValue{
		Options: toDomainConfigAutoTuneOptionsJSON(d.AutoTuneOptions),
		Status:  autoTuneConfigStatus(d),
	}
	out.DomainConfig.DeploymentStrategyOptions = elasticsearchConfigValue{
		Options: toDomainConfigDeploymentStrategyOptionsJSON(d.DeploymentStrategyOptions), Status: status,
	}
	out.DomainConfig.LogPublishingOptions = elasticsearchConfigValue{
		Options: toLogPublishingOptionsJSON(d.LogPublishingOptions), Status: status,
	}

	if v := toVPCDerivedInfoJSON(d.VPCOptions); v != nil {
		out.DomainConfig.VPCOptions = &elasticsearchConfigValue{Options: v, Status: status}
	}
}

// toDomainConfigAutoTuneOptionsJSON converts a backend AutoTuneOptions to the
// DomainConfig response's Options shape (types.AutoTuneOptions --
// DesiredState/MaintenanceSchedules/RollbackOnDisable), which is DIFFERENT
// from the DomainStatus response's shape (types.AutoTuneOptionsOutput, see
// toAutoTuneOptionsJSON in handler_domains.go). RollbackOnDisable is not
// modeled: it only applies to UpdateElasticsearchDomainConfig (not Create)
// and this backend has no rollback state machine to act on it.
func toDomainConfigAutoTuneOptionsJSON(a *AutoTuneOptions) domainConfigAutoTuneOptionsJSON {
	if a == nil {
		return domainConfigAutoTuneOptionsJSON{DesiredState: autoTuneStateDisabled}
	}

	desired := a.DesiredState
	if desired == "" {
		desired = autoTuneStateDisabled
	}

	return domainConfigAutoTuneOptionsJSON{
		DesiredState:         desired,
		MaintenanceSchedules: toMaintenanceSchedulesJSON(a.MaintenanceSchedules),
	}
}

// toMaintenanceSchedulesJSON converts backend maintenance schedules to their
// wire representation.
func toMaintenanceSchedulesJSON(schedules []AutoTuneMaintenanceSchedule) []autoTuneMaintenanceScheduleJSON {
	if schedules == nil {
		return nil
	}

	out := make([]autoTuneMaintenanceScheduleJSON, len(schedules))
	for i, s := range schedules {
		out[i] = autoTuneMaintenanceScheduleJSON{CronExpressionForRecurrence: s.CronExpressionForRecurrence}
		if !s.StartAt.IsZero() {
			out[i].StartAt = awstime.Epoch(s.StartAt)
		}

		if s.Duration != nil {
			out[i].Duration = &durationJSON{Unit: s.Duration.Unit, Value: s.Duration.Value}
		}
	}

	return out
}

// autoTuneConfigStatus builds AutoTuneOptionsStatus.Status (types.AutoTuneStatus),
// which -- unlike every other DomainConfig field -- is NOT the generic
// OptionStatus shape (confirmed against deserializers.go:9631-9700 in the
// pinned SDK; State uses the AutoTuneState enum, e.g. ENABLED/DISABLED, not
// OptionState's Active/Processing/...). This backend has no
// ENABLE_IN_PROGRESS/DISABLE_IN_PROGRESS transition window (Auto-Tune
// changes apply synchronously, matching the Processing/DomainProcessingStatus
// simplification elsewhere in this service), so State maps directly from
// DesiredState.
func autoTuneConfigStatus(d *Domain) autoTuneStatusJSON {
	state := autoTuneStateDisabled
	if d.AutoTuneOptions != nil && d.AutoTuneOptions.DesiredState == autoTuneStateEnabled {
		state = autoTuneStateEnabled
	}

	return autoTuneStatusJSON{
		State:         state,
		CreationDate:  awstime.Epoch(d.CreatedAt),
		UpdateDate:    awstime.Epoch(d.ConfigUpdatedAt),
		UpdateVersion: d.ConfigVersion,
	}
}

// toDomainConfigDeploymentStrategyOptionsJSON converts a backend
// DeploymentStrategyOptions to its DomainConfig wire representation,
// defaulting to "Default" when the domain never set one (matching
// types.DeploymentStrategy's Default value).
func toDomainConfigDeploymentStrategyOptionsJSON(d *DeploymentStrategyOptions) deploymentStrategyOptionsJSON {
	if d == nil {
		return deploymentStrategyOptionsJSON{DeploymentStrategy: "Default"}
	}

	return deploymentStrategyOptionsJSON{DeploymentStrategy: d.DeploymentStrategy}
}

// domainConfigStatus builds the OptionStatus (CreationDate/UpdateDate/
// UpdateVersion/State/PendingDeletion) shared by every DomainConfig field.
// AWS tracks these per-option; this backend tracks one domain-wide
// CreatedAt/ConfigUpdatedAt/ConfigVersion instead (see Domain's doc comment
// in models.go), so every field in a given response shares the same status.
func domainConfigStatus(d *Domain) elasticsearchConfigStatus {
	return elasticsearchConfigStatus{
		State:         statusActiveCap,
		CreationDate:  awstime.Epoch(d.CreatedAt),
		UpdateDate:    awstime.Epoch(d.ConfigUpdatedAt),
		UpdateVersion: d.ConfigVersion,
	}
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

// elasticsearchConfigStatus mirrors types.OptionStatus. CreationDate/
// UpdateDate are epoch-seconds timestamps (restjson1's unixTimestamp wire
// format -- see pkgs/awstime). PendingDeletion is always false: this backend
// never soft-deletes a domain's configuration items.
type elasticsearchConfigStatus struct {
	State           string  `json:"State"`
	CreationDate    float64 `json:"CreationDate"`
	UpdateDate      float64 `json:"UpdateDate"`
	UpdateVersion   int     `json:"UpdateVersion"`
	PendingDeletion bool    `json:"PendingDeletion"`
}

type elasticsearchConfigValue struct {
	Options any                       `json:"Options"`
	Status  elasticsearchConfigStatus `json:"Status"`
}

// autoTuneStatusJSON mirrors types.AutoTuneStatus -- see autoTuneConfigStatus's
// doc comment for why this differs from the generic elasticsearchConfigStatus.
type autoTuneStatusJSON struct {
	State           string  `json:"State"`
	ErrorMessage    string  `json:"ErrorMessage,omitempty"`
	CreationDate    float64 `json:"CreationDate"`
	UpdateDate      float64 `json:"UpdateDate"`
	UpdateVersion   int     `json:"UpdateVersion"`
	PendingDeletion bool    `json:"PendingDeletion"`
}

// domainConfigAutoTuneOptionsJSON mirrors types.AutoTuneOptions (the Options
// member of AutoTuneOptionsStatus) -- see toDomainConfigAutoTuneOptionsJSON's
// doc comment for why this differs from the DomainStatus response's shape.
type domainConfigAutoTuneOptionsJSON struct {
	DesiredState         string                            `json:"DesiredState,omitempty"`
	MaintenanceSchedules []autoTuneMaintenanceScheduleJSON `json:"MaintenanceSchedules,omitempty"`
}

// autoTuneConfigValue is the AutoTuneOptions member of DomainConfig -- it
// cannot reuse elasticsearchConfigValue because both its Options and Status
// shapes differ from every other DomainConfig field's.
type autoTuneConfigValue struct {
	Options domainConfigAutoTuneOptionsJSON `json:"Options"`
	Status  autoTuneStatusJSON              `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ElasticsearchVersion        elasticsearchConfigValue  `json:"ElasticsearchVersion"`
	ElasticsearchClusterConfig  elasticsearchConfigValue  `json:"ElasticsearchClusterConfig"`
	EBSOptions                  elasticsearchConfigValue  `json:"EBSOptions"`
	AccessPolicies              elasticsearchConfigValue  `json:"AccessPolicies"`
	AdvancedOptions             elasticsearchConfigValue  `json:"AdvancedOptions"`
	SnapshotOptions             elasticsearchConfigValue  `json:"SnapshotOptions"`
	EncryptionAtRestOptions     elasticsearchConfigValue  `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions elasticsearchConfigValue  `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       elasticsearchConfigValue  `json:"DomainEndpointOptions"`
	CognitoOptions              elasticsearchConfigValue  `json:"CognitoOptions"`
	AdvancedSecurityOptions     elasticsearchConfigValue  `json:"AdvancedSecurityOptions"`
	AutoTuneOptions             autoTuneConfigValue       `json:"AutoTuneOptions"`
	DeploymentStrategyOptions   elasticsearchConfigValue  `json:"DeploymentStrategyOptions"`
	LogPublishingOptions        elasticsearchConfigValue  `json:"LogPublishingOptions"`
	VPCOptions                  *elasticsearchConfigValue `json:"VPCOptions,omitempty"`
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
