package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// updateDomainConfigRequest is the request body for UpdateElasticsearchDomainConfig.
type updateDomainConfigRequest struct {
	ClusterConfig             *updateClusterConfig                `json:"ElasticsearchClusterConfig"`
	EBSOptions                *updateEBSOptions                   `json:"EBSOptions"`
	SnapshotOptions           *domainSnapshotOptions              `json:"SnapshotOptions"`
	EncryptionAtRest          *updateEnabledOption                `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption      *updateEnabledOption                `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts        *updateEndpointOptions              `json:"DomainEndpointOptions"`
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
		upd.ClusterConfigPatch = req.ClusterConfig.apply
	}

	if req.EBSOptions != nil {
		upd.EBSOptionsPatch = req.EBSOptions.apply
	}

	if req.SnapshotOptions != nil {
		so := SnapshotOptions{AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour}
		upd.SnapshotOptions = &so
	}

	if req.EncryptionAtRest != nil {
		upd.EncryptionAtRestEnabled = req.EncryptionAtRest.Enabled
	}

	if req.NodeToNodeEncryption != nil {
		upd.NodeToNodeEncryptionEnabled = req.NodeToNodeEncryption.Enabled
	}

	if req.DomainEndpointOpts != nil {
		upd.EnforceHTTPS = req.DomainEndpointOpts.EnforceHTTPS
		upd.TLSSecurityPolicy = req.DomainEndpointOpts.TLSSecurityPolicy
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

	h.writeJSON(r, w, h.buildDomainConfigOutput(domain))
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
func (h *Handler) buildDomainConfigOutput(d *Domain) *describeDomainConfigOutput {
	status := domainConfigStatus(d, h.Backend.Now())
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
		// types.ColdStorageOptions (elasticsearchservice@v1.45.4
		// deserializers.go) wraps Enabled in a nested object under
		// "ColdStorageOptions" -- there is no flat "ColdStorageEnabled" member.
		"ColdStorageOptions": map[string]any{
			keyEnabled: d.ClusterConfig.ColdStorageEnabled,
		},
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
		Options: map[string]any{keyEnabled: d.EncryptionAtRestEnabled},
		Status:  status,
	}
	out.DomainConfig.NodeToNodeEncryptionOptions = elasticsearchConfigValue{
		Options: map[string]any{keyEnabled: d.NodeToNodeEncryptionEnabled},
		Status:  status,
	}
	out.DomainConfig.DomainEndpointOptions = elasticsearchConfigValue{
		Options: map[string]any{
			"EnforceHTTPS":      d.EnforceHTTPS,
			"TLSSecurityPolicy": d.TLSSecurityPolicy,
		},
		Status: status,
	}

	h.applySecurityConfigFields(out, d, status)

	return out
}

// applySecurityConfigFields fills in the CognitoOptions/AdvancedSecurityOptions/
// AutoTuneOptions/LogPublishingOptions/VPCOptions members of out.DomainConfig,
// factored out of buildDomainConfigOutput to keep its cognitive complexity low.
func (h *Handler) applySecurityConfigFields(
	out *describeDomainConfigOutput, d *Domain, status elasticsearchConfigStatus,
) {
	out.DomainConfig.CognitoOptions = elasticsearchConfigValue{
		Options: toCognitoOptionsJSON(d.CognitoOptions),
		Status:  status,
	}
	out.DomainConfig.AdvancedSecurityOptions = elasticsearchConfigValue{
		Options: toAdvancedSecurityOptionsJSON(d.AdvancedSecurityOptions), Status: status,
	}
	out.DomainConfig.AutoTuneOptions = autoTuneConfigValue{
		Options: autoTuneOptionsToJSON(d.AutoTuneOptions),
		Status:  autoTuneConfigStatus(d),
	}
	out.DomainConfig.DeploymentStrategyOptions = elasticsearchConfigValue{
		Options: deploymentStrategyOptionsToJSON(d.DeploymentStrategyOptions), Status: status,
	}
	out.DomainConfig.LogPublishingOptions = elasticsearchConfigValue{
		Options: toLogPublishingOptionsJSON(d.LogPublishingOptions), Status: status,
	}

	if v := h.vpcDerivedInfoJSON(d); v != nil {
		out.DomainConfig.VPCOptions = &elasticsearchConfigValue{Options: v, Status: status}
	}
}

// Converts a backend AutoTuneOptions to the DomainConfig response's Options
// shape (types.AutoTuneOptions -- DesiredState/MaintenanceSchedules/
// RollbackOnDisable), which is DIFFERENT from the DomainStatus response's
// shape (types.AutoTuneOptionsOutput, see toAutoTuneOptionsJSON in
// handler_domains.go). RollbackOnDisable is stored and echoed verbatim.
func autoTuneOptionsToJSON(a *AutoTuneOptions) domainConfigAutoTuneOptionsJSON {
	if a == nil {
		return domainConfigAutoTuneOptionsJSON{DesiredState: autoTuneStateDisabled}
	}

	desired := a.DesiredState
	if desired == "" {
		desired = autoTuneStateDisabled
	}

	return domainConfigAutoTuneOptionsJSON{
		DesiredState:         desired,
		RollbackOnDisable:    a.RollbackOnDisable,
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

// Converts a backend DeploymentStrategyOptions to its DomainConfig wire
// representation, defaulting to "Default" when the domain never set one
// (matching types.DeploymentStrategy's Default value).
func deploymentStrategyOptionsToJSON(d *DeploymentStrategyOptions) deploymentStrategyOptionsJSON {
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
func domainConfigStatus(d *Domain, now time.Time) elasticsearchConfigStatus {
	return elasticsearchConfigStatus{
		State:         optionState(d, now),
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

	h.writeJSON(r, w, h.buildDomainConfigOutput(d))
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
// member of AutoTuneOptionsStatus) -- see autoTuneOptionsToJSON's
// doc comment for why this differs from the DomainStatus response's shape.
type domainConfigAutoTuneOptionsJSON struct {
	DesiredState         string                            `json:"DesiredState,omitempty"`
	RollbackOnDisable    string                            `json:"RollbackOnDisable,omitempty"`
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
type domainConfigFields struct {
	VPCOptions                  *elasticsearchConfigValue `json:"VPCOptions,omitempty"`
	AutoTuneOptions             autoTuneConfigValue       `json:"AutoTuneOptions"`
	EncryptionAtRestOptions     elasticsearchConfigValue  `json:"EncryptionAtRestOptions"`
	AccessPolicies              elasticsearchConfigValue  `json:"AccessPolicies"`
	AdvancedOptions             elasticsearchConfigValue  `json:"AdvancedOptions"`
	SnapshotOptions             elasticsearchConfigValue  `json:"SnapshotOptions"`
	ElasticsearchVersion        elasticsearchConfigValue  `json:"ElasticsearchVersion"`
	NodeToNodeEncryptionOptions elasticsearchConfigValue  `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       elasticsearchConfigValue  `json:"DomainEndpointOptions"`
	CognitoOptions              elasticsearchConfigValue  `json:"CognitoOptions"`
	AdvancedSecurityOptions     elasticsearchConfigValue  `json:"AdvancedSecurityOptions"`
	EBSOptions                  elasticsearchConfigValue  `json:"EBSOptions"`
	DeploymentStrategyOptions   elasticsearchConfigValue  `json:"DeploymentStrategyOptions"`
	LogPublishingOptions        elasticsearchConfigValue  `json:"LogPublishingOptions"`
	ElasticsearchClusterConfig  elasticsearchConfigValue  `json:"ElasticsearchClusterConfig"`
}

type describeDomainConfigOutput struct {
	DomainConfig domainConfigFields `json:"DomainConfig"`
}

// cancelDomainConfigChangeRequest is the request body for CancelDomainConfigChange.
type cancelDomainConfigChangeRequest struct {
	DryRun *bool `json:"DryRun"`
}

func (h *Handler) handleCancelDomainConfigChange(w http.ResponseWriter, r *http.Request, domainName string) {
	var req cancelDomainConfigChangeRequest
	if !h.decodeRequest(w, r, &req) {
		return
	}

	_, err := h.Backend.CancelDomainConfigChange(h.reqContext(r), domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	dryRun := false
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}

	h.writeJSON(r, w, map[string]any{
		"CancelledChangeIds":        []string{},
		"CancelledChangeProperties": []any{},
		"DryRun":                    dryRun,
	})
}

func (h *Handler) handleDescribeDomainAutoTunes(w http.ResponseWriter, r *http.Request, domainName string) {
	if err := h.Backend.DescribeDomainAutoTunes(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	writePagedList(h, w, r, listSpec("AutoTunes"), []any{}, nil)
}

func (h *Handler) handleDescribeDomainChangeProgress(w http.ResponseWriter, r *http.Request, domainName string) {
	change, err := h.Backend.DescribeDomainChangeProgress(h.reqContext(r), domainName, r.URL.Query().Get("changeid"))
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	// ConfigChangeStatus is mixed-case ("Completed", enums.go:83), unlike the
	// overall Status enum's "COMPLETED" (OverallChangeStatus).
	configStatus, overall := "Completed", "COMPLETED"
	if change.InProgress {
		configStatus, overall = "ApplyingChanges", "PROCESSING"
	}

	status := map[string]any{
		"ChangeId":           change.ChangeID,
		"ConfigChangeStatus": configStatus,
		"Status":             overall,
	}

	if !change.StartTime.IsZero() {
		status["StartTime"] = awstime.Epoch(change.StartTime)
		status["LastUpdatedTime"] = awstime.Epoch(change.StartTime)
	}

	h.writeJSON(r, w, map[string]any{"ChangeProgressStatus": status})
}

// updateClusterConfig carries only the members present in an update request.
type updateClusterConfig struct {
	ZoneAwarenessConfig    *domainZoneAwarenessConfig `json:"ZoneAwarenessConfig"`
	InstanceType           *string                    `json:"InstanceType"`
	InstanceCount          *int                       `json:"InstanceCount"`
	DedicatedMasterEnabled *bool                      `json:"DedicatedMasterEnabled"`
	DedicatedMasterType    *string                    `json:"DedicatedMasterType"`
	DedicatedMasterCount   *int                       `json:"DedicatedMasterCount"`
	ZoneAwarenessEnabled   *bool                      `json:"ZoneAwarenessEnabled"`
	WarmEnabled            *bool                      `json:"WarmEnabled"`
	WarmType               *string                    `json:"WarmType"`
	WarmCount              *int                       `json:"WarmCount"`
	ColdStorageOptions     *coldStorageOptionsJSON    `json:"ColdStorageOptions"`
}

func setIfPresent[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

func (u *updateClusterConfig) apply(c *ClusterConfig) {
	setIfPresent(&c.InstanceType, u.InstanceType)
	setIfPresent(&c.InstanceCount, u.InstanceCount)
	setIfPresent(&c.DedicatedMasterEnabled, u.DedicatedMasterEnabled)
	setIfPresent(&c.DedicatedMasterType, u.DedicatedMasterType)
	setIfPresent(&c.DedicatedMasterCount, u.DedicatedMasterCount)
	setIfPresent(&c.ZoneAwarenessEnabled, u.ZoneAwarenessEnabled)
	setIfPresent(&c.WarmEnabled, u.WarmEnabled)
	setIfPresent(&c.WarmType, u.WarmType)
	setIfPresent(&c.WarmCount, u.WarmCount)
	if u.ColdStorageOptions != nil {
		c.ColdStorageEnabled = u.ColdStorageOptions.Enabled
	}

	if u.ZoneAwarenessConfig != nil {
		c.ZoneAwarenessConfig = ZoneAwarenessConfig{AvailabilityZoneCount: u.ZoneAwarenessConfig.AvailabilityZoneCount}
	}
}

// updateEBSOptions carries only the members present in an update request.
type updateEBSOptions struct {
	VolumeType *string `json:"VolumeType"`
	VolumeSize *int    `json:"VolumeSize"`
	Iops       *int    `json:"Iops"`
	Throughput *int    `json:"Throughput"`
	EBSEnabled *bool   `json:"EBSEnabled"`
}

func (u *updateEBSOptions) apply(o *EBSOptions) {
	setIfPresent(&o.VolumeType, u.VolumeType)
	setIfPresent(&o.VolumeSize, u.VolumeSize)
	setIfPresent(&o.Iops, u.Iops)
	setIfPresent(&o.Throughput, u.Throughput)
	setIfPresent(&o.EBSEnabled, u.EBSEnabled)
}

type updateEnabledOption struct {
	Enabled *bool `json:"Enabled"`
}

type updateEndpointOptions struct {
	EnforceHTTPS      *bool   `json:"EnforceHTTPS"`
	TLSSecurityPolicy *string `json:"TLSSecurityPolicy"`
}
