package kinesisvideo

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// Wire DTOs for the storage, signaling-endpoint and edge-agent operations.
// JSON keys equal the SDK struct field names (kinesisvideo@v1.41.1 serializers.go).

type streamStorageConfigRequest struct {
	StreamStorageConfiguration *streamStorageConfigurationDTO `json:"StreamStorageConfiguration,omitempty"`
	CurrentVersion             string                         `json:"CurrentVersion,omitempty"`
	StreamARN                  string                         `json:"StreamARN,omitempty"`
	StreamName                 string                         `json:"StreamName,omitempty"`
}

type describeStreamStorageConfigurationResponse struct {
	StreamStorageConfiguration *streamStorageConfigurationDTO `json:"StreamStorageConfiguration"`
	StreamARN                  string                         `json:"StreamARN"`
	StreamName                 string                         `json:"StreamName"`
}

type mediaStorageConfigurationDTO struct {
	Status    string `json:"Status"`
	StreamARN string `json:"StreamARN,omitempty"`
}

type mediaStorageConfigRequest struct {
	MediaStorageConfiguration *mediaStorageConfigurationDTO `json:"MediaStorageConfiguration,omitempty"`
	ChannelARN                string                        `json:"ChannelARN,omitempty"`
	ChannelName               string                        `json:"ChannelName,omitempty"`
}

type describeMediaStorageConfigurationResponse struct {
	MediaStorageConfiguration *mediaStorageConfigurationDTO `json:"MediaStorageConfiguration,omitempty"`
}

type singleMasterChannelEndpointConfigurationDTO struct {
	Role      string   `json:"Role,omitempty"`
	Protocols []string `json:"Protocols,omitempty"`
}

type getSignalingChannelEndpointRequest struct {
	EndpointConfig *singleMasterChannelEndpointConfigurationDTO `json:"SingleMasterChannelEndpointConfiguration,omitempty"`
	ChannelARN     string                                       `json:"ChannelARN,omitempty"`
}

type resourceEndpointListItemDTO struct {
	Protocol         string `json:"Protocol"`
	ResourceEndpoint string `json:"ResourceEndpoint"`
}

type getSignalingChannelEndpointResponse struct {
	ResourceEndpointList []resourceEndpointListItemDTO `json:"ResourceEndpointList"`
}

type scheduleConfigDTO struct {
	ScheduleExpression string `json:"ScheduleExpression"`
	DurationInSeconds  int32  `json:"DurationInSeconds"`
}

type mediaSourceConfigDTO struct {
	MediaURISecretARN string `json:"MediaUriSecretArn"`
	MediaURIType      string `json:"MediaUriType"`
}

type recorderConfigDTO struct {
	MediaSourceConfig *mediaSourceConfigDTO `json:"MediaSourceConfig,omitempty"`
	ScheduleConfig    *scheduleConfigDTO    `json:"ScheduleConfig,omitempty"`
}

type uploaderConfigDTO struct {
	ScheduleConfig *scheduleConfigDTO `json:"ScheduleConfig,omitempty"`
}

type localSizeConfigDTO struct {
	StrategyOnFullSize    string `json:"StrategyOnFullSize,omitempty"`
	MaxLocalMediaSizeInMB int32  `json:"MaxLocalMediaSizeInMB,omitempty"`
}

type deletionConfigDTO struct {
	DeleteAfterUpload    *bool               `json:"DeleteAfterUpload,omitempty"`
	EdgeRetentionInHours *int32              `json:"EdgeRetentionInHours,omitempty"`
	LocalSizeConfig      *localSizeConfigDTO `json:"LocalSizeConfig,omitempty"`
}

type edgeConfigDTO struct {
	RecorderConfig *recorderConfigDTO `json:"RecorderConfig,omitempty"`
	DeletionConfig *deletionConfigDTO `json:"DeletionConfig,omitempty"`
	UploaderConfig *uploaderConfigDTO `json:"UploaderConfig,omitempty"`
	HubDeviceArn   string             `json:"HubDeviceArn,omitempty"`
}

func scheduleFromDTO(d *scheduleConfigDTO) *EdgeSchedule {
	if d == nil {
		return nil
	}

	return &EdgeSchedule{ScheduleExpression: d.ScheduleExpression, DurationInSeconds: d.DurationInSeconds}
}

func scheduleToDTO(s *EdgeSchedule) *scheduleConfigDTO {
	if s == nil {
		return nil
	}

	return &scheduleConfigDTO{ScheduleExpression: s.ScheduleExpression, DurationInSeconds: s.DurationInSeconds}
}

func edgeConfigFromDTO(d *edgeConfigDTO) EdgeConfig {
	if d == nil {
		return EdgeConfig{}
	}

	cfg := EdgeConfig{HubDeviceARN: d.HubDeviceArn}

	if r := d.RecorderConfig; r != nil {
		cfg.Recorder = &EdgeRecorder{Schedule: scheduleFromDTO(r.ScheduleConfig)}
		if m := r.MediaSourceConfig; m != nil {
			cfg.Recorder.MediaSource = &EdgeMediaSource{
				MediaURISecretARN: m.MediaURISecretARN,
				MediaURIType:      m.MediaURIType,
			}
		}
	}

	if u := d.UploaderConfig; u != nil {
		cfg.Uploader = &EdgeUploader{Schedule: scheduleFromDTO(u.ScheduleConfig)}
	}

	if del := d.DeletionConfig; del != nil {
		cfg.Deletion = &EdgeDeletion{
			DeleteAfterUpload:    del.DeleteAfterUpload,
			EdgeRetentionInHours: del.EdgeRetentionInHours,
		}
		if l := del.LocalSizeConfig; l != nil {
			cfg.Deletion.LocalSize = &EdgeLocalSize{
				StrategyOnFullSize:    l.StrategyOnFullSize,
				MaxLocalMediaSizeInMB: l.MaxLocalMediaSizeInMB,
			}
		}
	}

	return cfg
}

func edgeConfigToDTO(cfg EdgeConfig) *edgeConfigDTO {
	d := &edgeConfigDTO{HubDeviceArn: cfg.HubDeviceARN}

	if r := cfg.Recorder; r != nil {
		d.RecorderConfig = &recorderConfigDTO{ScheduleConfig: scheduleToDTO(r.Schedule)}
		if m := r.MediaSource; m != nil {
			d.RecorderConfig.MediaSourceConfig = &mediaSourceConfigDTO{
				MediaURISecretARN: m.MediaURISecretARN,
				MediaURIType:      m.MediaURIType,
			}
		}
	}

	if u := cfg.Uploader; u != nil {
		d.UploaderConfig = &uploaderConfigDTO{ScheduleConfig: scheduleToDTO(u.Schedule)}
	}

	if del := cfg.Deletion; del != nil {
		d.DeletionConfig = &deletionConfigDTO{
			DeleteAfterUpload:    del.DeleteAfterUpload,
			EdgeRetentionInHours: del.EdgeRetentionInHours,
		}
		if l := del.LocalSize; l != nil {
			d.DeletionConfig.LocalSizeConfig = &localSizeConfigDTO{
				StrategyOnFullSize:    l.StrategyOnFullSize,
				MaxLocalMediaSizeInMB: l.MaxLocalMediaSizeInMB,
			}
		}
	}

	return d
}

type edgeStreamRequest struct {
	EdgeConfig *edgeConfigDTO `json:"EdgeConfig,omitempty"`
	StreamARN  string         `json:"StreamARN,omitempty"`
	StreamName string         `json:"StreamName,omitempty"`
}

type edgeStateResponse struct {
	EdgeConfig      *edgeConfigDTO `json:"EdgeConfig"`
	StreamARN       string         `json:"StreamARN"`
	StreamName      string         `json:"StreamName"`
	SyncStatus      string         `json:"SyncStatus"`
	CreationTime    float64        `json:"CreationTime"`
	LastUpdatedTime float64        `json:"LastUpdatedTime"`
}

func edgeStateToResponse(e *EdgeState) edgeStateResponse {
	return edgeStateResponse{
		CreationTime:    awstime.Epoch(e.CreationTime),
		EdgeConfig:      edgeConfigToDTO(e.Config),
		LastUpdatedTime: awstime.Epoch(e.LastUpdatedTime),
		StreamARN:       e.StreamARN,
		StreamName:      e.StreamName,
		SyncStatus:      e.SyncStatus,
	}
}

type listEdgeAgentConfigurationsRequest struct {
	HubDeviceArn string `json:"HubDeviceArn,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
}

type listEdgeAgentConfigurationsResponse struct {
	NextToken   string              `json:"NextToken,omitempty"`
	EdgeConfigs []edgeStateResponse `json:"EdgeConfigs"`
}
