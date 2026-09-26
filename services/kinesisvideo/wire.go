package kinesisvideo

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// Wire DTOs for the Kinesis Video Streams REST-JSON control plane. Field
// names match the AWS smithy model exactly (aws-sdk-go-v2/service/
// kinesisvideo@v1.41.1 serializers.go/deserializers.go emit JSON keys equal
// to the Go struct field names verbatim, no @jsonName overrides).

type tagDTO struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type streamStorageConfigurationDTO struct {
	DefaultStorageTier string `json:"DefaultStorageTier,omitempty"`
}

type streamNameConditionDTO struct {
	ComparisonOperator string `json:"ComparisonOperator,omitempty"`
	ComparisonValue    string `json:"ComparisonValue,omitempty"`
}

type channelNameConditionDTO struct {
	ComparisonOperator string `json:"ComparisonOperator,omitempty"`
	ComparisonValue    string `json:"ComparisonValue,omitempty"`
}

type singleMasterConfigurationDTO struct {
	MessageTTLSeconds int32 `json:"MessageTtlSeconds,omitempty"`
}

type streamInfoDTO struct {
	DeviceName           string  `json:"DeviceName,omitempty"`
	KmsKeyID             string  `json:"KmsKeyId,omitempty"`
	MediaType            string  `json:"MediaType,omitempty"`
	Status               string  `json:"Status,omitempty"`
	StreamARN            string  `json:"StreamARN,omitempty"`
	StreamName           string  `json:"StreamName,omitempty"`
	Version              string  `json:"Version,omitempty"`
	CreationTime         float64 `json:"CreationTime"`
	DataRetentionInHours int32   `json:"DataRetentionInHours"`
}

func streamInfoFromStream(s *Stream) streamInfoDTO {
	return streamInfoDTO{
		CreationTime:         awstime.Epoch(s.CreationTime),
		DataRetentionInHours: s.DataRetentionInHours,
		DeviceName:           s.DeviceName,
		KmsKeyID:             s.KmsKeyID,
		MediaType:            s.MediaType,
		Status:               s.Status,
		StreamARN:            s.ARN,
		StreamName:           s.Name,
		Version:              s.Version,
	}
}

type channelInfoDTO struct {
	SingleMasterConfiguration *singleMasterConfigurationDTO `json:"SingleMasterConfiguration,omitempty"`
	ChannelARN                string                        `json:"ChannelARN,omitempty"`
	ChannelName               string                        `json:"ChannelName,omitempty"`
	ChannelStatus             string                        `json:"ChannelStatus,omitempty"`
	ChannelType               string                        `json:"ChannelType,omitempty"`
	Version                   string                        `json:"Version,omitempty"`
	CreationTime              float64                       `json:"CreationTime"`
}

func channelInfoFromChannel(c *Channel) channelInfoDTO {
	return channelInfoDTO{
		ChannelARN:    c.ARN,
		ChannelName:   c.Name,
		ChannelStatus: c.Status,
		ChannelType:   c.Type,
		CreationTime:  awstime.Epoch(c.CreationTime),
		SingleMasterConfiguration: &singleMasterConfigurationDTO{
			MessageTTLSeconds: c.MessageTTLSeconds,
		},
		Version: c.Version,
	}
}

type createStreamRequest struct {
	StreamStorageConfiguration *streamStorageConfigurationDTO `json:"StreamStorageConfiguration,omitempty"`
	Tags                       map[string]string              `json:"Tags,omitempty"`
	StreamName                 string                         `json:"StreamName"`
	DeviceName                 string                         `json:"DeviceName,omitempty"`
	KmsKeyID                   string                         `json:"KmsKeyId,omitempty"`
	MediaType                  string                         `json:"MediaType,omitempty"`
	DataRetentionInHours       int32                          `json:"DataRetentionInHours,omitempty"`
}

type createStreamResponse struct {
	StreamARN string `json:"StreamARN"`
}

type describeStreamRequest struct {
	StreamARN  string `json:"StreamARN,omitempty"`
	StreamName string `json:"StreamName,omitempty"`
}

type describeStreamResponse struct {
	StreamInfo streamInfoDTO `json:"StreamInfo"`
}

type listStreamsRequest struct {
	StreamNameCondition *streamNameConditionDTO `json:"StreamNameCondition,omitempty"`
	NextToken           string                  `json:"NextToken,omitempty"`
	MaxResults          int32                   `json:"MaxResults,omitempty"`
}

type listStreamsResponse struct {
	NextToken      string          `json:"NextToken,omitempty"`
	StreamInfoList []streamInfoDTO `json:"StreamInfoList"`
}

type updateStreamRequest struct {
	CurrentVersion string `json:"CurrentVersion"`
	DeviceName     string `json:"DeviceName,omitempty"`
	MediaType      string `json:"MediaType,omitempty"`
	StreamARN      string `json:"StreamARN,omitempty"`
	StreamName     string `json:"StreamName,omitempty"`
}

type deleteStreamRequest struct {
	StreamARN      string `json:"StreamARN"`
	CurrentVersion string `json:"CurrentVersion,omitempty"`
}

type updateDataRetentionRequest struct {
	CurrentVersion             string `json:"CurrentVersion"`
	Operation                  string `json:"Operation"`
	StreamARN                  string `json:"StreamARN,omitempty"`
	StreamName                 string `json:"StreamName,omitempty"`
	DataRetentionChangeInHours int32  `json:"DataRetentionChangeInHours"`
}

type getDataEndpointRequest struct {
	APIName    string `json:"APIName"`
	StreamARN  string `json:"StreamARN,omitempty"`
	StreamName string `json:"StreamName,omitempty"`
}

type getDataEndpointResponse struct {
	DataEndpoint string `json:"DataEndpoint"`
}

type tagStreamRequest struct {
	Tags       map[string]string `json:"Tags"`
	StreamARN  string            `json:"StreamARN,omitempty"`
	StreamName string            `json:"StreamName,omitempty"`
}

type untagStreamRequest struct {
	StreamARN  string   `json:"StreamARN,omitempty"`
	StreamName string   `json:"StreamName,omitempty"`
	TagKeyList []string `json:"TagKeyList"`
}

type listTagsForStreamRequest struct {
	NextToken  string `json:"NextToken,omitempty"`
	StreamARN  string `json:"StreamARN,omitempty"`
	StreamName string `json:"StreamName,omitempty"`
}

type listTagsForStreamResponse struct {
	Tags      map[string]string `json:"Tags"`
	NextToken string            `json:"NextToken,omitempty"`
}

type tagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	Tags        []tagDTO `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeyList  []string `json:"TagKeyList"`
}

type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
	NextToken   string `json:"NextToken,omitempty"`
}

type listTagsForResourceResponse struct {
	Tags      map[string]string `json:"Tags"`
	NextToken string            `json:"NextToken,omitempty"`
}

type createSignalingChannelRequest struct {
	SingleMasterConfiguration *singleMasterConfigurationDTO `json:"SingleMasterConfiguration,omitempty"`
	ChannelName               string                        `json:"ChannelName"`
	ChannelType               string                        `json:"ChannelType,omitempty"`
	Tags                      []tagDTO                      `json:"Tags,omitempty"`
}

type createSignalingChannelResponse struct {
	ChannelARN string `json:"ChannelARN"`
}

type describeSignalingChannelRequest struct {
	ChannelARN  string `json:"ChannelARN,omitempty"`
	ChannelName string `json:"ChannelName,omitempty"`
}

type describeSignalingChannelResponse struct {
	ChannelInfo channelInfoDTO `json:"ChannelInfo"`
}

type listSignalingChannelsRequest struct {
	ChannelNameCondition *channelNameConditionDTO `json:"ChannelNameCondition,omitempty"`
	NextToken            string                   `json:"NextToken,omitempty"`
	MaxResults           int32                    `json:"MaxResults,omitempty"`
}

type listSignalingChannelsResponse struct {
	NextToken       string           `json:"NextToken,omitempty"`
	ChannelInfoList []channelInfoDTO `json:"ChannelInfoList"`
}

type updateSignalingChannelRequest struct {
	SingleMasterConfiguration *singleMasterConfigurationDTO `json:"SingleMasterConfiguration,omitempty"`
	ChannelARN                string                        `json:"ChannelARN"`
	CurrentVersion            string                        `json:"CurrentVersion"`
}

type deleteSignalingChannelRequest struct {
	ChannelARN     string `json:"ChannelARN"`
	CurrentVersion string `json:"CurrentVersion,omitempty"`
}

type imageGenerationDestinationConfigDTO struct {
	DestinationRegion string `json:"DestinationRegion"`
	URI               string `json:"Uri"`
}

type imageGenerationConfigurationDTO struct {
	DestinationConfig *imageGenerationDestinationConfigDTO `json:"DestinationConfig"`
	FormatConfig      map[string]string                    `json:"FormatConfig,omitempty"`
	Format            string                               `json:"Format"`
	ImageSelectorType string                               `json:"ImageSelectorType"`
	Status            string                               `json:"Status"`
	SamplingInterval  int32                                `json:"SamplingInterval"`
	HeightPixels      int32                                `json:"HeightPixels,omitempty"`
	WidthPixels       int32                                `json:"WidthPixels,omitempty"`
}

func imageGenerationConfigFromDTO(dto *imageGenerationConfigurationDTO) *ImageGenerationConfig {
	if dto == nil {
		return nil
	}

	cfg := &ImageGenerationConfig{
		Format:            dto.Format,
		ImageSelectorType: dto.ImageSelectorType,
		Status:            dto.Status,
		SamplingInterval:  dto.SamplingInterval,
		HeightPixels:      dto.HeightPixels,
		WidthPixels:       dto.WidthPixels,
		FormatConfig:      dto.FormatConfig,
	}

	if dto.DestinationConfig != nil {
		cfg.DestinationRegion = dto.DestinationConfig.DestinationRegion
		cfg.URI = dto.DestinationConfig.URI
	}

	return cfg
}

func imageGenerationConfigToDTO(cfg *ImageGenerationConfig) *imageGenerationConfigurationDTO {
	if cfg == nil {
		return nil
	}

	return &imageGenerationConfigurationDTO{
		DestinationConfig: &imageGenerationDestinationConfigDTO{
			DestinationRegion: cfg.DestinationRegion,
			URI:               cfg.URI,
		},
		Format:            cfg.Format,
		ImageSelectorType: cfg.ImageSelectorType,
		Status:            cfg.Status,
		SamplingInterval:  cfg.SamplingInterval,
		HeightPixels:      cfg.HeightPixels,
		WidthPixels:       cfg.WidthPixels,
		FormatConfig:      cfg.FormatConfig,
	}
}

type describeImageGenerationConfigurationRequest struct {
	StreamARN  string `json:"StreamARN,omitempty"`
	StreamName string `json:"StreamName,omitempty"`
}

type describeImageGenerationConfigurationResponse struct {
	ImageGenerationConfiguration *imageGenerationConfigurationDTO `json:"ImageGenerationConfiguration,omitempty"`
}

type updateImageGenerationConfigurationRequest struct {
	ImageGenerationConfiguration *imageGenerationConfigurationDTO `json:"ImageGenerationConfiguration,omitempty"`
	StreamARN                    string                           `json:"StreamARN,omitempty"`
	StreamName                   string                           `json:"StreamName,omitempty"`
}

type notificationDestinationConfigDTO struct {
	URI string `json:"Uri"`
}

type notificationConfigurationDTO struct {
	DestinationConfig *notificationDestinationConfigDTO `json:"DestinationConfig"`
	Status            string                            `json:"Status"`
}

func notificationConfigFromDTO(dto *notificationConfigurationDTO) *NotificationConfig {
	if dto == nil {
		return nil
	}

	cfg := &NotificationConfig{Status: dto.Status}
	if dto.DestinationConfig != nil {
		cfg.DestinationURI = dto.DestinationConfig.URI
	}

	return cfg
}

func notificationConfigToDTO(cfg *NotificationConfig) *notificationConfigurationDTO {
	if cfg == nil {
		return nil
	}

	return &notificationConfigurationDTO{
		DestinationConfig: &notificationDestinationConfigDTO{URI: cfg.DestinationURI},
		Status:            cfg.Status,
	}
}

type describeNotificationConfigurationRequest struct {
	StreamARN  string `json:"StreamARN,omitempty"`
	StreamName string `json:"StreamName,omitempty"`
}

type describeNotificationConfigurationResponse struct {
	NotificationConfiguration *notificationConfigurationDTO `json:"NotificationConfiguration,omitempty"`
}

type updateNotificationConfigurationRequest struct {
	NotificationConfiguration *notificationConfigurationDTO `json:"NotificationConfiguration,omitempty"`
	StreamARN                 string                        `json:"StreamARN,omitempty"`
	StreamName                string                        `json:"StreamName,omitempty"`
}

type errorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"Message"`
}
