package kinesisvideo

// StorageBackend is the interface for the Kinesis Video Streams backend.
type StorageBackend interface {
	CreateStream(accountID, region, name, deviceName, mediaType, kmsKeyID, defaultStorageTier string,
		dataRetentionInHours int32, tags map[string]string) (*Stream, error)
	DescribeStream(name, streamARN string) (*Stream, error)
	ListStreams(nextToken string, maxResults int, condition *StreamNameCondition) ([]*Stream, string, error)
	UpdateStream(name, streamARN, currentVersion, deviceName, mediaType string) error
	DeleteStream(streamARN, currentVersion string) error
	UpdateDataRetention(name, streamARN, currentVersion, operation string, changeInHours int32) error

	TagStream(name, streamARN string, tags map[string]string) error
	UntagStream(name, streamARN string, tagKeys []string) error
	ListTagsForStream(name, streamARN string) (map[string]string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	GetDataEndpoint(name, streamARN, apiName, region string) (string, error)

	DescribeImageGenerationConfiguration(name, streamARN string) (*ImageGenerationConfig, error)
	UpdateImageGenerationConfiguration(name, streamARN string, cfg *ImageGenerationConfig) error
	DescribeNotificationConfiguration(name, streamARN string) (*NotificationConfig, error)
	UpdateNotificationConfiguration(name, streamARN string, cfg *NotificationConfig) error

	CreateSignalingChannel(
		accountID, region, name, channelType string, messageTTLSeconds int32, tags map[string]string,
	) (*Channel, error)
	DescribeSignalingChannel(name, channelARN string) (*Channel, error)
	ListSignalingChannels(nextToken string, maxResults int, condition *ChannelNameCondition) ([]*Channel, string, error)
	UpdateSignalingChannel(channelARN, currentVersion string, messageTTLSeconds *int32) error
	DeleteSignalingChannel(channelARN, currentVersion string) error

	DescribeStreamStorageConfiguration(name, streamARN string) (*Stream, error)
	UpdateStreamStorageConfiguration(name, streamARN, currentVersion, defaultStorageTier string) error
	DescribeMediaStorageConfiguration(name, channelARN string) (*MediaStorage, error)
	UpdateMediaStorageConfiguration(channelARN string, cfg MediaStorage) error
	GetSignalingChannelEndpoint(channelARN, role, region string, protocols []string) ([]ChannelEndpoint, error)

	StartEdgeConfigurationUpdate(name, streamARN string, cfg EdgeConfig) (*EdgeState, error)
	DescribeEdgeConfiguration(name, streamARN string) (*EdgeState, error)
	DeleteEdgeConfiguration(name, streamARN string) error
	ListEdgeAgentConfigurations(hubDeviceARN, nextToken string, maxResults int) ([]*EdgeState, string, error)

	Reset()
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
