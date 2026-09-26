package kinesisvideo

import (
	"maps"
	"time"
)

// Stream status values (shared with Channel -- AWS models both under one "Status" enum).
const (
	statusActive = "ACTIVE"
)

// Stream is the persisted representation of a Kinesis video stream.
type Stream struct {
	CreationTime         time.Time
	ImageGeneration      *ImageGenerationConfig
	Notification         *NotificationConfig
	Tags                 map[string]string
	Name                 string
	ARN                  string
	Status               string
	Version              string
	DeviceName           string
	KmsKeyID             string
	MediaType            string
	DefaultStorageTier   string
	DataRetentionInHours int32
}

func (s *Stream) clone() *Stream {
	if s == nil {
		return nil
	}

	cp := *s
	cp.Tags = make(map[string]string, len(s.Tags))
	maps.Copy(cp.Tags, s.Tags)

	if s.ImageGeneration != nil {
		ig := *s.ImageGeneration
		ig.FormatConfig = make(map[string]string, len(s.ImageGeneration.FormatConfig))
		maps.Copy(ig.FormatConfig, s.ImageGeneration.FormatConfig)
		cp.ImageGeneration = &ig
	}

	if s.Notification != nil {
		n := *s.Notification
		cp.Notification = &n
	}

	return &cp
}

// ImageGenerationConfig mirrors types.ImageGenerationConfiguration.
type ImageGenerationConfig struct {
	FormatConfig      map[string]string
	DestinationRegion string
	URI               string
	Format            string
	ImageSelectorType string
	Status            string
	SamplingInterval  int32
	HeightPixels      int32
	WidthPixels       int32
}

// NotificationConfig mirrors types.NotificationConfiguration.
type NotificationConfig struct {
	DestinationURI string
	Status         string
}

// Channel is the persisted representation of a signaling channel.
type Channel struct {
	CreationTime      time.Time
	Tags              map[string]string
	Name              string
	ARN               string
	Type              string
	Status            string
	Version           string
	MessageTTLSeconds int32
}

func (c *Channel) clone() *Channel {
	if c == nil {
		return nil
	}

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp
}

// StreamNameCondition mirrors types.StreamNameCondition.
type StreamNameCondition struct {
	ComparisonOperator string
	ComparisonValue    string
}

// ChannelNameCondition mirrors types.ChannelNameCondition.
type ChannelNameCondition struct {
	ComparisonOperator string
	ComparisonValue    string
}
