package mediatailor

import (
	"context"
	"time"
)

// StorageBackend is the interface for MediaTailor storage operations.
type StorageBackend interface {
	// PlaybackConfiguration
	PutPlaybackConfiguration(
		name, adDecisionServerURL, videoContentSourceURL string,
		tags map[string]string,
		extra map[string]any,
	) (*PlaybackConfiguration, error)
	GetPlaybackConfiguration(name string) (*PlaybackConfiguration, error)
	DeletePlaybackConfiguration(name string) error
	ListPlaybackConfigurations(
		maxResults int,
		nextToken string,
	) ([]*PlaybackConfigurationSummary, string, error)

	// Channel
	CreateChannel(
		name, playbackMode, tier string,
		outputs []OutputItem,
		fillerSlate *SlateSource,
		audiences []string,
		timeShift *TimeShiftConfiguration,
		tags map[string]string,
	) (*Channel, error)
	DescribeChannel(name string) (*Channel, error)
	UpdateChannel(
		name string,
		outputs []OutputItem,
		fillerSlate *SlateSource,
		audiences []string,
		timeShift *TimeShiftConfiguration,
	) (*Channel, error)
	DeleteChannel(name string) error
	ListChannels(maxResults int, nextToken string) ([]*ChannelSummary, string, error)
	StartChannel(name string) error
	StopChannel(name string) error

	// SourceLocation
	CreateSourceLocation(
		name, baseURL string,
		accessConfig *AccessConfiguration,
		defaultSegmentDelivery *DefaultSegmentDeliveryConfiguration,
		segmentDeliveryConfigs []SegmentDeliveryConfiguration,
		tags map[string]string,
	) (*SourceLocation, error)
	DescribeSourceLocation(name string) (*SourceLocation, error)
	UpdateSourceLocation(
		name, baseURL string,
		accessConfig *AccessConfiguration,
		defaultSegmentDelivery *DefaultSegmentDeliveryConfiguration,
		segmentDeliveryConfigs []SegmentDeliveryConfiguration,
	) (*SourceLocation, error)
	DeleteSourceLocation(name string) error
	ListSourceLocations(maxResults int, nextToken string) ([]*SourceLocationSummary, string, error)

	// VodSource
	CreateVodSource(
		sourceLocationName, vodSourceName string,
		httpPackageConfigurations []HTTPPackageConfiguration,
		tags map[string]string,
	) (*VodSource, error)
	DescribeVodSource(sourceLocationName, vodSourceName string) (*VodSource, error)
	UpdateVodSource(
		sourceLocationName, vodSourceName string,
		httpPackageConfigurations []HTTPPackageConfiguration,
	) (*VodSource, error)
	DeleteVodSource(sourceLocationName, vodSourceName string) error
	ListVodSources(
		sourceLocationName string,
		maxResults int,
		nextToken string,
	) ([]*VodSourceSummary, string, error)

	// LiveSource
	CreateLiveSource(
		sourceLocationName, liveSourceName string,
		httpPackageConfigurations []HTTPPackageConfiguration,
		tags map[string]string,
	) (*LiveSource, error)
	DescribeLiveSource(sourceLocationName, liveSourceName string) (*LiveSource, error)
	UpdateLiveSource(
		sourceLocationName, liveSourceName string,
		httpPackageConfigurations []HTTPPackageConfiguration,
	) (*LiveSource, error)
	DeleteLiveSource(sourceLocationName, liveSourceName string) error
	ListLiveSources(
		sourceLocationName string,
		maxResults int,
		nextToken string,
	) ([]*LiveSourceSummary, string, error)

	// PrefetchSchedule
	CreatePrefetchSchedule(
		playbackConfigName, name, scheduleType, streamID string,
		retrieval *PrefetchRetrieval,
		consumption *PrefetchConsumption,
		recurringConfig map[string]any,
		tags map[string]string,
	) (*PrefetchSchedule, error)
	GetPrefetchSchedule(playbackConfigName, name string) (*PrefetchSchedule, error)
	DeletePrefetchSchedule(playbackConfigName, name string) error
	ListPrefetchSchedules(
		playbackConfigName, scheduleType, streamID string,
		maxResults int,
		nextToken string,
	) ([]*PrefetchSchedule, string, error)

	// Program
	CreateProgram(
		channelName, programName, sourceLocationName, vodSourceName, liveSourceName string,
		scheduleConfig *ScheduleConfiguration,
		adBreaks []AdBreak,
		audienceMedia []AudienceMedia,
		tags map[string]string,
	) (*Program, error)
	DescribeProgram(channelName, programName string) (*Program, error)
	UpdateProgram(
		channelName, programName string,
		scheduleConfig *UpdateProgramScheduleConfiguration,
		adBreaks []AdBreak,
		audienceMedia []AudienceMedia,
	) (*Program, error)
	DeleteProgram(channelName, programName string) error
	GetChannelSchedule(
		channelName string,
		maxResults int,
		nextToken string,
	) ([]*ProgramScheduleEntry, string, error)

	// ChannelPolicy
	PutChannelPolicy(channelName, policy string) error
	GetChannelPolicy(channelName string) (string, error)
	DeleteChannelPolicy(channelName string) error

	// Function
	PutFunction(
		functionID, functionType, description string,
		customOutput, httpRequest, sequentialExecutor map[string]any,
		tags map[string]string,
	) (*Function, error)
	GetFunction(functionID string) (*Function, error)
	DeleteFunction(functionID string) error
	ListFunctions(maxResults int, nextToken string) ([]*FunctionSummary, string, error)

	// Logs
	ConfigureLogsForChannel(channelName string, logTypes []string) (string, []string, error)
	ConfigureLogsForPlaybackConfiguration(
		playbackConfigName string,
		percentEnabled int,
		enabledLoggingStrategies []string,
		adsInteractionLog, manifestServiceInteractionLog map[string]any,
	) (*PlaybackConfigurationLogConfiguration, error)

	// Tags
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// PlaybackConfiguration represents a MediaTailor playback configuration.
// Tags first: reduces GC pointer scan.
type PlaybackConfiguration struct {
	Tags                        map[string]string
	LogConfiguration            *PlaybackConfigurationLogConfiguration
	Extra                       map[string]any
	Name                        string
	AdDecisionServerURL         string
	VideoContentSourceURL       string
	PlaybackConfigurationARN    string
	PlaybackEndpointPrefix      string
	SessionInitializationPrefix string
	HlsManifestEndpointPrefix   string
	// DualStackPlaybackEndpointPrefix and DualStackSessionInitializationEndpointPrefix
	// are response-only members of the real PlaybackConfiguration type (no
	// PutPlaybackConfigurationInput member sets them; aws-sdk-go-v2/service/mediatailor
	// @v1.63.4 types/types.go:1049,1053). gopherstack never populates them -- doing so
	// would mean fabricating a dual-stack URL a client might actually dial, which is
	// worse than the field being absent, matching a real account with dual-stack
	// endpoints not provisioned.
	DualStackPlaybackEndpointPrefix              string
	DualStackSessionInitializationEndpointPrefix string
	// HlsDualStackManifestEndpointPrefix is HlsConfiguration's own response-only
	// dual-stack member (no PutPlaybackConfigurationInput counterpart;
	// aws-sdk-go-v2/service/mediatailor@v1.63.4 types/types.go:688). Same
	// reasoning as the two fields above: never populated, absent rather than
	// a fabricated dialable URL.
	HlsDualStackManifestEndpointPrefix string
}

// PlaybackConfigurationLogConfiguration is the logging configuration for a
// playback configuration, set via ConfigureLogsForPlaybackConfiguration.
type PlaybackConfigurationLogConfiguration struct {
	AdsInteractionLog             map[string]any
	ManifestServiceInteractionLog map[string]any
	EnabledLoggingStrategies      []string
	PercentEnabled                int
}

// SlateSource identifies a slate source for channel filler slate.
type SlateSource struct {
	SourceLocationName string
	VodSourceName      string
}

// PlaybackConfigurationSummary is a playback configuration in a list response.
type PlaybackConfigurationSummary struct {
	Tags                        map[string]string
	LogConfiguration            *PlaybackConfigurationLogConfiguration
	Extra                       map[string]any
	Name                        string
	AdDecisionServerURL         string
	PlaybackEndpointPrefix      string
	SessionInitializationPrefix string
	HlsManifestEndpointPrefix   string
	VideoContentSourceURL       string
	PlaybackConfigurationARN    string
}

// TimeShiftConfiguration is the time-shifted viewing configuration for a channel.
type TimeShiftConfiguration struct {
	MaxTimeDelaySeconds int
}

// ChannelLogConfiguration is the log configuration for a channel, set via
// ConfigureLogsForChannel.
type ChannelLogConfiguration struct {
	LogTypes []string
}

// Channel represents a MediaTailor channel.
// Tags first, strings before slice: reduces GC pointer scan.
type Channel struct {
	FillerSlate      *SlateSource
	TimeShift        *TimeShiftConfiguration
	CreationTime     time.Time
	LastModified     time.Time
	Tags             map[string]string
	LogConfiguration ChannelLogConfiguration
	ARN              string
	Name             string
	PlaybackMode     string
	ChannelState     string
	Tier             string
	Outputs          []OutputItem
	Audiences        []string
}

// ChannelSummary is a channel in a list response.
type ChannelSummary struct {
	FillerSlate      *SlateSource
	TimeShift        *TimeShiftConfiguration
	CreationTime     time.Time
	LastModified     time.Time
	Tags             map[string]string
	LogConfiguration ChannelLogConfiguration
	Name             string
	ARN              string
	PlaybackMode     string
	ChannelState     string
	Tier             string
	Outputs          []OutputItem
	Audiences        []string
}

// OutputItem represents a channel output configuration.
// Pointer fields first: reduces GC pointer scan.
type OutputItem struct {
	HlsPlaylistSettings  *HlsPlaylistSettings  `json:"hlsPlaylistSettings,omitempty"`
	DashPlaylistSettings *DashPlaylistSettings `json:"dashPlaylistSettings,omitempty"`
	ManifestName         string                `json:"manifestName"`
	SourceGroup          string                `json:"sourceGroup"`
}

// HlsPlaylistSettings holds HLS playlist configuration.
type HlsPlaylistSettings struct {
	ManifestWindowSeconds int `json:"manifestWindowSeconds"`
}

// DashPlaylistSettings holds DASH playlist configuration.
type DashPlaylistSettings struct {
	ManifestWindowSeconds             int `json:"manifestWindowSeconds"`
	MinBufferTimeSeconds              int `json:"minBufferTimeSeconds"`
	MinUpdatePeriodSeconds            int `json:"minUpdatePeriodSeconds"`
	SuggestedPresentationDelaySeconds int `json:"suggestedPresentationDelaySeconds"`
}

// AccessConfiguration is the authentication configuration for a source
// location's HttpConfiguration.BaseUrl.
type AccessConfiguration struct {
	SecretsManagerAccessTokenConfiguration *SecretsManagerAccessTokenConfiguration
	AccessType                             string
}

// SecretsManagerAccessTokenConfiguration is the AWS Secrets Manager access
// token configuration for AccessConfiguration's
// SECRETS_MANAGER_ACCESS_TOKEN AccessType.
type SecretsManagerAccessTokenConfiguration struct {
	HeaderName      string
	SecretArn       string
	SecretStringKey string
}

// DefaultSegmentDeliveryConfiguration is the default host for a source
// location's segment delivery server.
type DefaultSegmentDeliveryConfiguration struct {
	BaseURL string
}

// SegmentDeliveryConfiguration is a named segment delivery server for a
// source location.
type SegmentDeliveryConfiguration struct {
	BaseURL string
	Name    string
}

// SourceLocation represents a MediaTailor source location.
type SourceLocation struct {
	CreationTime                        time.Time
	LastModified                        time.Time
	AccessConfiguration                 *AccessConfiguration
	DefaultSegmentDeliveryConfiguration *DefaultSegmentDeliveryConfiguration
	Tags                                map[string]string
	Name                                string
	ARN                                 string
	HTTPConfigurationURL                string
	SegmentDeliveryConfigurations       []SegmentDeliveryConfiguration
}

// SourceLocationSummary is a source location in a list response.
type SourceLocationSummary struct {
	CreationTime                        time.Time
	LastModified                        time.Time
	AccessConfiguration                 *AccessConfiguration
	DefaultSegmentDeliveryConfiguration *DefaultSegmentDeliveryConfiguration
	Tags                                map[string]string
	Name                                string
	ARN                                 string
	HTTPConfigurationURL                string
	SegmentDeliveryConfigurations       []SegmentDeliveryConfiguration
}

// VodSource represents a MediaTailor VOD source.
// Tags first, strings before slice: reduces GC pointer scan.
type VodSource struct {
	CreationTime              time.Time
	LastModified              time.Time
	Tags                      map[string]string
	ARN                       string
	SourceLocationName        string
	VodSourceName             string
	HTTPPackageConfigurations []HTTPPackageConfiguration
}

// VodSourceSummary is a VOD source in a list response.
type VodSourceSummary struct {
	CreationTime              time.Time
	LastModified              time.Time
	Tags                      map[string]string
	SourceLocationName        string
	VodSourceName             string
	ARN                       string
	HTTPPackageConfigurations []HTTPPackageConfiguration
}

// HTTPPackageConfiguration is a packaging configuration for a VOD source.
type HTTPPackageConfiguration struct {
	Path        string `json:"path"`
	SourceGroup string `json:"sourceGroup"`
	Type        string `json:"type"`
}

// LiveSource represents a MediaTailor live source.
// Tags first, strings before slice: reduces GC pointer scan.
type LiveSource struct {
	CreationTime              time.Time
	LastModified              time.Time
	Tags                      map[string]string
	ARN                       string
	SourceLocationName        string
	LiveSourceName            string
	HTTPPackageConfigurations []HTTPPackageConfiguration
}

// LiveSourceSummary is a live source in a list response.
type LiveSourceSummary struct {
	CreationTime              time.Time
	LastModified              time.Time
	Tags                      map[string]string
	SourceLocationName        string
	LiveSourceName            string
	ARN                       string
	HTTPPackageConfigurations []HTTPPackageConfiguration
}

// PrefetchRetrieval holds the retrieval configuration for a prefetch schedule.
type PrefetchRetrieval struct {
	DynamicVariables map[string]string
	StartTime        time.Time
	EndTime          time.Time
}

// PrefetchConsumption holds the consumption configuration for a prefetch schedule.
type PrefetchConsumption struct {
	StartTime time.Time
	EndTime   time.Time
}

// PrefetchSchedule represents a MediaTailor prefetch schedule.
type PrefetchSchedule struct {
	CreationTime                   time.Time
	Retrieval                      *PrefetchRetrieval
	Consumption                    *PrefetchConsumption
	RecurringPrefetchConfiguration map[string]any
	Tags                           map[string]string
	ARN                            string
	Name                           string
	PlaybackConfigurationName      string
	ScheduleType                   string
	StreamID                       string
}

// KeyValuePair is a SCTE35_ENHANCED ad break metadata entry.
type KeyValuePair struct {
	Key   string
	Value string
}

// SpliceInsertMessage configures the SCTE-35 splice_insert() message for an ad break.
type SpliceInsertMessage struct {
	AvailNum        int32
	AvailsExpected  int32
	SpliceEventID   int32
	UniqueProgramID int32
}

// TimeSignalMessage configures the SCTE-35 time_signal message for an ad
// break. SegmentationDescriptors is carried as decoded-JSON pass-through:
// MediaTailor stores this deeply nested SCTE-35 metadata without
// interpreting it during ad insertion, and gopherstack does not run a real
// ad-insertion pipeline, so exact round-trip (what a client PUTs is exactly
// what a client GETs back) is the correct emulation without hand-modeling
// every nested SCTE-35 field.
type TimeSignalMessage struct {
	SegmentationDescriptors []map[string]any
}

// AdBreak is an ad break configuration attached to a Program or AlternateMedia.
type AdBreak struct {
	Slate               *SlateSource
	SpliceInsertMessage *SpliceInsertMessage
	TimeSignalMessage   *TimeSignalMessage
	MessageType         string
	AdBreakMetadata     []KeyValuePair
	OffsetMillis        int64
}

// ClipRange is a VOD source clip range configuration.
type ClipRange struct {
	StartOffsetMillis int64
	EndOffsetMillis   int64
}

// AlternateMedia is a playlist of media that plays instead of the default
// media on a particular program, scoped to an Audience.
type AlternateMedia struct {
	ClipRange                *ClipRange
	SourceLocationName       string
	VodSourceName            string
	LiveSourceName           string
	AdBreaks                 []AdBreak
	ScheduledStartTimeMillis int64
	DurationMillis           int64
}

// AudienceMedia pairs an audience with the AlternateMedia MediaTailor plays
// for it.
type AudienceMedia struct {
	Audience       string
	AlternateMedia []AlternateMedia
}

// Transition is a program's schedule transition configuration, required by
// CreateProgram's ScheduleConfiguration.
type Transition struct {
	RelativePosition         string
	Type                     string
	RelativeProgram          string
	DurationMillis           int64
	ScheduledStartTimeMillis int64
}

// ScheduleConfiguration is a program's schedule configuration, required by
// CreateProgram.
type ScheduleConfiguration struct {
	ClipRange  *ClipRange
	Transition Transition
}

// UpdateProgramTransition is a program's schedule transition update, used by
// UpdateProgram.
type UpdateProgramTransition struct {
	DurationMillis           int64
	ScheduledStartTimeMillis int64
}

// UpdateProgramScheduleConfiguration is a program's schedule update
// configuration, required by UpdateProgram (its members are individually
// optional -- an empty object is a valid "change nothing" update).
type UpdateProgramScheduleConfiguration struct {
	ClipRange  *ClipRange
	Transition *UpdateProgramTransition
}

// Program represents a MediaTailor program within a channel.
type Program struct {
	ScheduledStartTime time.Time
	CreationTime       time.Time
	ClipRange          *ClipRange
	Tags               map[string]string
	ARN                string
	ChannelName        string
	ProgramName        string
	SourceLocationName string
	VodSourceName      string
	LiveSourceName     string
	AdBreaks           []AdBreak
	AudienceMedia      []AudienceMedia
	DurationMillis     int64
}

// ScheduleAdBreak is the schedule's ad break properties, as returned in a
// ProgramScheduleEntry.
type ScheduleAdBreak struct {
	ApproximateStartTime       time.Time
	SourceLocationName         string
	VodSourceName              string
	ApproximateDurationSeconds int64
}

// ProgramScheduleEntry is a program as returned in a channel schedule.
type ProgramScheduleEntry struct {
	ApproximateStartTime       time.Time
	ARN                        string
	ChannelName                string
	ProgramName                string
	SourceLocationName         string
	VodSourceName              string
	LiveSourceName             string
	ScheduleEntryType          string
	ScheduleAdBreaks           []ScheduleAdBreak
	Audiences                  []string
	ApproximateDurationSeconds int64
}

// Function represents a MediaTailor function. CustomOutputConfiguration,
// HTTPRequestConfiguration and SequentialExecutorConfiguration are stored as
// decoded-JSON pass-through (the FunctionType-specific config gopherstack
// does not execute/interpret, matching PlaybackConfiguration's Extra
// pattern in handler_helpers.go's extractExtraConfig/mergeExtraConfig) so a
// client reads back exactly what it Put.
type Function struct {
	CustomOutputConfiguration       map[string]any
	HTTPRequestConfiguration        map[string]any
	SequentialExecutorConfiguration map[string]any
	Tags                            map[string]string
	FunctionID                      string
	FunctionType                    string
	ARN                             string
	Description                     string
}

// FunctionSummary is a function in a list response.
type FunctionSummary struct {
	CustomOutputConfiguration       map[string]any
	HTTPRequestConfiguration        map[string]any
	SequentialExecutorConfiguration map[string]any
	Tags                            map[string]string
	FunctionID                      string
	FunctionType                    string
	ARN                             string
	Description                     string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
