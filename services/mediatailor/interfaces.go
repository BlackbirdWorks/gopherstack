package mediatailor

import "time"

// StorageBackend is the interface for MediaTailor storage operations.
type StorageBackend interface {
	// PlaybackConfiguration
	PutPlaybackConfiguration(
		name, adDecisionServerURL, videoContentSourceURL string,
		tags map[string]string,
	) (*PlaybackConfiguration, error)
	GetPlaybackConfiguration(name string) (*PlaybackConfiguration, error)
	DeletePlaybackConfiguration(name string) error
	ListPlaybackConfigurations(maxResults int, nextToken string) ([]*PlaybackConfigurationSummary, string, error)

	// Channel
	CreateChannel(name, playbackMode, tier string, outputs []OutputItem, tags map[string]string) (*Channel, error)
	DescribeChannel(name string) (*Channel, error)
	UpdateChannel(name string, outputs []OutputItem) (*Channel, error)
	DeleteChannel(name string) error
	ListChannels(maxResults int, nextToken string) ([]*ChannelSummary, string, error)
	StartChannel(name string) error
	StopChannel(name string) error

	// SourceLocation
	CreateSourceLocation(name, baseURL string, tags map[string]string) (*SourceLocation, error)
	DescribeSourceLocation(name string) (*SourceLocation, error)
	UpdateSourceLocation(name, baseURL string) (*SourceLocation, error)
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
	ListVodSources(sourceLocationName string, maxResults int, nextToken string) ([]*VodSourceSummary, string, error)

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
	ListLiveSources(sourceLocationName string, maxResults int, nextToken string) ([]*LiveSourceSummary, string, error)

	// PrefetchSchedule
	CreatePrefetchSchedule(
		playbackConfigName, name, streamID string,
		retrieval *PrefetchRetrieval,
		consumption *PrefetchConsumption,
	) (*PrefetchSchedule, error)
	GetPrefetchSchedule(playbackConfigName, name string) (*PrefetchSchedule, error)
	DeletePrefetchSchedule(playbackConfigName, name string) error
	ListPrefetchSchedules(
		playbackConfigName string,
		maxResults int,
		nextToken string,
	) ([]*PrefetchSchedule, string, error)

	// Program
	CreateProgram(
		channelName, programName, sourceLocationName, vodSourceName, liveSourceName string,
		tags map[string]string,
	) (*Program, error)
	DescribeProgram(channelName, programName string) (*Program, error)
	UpdateProgram(channelName, programName string) (*Program, error)
	DeleteProgram(channelName, programName string) error
	GetChannelSchedule(channelName string, maxResults int, nextToken string) ([]*ProgramScheduleEntry, string, error)

	// ChannelPolicy
	PutChannelPolicy(channelName, policy string) error
	GetChannelPolicy(channelName string) (string, error)
	DeleteChannelPolicy(channelName string) error

	// Function
	PutFunction(functionID, functionType, description string, tags map[string]string) (*Function, error)
	GetFunction(functionID string) (*Function, error)
	DeleteFunction(functionID string) error
	ListFunctions(maxResults int, nextToken string) ([]*FunctionSummary, string, error)

	// Logs
	ConfigureLogsForChannel(channelName string, logTypes []string) (string, []string, error)
	ConfigureLogsForPlaybackConfiguration(playbackConfigName string, percentEnabled int) (string, int, error)

	// Tags
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// PlaybackConfiguration represents a MediaTailor playback configuration.
// Tags first: reduces GC pointer scan.
type PlaybackConfiguration struct {
	Tags                        map[string]string
	Name                        string
	AdDecisionServerURL         string
	VideoContentSourceURL       string
	PlaybackConfigurationARN    string
	PlaybackEndpointPrefix      string
	SessionInitializationPrefix string
}

// PlaybackConfigurationSummary is a playback configuration in a list response.
type PlaybackConfigurationSummary struct {
	Tags                     map[string]string
	Name                     string
	AdDecisionServerURL      string
	VideoContentSourceURL    string
	PlaybackConfigurationARN string
}

// Channel represents a MediaTailor channel.
// Tags first, strings before slice: reduces GC pointer scan.
type Channel struct {
	Tags         map[string]string
	ARN          string
	Name         string
	PlaybackMode string
	ChannelState string
	Tier         string
	Outputs      []OutputItem
	CreationTime time.Time
	LastModified time.Time
}

// ChannelSummary is a channel in a list response.
type ChannelSummary struct {
	Tags         map[string]string
	Name         string
	ARN          string
	PlaybackMode string
	ChannelState string
	Tier         string
	CreationTime time.Time
	LastModified time.Time
}

// OutputItem represents a channel output configuration.
// Pointer fields first: reduces GC pointer scan.
type OutputItem struct {
	HlsPlaylistSettings  *HlsPlaylistSettings  `json:"hlsPlaylistSettings,omitempty"`
	DashPlaylistSettings *DashPlaylistSettings  `json:"dashPlaylistSettings,omitempty"`
	ManifestName         string                 `json:"manifestName"`
	SourceGroup          string                 `json:"sourceGroup"`
}

// HlsPlaylistSettings holds HLS playlist configuration.
type HlsPlaylistSettings struct {
	ManifestWindowSeconds int `json:"manifestWindowSeconds"`
}

// DashPlaylistSettings holds DASH playlist configuration.
type DashPlaylistSettings struct {
	ManifestWindowSeconds  int `json:"manifestWindowSeconds"`
	MinBufferTimeSeconds   int `json:"minBufferTimeSeconds"`
	MinUpdatePeriodSeconds int `json:"minUpdatePeriodSeconds"`
	SuggestedPresentationDelaySeconds int `json:"suggestedPresentationDelaySeconds"`
}

// SourceLocation represents a MediaTailor source location.
type SourceLocation struct {
	Tags                 map[string]string
	Name                 string
	ARN                  string
	HTTPConfigurationURL string
	CreationTime         time.Time
	LastModified         time.Time
}

// SourceLocationSummary is a source location in a list response.
type SourceLocationSummary struct {
	Tags                 map[string]string
	Name                 string
	ARN                  string
	HTTPConfigurationURL string
	CreationTime         time.Time
	LastModified         time.Time
}

// VodSource represents a MediaTailor VOD source.
// Tags first, strings before slice: reduces GC pointer scan.
type VodSource struct {
	Tags                      map[string]string
	ARN                       string
	SourceLocationName        string
	VodSourceName             string
	HTTPPackageConfigurations []HTTPPackageConfiguration
	CreationTime              time.Time
	LastModified              time.Time
}

// VodSourceSummary is a VOD source in a list response.
type VodSourceSummary struct {
	Tags               map[string]string
	SourceLocationName string
	VodSourceName      string
	ARN                string
	CreationTime       time.Time
	LastModified       time.Time
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
	Tags                      map[string]string
	ARN                       string
	SourceLocationName        string
	LiveSourceName            string
	HTTPPackageConfigurations []HTTPPackageConfiguration
	CreationTime              time.Time
	LastModified              time.Time
}

// LiveSourceSummary is a live source in a list response.
type LiveSourceSummary struct {
	Tags               map[string]string
	SourceLocationName string
	LiveSourceName     string
	ARN                string
	CreationTime       time.Time
	LastModified       time.Time
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
	Retrieval                 *PrefetchRetrieval
	Consumption               *PrefetchConsumption
	ARN                       string
	Name                      string
	PlaybackConfigurationName string
	StreamID                  string
	CreationTime              time.Time
}

// Program represents a MediaTailor program within a channel.
type Program struct {
	Tags               map[string]string
	ARN                string
	ChannelName        string
	ProgramName        string
	SourceLocationName string
	VodSourceName      string
	LiveSourceName     string
	ScheduledStartTime time.Time
	DurationInSeconds  int64
	CreationTime       time.Time
}

// ProgramScheduleEntry is a program as returned in a channel schedule.
type ProgramScheduleEntry struct {
	ARN                        string
	ChannelName                string
	ProgramName                string
	SourceLocationName         string
	VodSourceName              string
	LiveSourceName             string
	ScheduleEntryType          string
	ApproximateDurationSeconds int64
}

// Function represents a MediaTailor function.
type Function struct {
	Tags         map[string]string
	FunctionID   string
	FunctionType string
	ARN          string
	Description  string
}

// FunctionSummary is a function in a list response.
type FunctionSummary struct {
	Tags         map[string]string
	FunctionID   string
	FunctionType string
	ARN          string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
