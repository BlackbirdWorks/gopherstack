package medialive

import (
	"context"
	"time"
)

// StorageBackend is the interface for MediaLive storage operations.
type StorageBackend interface {
	// Channels
	CreateChannel(
		name, channelClass, roleArn string,
		anywhereSettings ChannelAnywhereSettings,
		extras ChannelCreateExtras,
		tags map[string]string,
	) (*Channel, error)
	DescribeChannel(channelID string) (*Channel, error)
	UpdateChannel(
		channelID, name, roleArn string,
		anywhereSettings ChannelAnywhereSettings,
		hasAnywhereSettings bool,
		extras ChannelUpdateExtras,
	) (*Channel, error)
	DeleteChannel(channelID string) (*Channel, error)
	ListChannels(maxResults int, nextToken string) ([]*ChannelSummary, string, error)
	StartChannel(channelID string) (*Channel, error)
	StopChannel(channelID string) (*Channel, error)

	// Inputs
	CreateInput(name, inputType, roleArn string, tags map[string]string) (*Input, error)
	DescribeInput(inputID string) (*Input, error)
	UpdateInput(inputID, name, roleArn string) (*Input, error)
	DeleteInput(inputID string) error
	ListInputs(maxResults int, nextToken string) ([]*InputSummary, string, error)

	// InputSecurityGroups
	CreateInputSecurityGroup(
		whitelistRules []WhitelistRule,
		tags map[string]string,
	) (*InputSecurityGroup, error)
	DescribeInputSecurityGroup(groupID string) (*InputSecurityGroup, error)
	UpdateInputSecurityGroup(
		groupID string,
		whitelistRules []WhitelistRule,
	) (*InputSecurityGroup, error)
	DeleteInputSecurityGroup(groupID string) error
	ListInputSecurityGroups(
		maxResults int,
		nextToken string,
	) ([]*InputSecurityGroupSummary, string, error)

	// Multiplexes
	CreateMultiplex(
		name string,
		availabilityZones []string,
		settings MultiplexSettings,
		tags map[string]string,
	) (*Multiplex, error)
	DescribeMultiplex(multiplexID string) (*Multiplex, error)
	UpdateMultiplex(multiplexID, name string, settings MultiplexSettings) (*Multiplex, error)
	DeleteMultiplex(multiplexID string) (*Multiplex, error)
	ListMultiplexes(maxResults int, nextToken string) ([]*MultiplexSummary, string, error)
	StartMultiplex(multiplexID string) (*Multiplex, error)
	StopMultiplex(multiplexID string) (*Multiplex, error)

	// MultiplexPrograms
	CreateMultiplexProgram(
		multiplexID string,
		prog MultiplexProgramSettings,
	) (*MultiplexProgram, error)
	DescribeMultiplexProgram(multiplexID, programName string) (*MultiplexProgram, error)
	UpdateMultiplexProgram(
		multiplexID string,
		prog MultiplexProgramSettings,
	) (*MultiplexProgram, error)
	DeleteMultiplexProgram(multiplexID, programName string) (*MultiplexProgram, error)
	ListMultiplexPrograms(
		multiplexID string,
		maxResults int,
		nextToken string,
	) ([]*MultiplexProgramSummary, string, error)

	// Tags
	CreateTags(resourceARN string, tags map[string]string) error
	DeleteTags(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// InputDevices
	ClaimDevice(id string) (*InputDevice, error)
	ListInputDevices(maxResults int, nextToken string) ([]*InputDevice, string, error)
	DescribeInputDevice(deviceID string) (*InputDevice, error)
	UpdateInputDevice(deviceID, name string) (*InputDevice, error)
	RebootInputDevice(deviceID string) error
	TransferInputDevice(deviceID, targetCustomerID, targetRegion, message string) error
	AcceptInputDeviceTransfer(deviceID string) error
	CancelInputDeviceTransfer(deviceID string) error
	RejectInputDeviceTransfer(deviceID string) error
	ListInputDeviceTransfers(
		transferType string,
		maxResults int,
		nextToken string,
	) ([]*InputDeviceTransfer, string, error)

	// Clusters
	CreateCluster(
		name, clusterType, instanceRoleArn string,
		networkSettings ClusterNetworkSettings,
		tags map[string]string,
	) (*Cluster, error)
	DescribeCluster(clusterID string) (*Cluster, error)
	UpdateCluster(
		clusterID, name string,
		networkSettings ClusterNetworkSettings,
		hasNetworkSettings bool,
	) (*Cluster, error)
	DeleteCluster(clusterID string) (*Cluster, error)
	ListClusters(maxResults int, nextToken string) ([]*ClusterSummary, string, error)

	// Nodes
	CreateNode(clusterID, name, role string, tags map[string]string) (*Node, error)
	DescribeNode(clusterID, nodeID string) (*Node, error)
	UpdateNode(clusterID, nodeID, name, role string) (*Node, error)
	UpdateNodeState(clusterID, nodeID, state string) (*Node, error)
	DeleteNode(clusterID, nodeID string) (*Node, error)
	ListNodes(clusterID string, maxResults int, nextToken string) ([]*NodeSummary, string, error)
	CreateNodeRegistrationScript(clusterID string) (string, error)
	ListClusterAlerts(
		clusterID string,
		maxResults int,
		nextToken string,
	) ([]map[string]any, string, error)

	// SignalMaps
	CreateSignalMap(
		name, description, discoveryEntryPointArn string,
		cwGroupIDs, ebGroupIDs []string,
		tags map[string]string,
	) (*SignalMap, error)
	GetSignalMap(identifier string) (*SignalMap, error)
	ListSignalMaps(maxResults int, nextToken string) ([]*SignalMap, string, error)
	DeleteSignalMap(identifier string) error
	StartUpdateSignalMap(
		identifier, name, description string,
		cwGroupIDs, ebGroupIDs []string,
	) (*SignalMap, error)
	StartMonitorDeployment(identifier string) (*SignalMap, error)

	// CloudWatch Alarm Template Groups
	CreateCloudWatchAlarmTemplateGroup(
		name, description string,
		tags map[string]string,
	) (*CloudWatchAlarmTemplateGroup, error)
	GetCloudWatchAlarmTemplateGroup(identifier string) (*CloudWatchAlarmTemplateGroup, error)
	ListCloudWatchAlarmTemplateGroups(
		maxResults int,
		nextToken string,
	) ([]*CloudWatchAlarmTemplateGroupSummary, string, error)
	UpdateCloudWatchAlarmTemplateGroup(
		identifier, name, description string,
	) (*CloudWatchAlarmTemplateGroup, error)
	DeleteCloudWatchAlarmTemplateGroup(identifier string) error

	// CloudWatch Alarm Templates
	CreateCloudWatchAlarmTemplate(
		name string,
		description string,
		groupIdentifier string,
		metricName string,
		namespace string,
		statistic string,
		comparisonOperator string,
		targetResourceType string,
		treatMissingData string,
		threshold float64,
		evaluationPeriods, datapointsToAlarm, period int32,
		tags map[string]string,
	) (*CloudWatchAlarmTemplate, error)
	GetCloudWatchAlarmTemplate(identifier string) (*CloudWatchAlarmTemplate, error)
	ListCloudWatchAlarmTemplates(
		maxResults int,
		nextToken string,
	) ([]*CloudWatchAlarmTemplate, string, error)
	UpdateCloudWatchAlarmTemplate(
		identifier string,
		name string,
		description string,
		groupIdentifier string,
		metricName string,
		namespace string,
		statistic string,
		comparisonOperator string,
		targetResourceType string,
		treatMissingData string,
		threshold float64,
		evaluationPeriods, datapointsToAlarm, period int32,
	) (*CloudWatchAlarmTemplate, error)
	DeleteCloudWatchAlarmTemplate(identifier string) error

	// EventBridge Rule Template Groups
	CreateEventBridgeRuleTemplateGroup(
		name, description string,
		tags map[string]string,
	) (*EventBridgeRuleTemplateGroup, error)
	GetEventBridgeRuleTemplateGroup(identifier string) (*EventBridgeRuleTemplateGroup, error)
	ListEventBridgeRuleTemplateGroups(
		maxResults int,
		nextToken string,
	) ([]*EventBridgeRuleTemplateGroupSummary, string, error)
	UpdateEventBridgeRuleTemplateGroup(
		identifier, name, description string,
	) (*EventBridgeRuleTemplateGroup, error)
	DeleteEventBridgeRuleTemplateGroup(identifier string) error

	// EventBridge Rule Templates
	CreateEventBridgeRuleTemplate(
		name, description, groupIdentifier, eventType string,
		eventTargets []EventBridgeRuleTemplateTarget,
		tags map[string]string,
	) (*EventBridgeRuleTemplate, error)
	GetEventBridgeRuleTemplate(identifier string) (*EventBridgeRuleTemplate, error)
	ListEventBridgeRuleTemplates(
		maxResults int,
		nextToken string,
	) ([]*EventBridgeRuleTemplateSummary, string, error)
	UpdateEventBridgeRuleTemplate(
		identifier, name, description, groupIdentifier, eventType string,
		eventTargets []EventBridgeRuleTemplateTarget,
	) (*EventBridgeRuleTemplate, error)
	DeleteEventBridgeRuleTemplate(identifier string) error

	// Offerings (read-only catalog)
	ListOfferings(maxResults int, nextToken string) ([]*Offering, string, error)
	DescribeOffering(offeringID string) (*Offering, error)

	// Reservations
	PurchaseOffering(
		offeringID, name string,
		count int32,
		renewalSettings RenewalSettings,
		tags map[string]string,
	) (*Reservation, error)
	ListReservations(maxResults int, nextToken string) ([]*Reservation, string, error)
	DescribeReservation(reservationID string) (*Reservation, error)
	DeleteReservation(reservationID string) (*Reservation, error)
	UpdateReservation(
		reservationID, name string,
		renewalSettings RenewalSettings,
		hasRenewalSettings bool,
	) (*Reservation, error)

	// Batch ops
	// BatchStart/BatchStop take only channel and multiplex IDs -- the real
	// BatchStartInput/BatchStopInput shapes have NO inputIds field (verified
	// against aws-sdk-go-v2/service/medialive's api_op_BatchStart.go /
	// api_op_BatchStop.go; only ChannelIds+MultiplexIds).
	BatchStart(channelIDs, multiplexIDs []string) (*BatchResult, error)
	BatchStop(channelIDs, multiplexIDs []string) (*BatchResult, error)
	// BatchDelete takes channel, input, multiplex, AND input-security-group
	// IDs -- BatchDeleteInput is the one Batch* shape with all four fields.
	BatchDelete(channelIDs, inputIDs, multiplexIDs, inputSecurityGroupIDs []string) (*BatchResult, error)
	BatchUpdateSchedule(
		channelID string,
		creates []ScheduleAction,
		deleteActionNames []string,
	) (*BatchUpdateScheduleResult, error)

	// Networks
	CreateNetwork(
		name string,
		ipPools []IPPool,
		routes []Route,
		tags map[string]string,
	) (*Network, error)
	DescribeNetwork(networkID string) (*Network, error)
	UpdateNetwork(networkID, name string, ipPools []IPPool, routes []Route) (*Network, error)
	DeleteNetwork(networkID string) (*Network, error)
	ListNetworks(maxResults int, nextToken string) ([]*Network, string, error)

	// SdiSources
	CreateSdiSource(name, sdiType, mode string, tags map[string]string) (*SdiSource, error)
	DescribeSdiSource(sdiSourceID string) (*SdiSource, error)
	UpdateSdiSource(sdiSourceID, name, sdiType, mode string) (*SdiSource, error)
	DeleteSdiSource(sdiSourceID string) (*SdiSource, error)
	ListSdiSources(maxResults int, nextToken string) ([]*SdiSource, string, error)

	// ChannelPlacementGroups (nested under a cluster)
	CreateChannelPlacementGroup(
		clusterID, name string,
		nodes []string,
		tags map[string]string,
	) (*ChannelPlacementGroup, error)
	DescribeChannelPlacementGroup(clusterID, groupID string) (*ChannelPlacementGroup, error)
	UpdateChannelPlacementGroup(
		clusterID, groupID, name string,
		nodes []string,
	) (*ChannelPlacementGroup, error)
	DeleteChannelPlacementGroup(clusterID, groupID string) (*ChannelPlacementGroup, error)
	ListChannelPlacementGroups(
		clusterID string,
		maxResults int,
		nextToken string,
	) ([]*ChannelPlacementGroup, string, error)

	// Account configuration
	DescribeAccountConfiguration() (*AccountConfiguration, error)
	UpdateAccountConfiguration(kmsKeyID string) (*AccountConfiguration, error)

	// Schedule
	DescribeSchedule(channelID string) ([]ScheduleAction, error)
	DeleteSchedule(channelID string) error

	// Alerts and versions
	ListAlerts(channelID string) ([]map[string]any, error)
	ListMultiplexAlerts(multiplexID string) ([]map[string]any, error)
	ListVersions() []ChannelEngineVersion

	// Channel lifecycle extras
	UpdateChannelClass(channelID, channelClass string) (*Channel, error)
	RestartChannelPipelines(channelID string, pipelineIDs []string) (*Channel, error)
	DescribeThumbnails(channelID string) (*Channel, error)

	// InputDevice lifecycle extras
	StartInputDevice(deviceID string) error
	StopInputDevice(deviceID string) error
	StartInputDeviceMaintenanceWindow(deviceID string) error
	DescribeInputDeviceThumbnail(deviceID string) (*InputDevice, error)

	// SignalMap monitor deployment teardown
	StartDeleteMonitorDeployment(identifier string) (*SignalMap, error)

	// Partner inputs
	CreatePartnerInput(inputID string, tags map[string]string) (*Input, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// IPPool is a CIDR pool for a Network.
type IPPool struct {
	Cidr string `json:"cidr"`
}

// Route is a static route for a Network.
type Route struct {
	Cidr    string `json:"cidr"`
	Gateway string `json:"gateway"`
}

// Network represents a MediaLive Anywhere network resource.
type Network struct {
	Tags                 map[string]string
	ARN                  string
	ID                   string
	Name                 string
	State                string
	AssociatedClusterIDs []string
	IPPools              []IPPool
	Routes               []Route
}

// SdiSource represents a MediaLive SDI source resource.
type SdiSource struct {
	ARN    string
	ID     string
	Name   string
	Type   string
	Mode   string
	State  string
	Inputs []string
}

// ChannelPlacementGroup represents a placement group within a cluster.
type ChannelPlacementGroup struct {
	Tags      map[string]string
	ARN       string
	ID        string
	Name      string
	ClusterID string
	State     string
	Channels  []string
	Nodes     []string
}

// AccountConfiguration holds account-wide MediaLive settings.
type AccountConfiguration struct {
	KmsKeyID string
}

// ChannelEngineVersion is an available channel engine version.
type ChannelEngineVersion struct {
	ExpirationDate string
	Version        string
}

// ChannelAnywhereSettings holds the MediaLive Anywhere Cluster/
// ChannelPlacementGroup association for a Channel. Wire keys
// (anywhereSettings.clusterId/channelPlacementGroupId) verified against
// aws-sdk-go-v2/service/medialive's types.AnywhereSettings. Before this fix,
// gopherstack didn't track this at all: CreateChannel/UpdateChannel
// silently dropped a caller's anywhereSettings, and Cluster.ChannelIds/
// ChannelPlacementGroup.Channels/Node.ChannelPlacementGroups (all real wire
// fields) had nothing to derive from and were hardcoded to empty lists.
type ChannelAnywhereSettings struct {
	ClusterID               string
	ChannelPlacementGroupID string
}

// hasAnywhereSettings reports whether s has any real content, so callers can
// omit an empty "anywhereSettings" key the same way a real, non-Anywhere
// Channel omits it entirely.
func (s ChannelAnywhereSettings) hasAnywhereSettings() bool {
	return s.ClusterID != "" || s.ChannelPlacementGroupID != ""
}

// --- Channel extended settings (gopherstack-jb9i) ---
//
// CreateChannelInput/UpdateChannelInput have 17 top-level members (verified
// against aws-sdk-go-v2/service/medialive@v1.97.2's api_op_CreateChannel.go/
// api_op_UpdateChannel.go); before this fix gopherstack modeled 5
// (name/channelClass/roleArn/tags/anywhereSettings). The 12 added here are
// CdiInputSpecification/ChannelEngineVersion/ChannelSecurityGroups/
// Destinations/EncoderSettings/InferenceSettings/InputAttachments/
// InputSpecification/LinkedChannelSettings/LogLevel/Maintenance/Vpc.
// EncoderSettings is the deepest: it fans out into per-codec union types
// (AudioCodecSettings/VideoCodecSettings, each ~20 variants) and
// per-output-technology union types (OutputGroupSettings/OutputSettings,
// each ~10-13 variants) that are genuinely impractical to hand-model in
// full -- every EncoderSettings sub-type below documents exactly what is
// and is not modeled; see also PARITY.md's gaps entry.

// CdiInputSpecification specifies the maximum CDI input resolution for a
// channel. Wire key "cdiInputSpecification.resolution" -- verified against
// types.CdiInputSpecification.
type CdiInputSpecification struct {
	Resolution string
}

func (s CdiInputSpecification) hasCdiInputSpecification() bool { return s.Resolution != "" }

// InputSpecification describes the class of network/file inputs a channel
// expects. Wire keys "inputSpecification.codec/maximumBitrate/resolution" --
// verified against types.InputSpecification.
type InputSpecification struct {
	Codec          string
	MaximumBitrate string
	Resolution     string
}

func (s InputSpecification) hasInputSpecification() bool {
	return s.Codec != "" || s.MaximumBitrate != "" || s.Resolution != ""
}

// ChannelVpcSettings holds the caller-supplied VPC output configuration
// (request shape types.VpcOutputSettings: subnetIds/publicAddressAllocationIds/
// securityGroupIds). The response shape (types.VpcOutputSettingsDescription)
// additionally reports availabilityZones/networkInterfaceIds -- values
// MediaLive computes from a real VPC/ENI integration that gopherstack does
// not have; left omitted on output rather than fabricated (same convention
// as ChannelEngineVersion's ExpirationDate below).
type ChannelVpcSettings struct {
	SubnetIDs                  []string
	PublicAddressAllocationIDs []string
	SecurityGroupIDs           []string
}

func (v ChannelVpcSettings) hasVpc() bool { return len(v.SubnetIDs) > 0 }

// ChannelMaintenance holds a channel's maintenance window configuration.
// Create accepts day/startTime (types.MaintenanceCreateSettings); Update
// additionally accepts scheduledDate (types.MaintenanceUpdateSettings). The
// response shape (types.MaintenanceStatus) also reports a computed
// maintenanceDeadline that gopherstack has no scheduler to derive -- always
// omitted, never fabricated.
type ChannelMaintenance struct {
	Day           string
	StartTime     string
	ScheduledDate string
}

func (m ChannelMaintenance) hasMaintenance() bool {
	return m.Day != "" || m.StartTime != "" || m.ScheduledDate != ""
}

// AudioFeedInputMapping maps an audio selector in the channel to a feed
// input on an associated Elemental Inference feed. Wire keys
// "audioSelectorName"/"feedInput" -- verified against types.AudioFeedInput.
type AudioFeedInputMapping struct {
	AudioSelectorName string
	FeedInput         string
}

// ChannelInferenceSettings configures Elemental Inference features for a
// channel. Wire keys "audioFeedInputs"/"feedArn" -- verified against
// types.InferenceSettings/types.DescribeInferenceSettings (identical shape
// on request and response).
type ChannelInferenceSettings struct {
	FeedArn         string
	AudioFeedInputs []AudioFeedInputMapping
}

func (s ChannelInferenceSettings) hasInferenceSettings() bool {
	return len(s.AudioFeedInputs) > 0 || s.FeedArn != ""
}

// ChannelFollowerSettings holds a follower channel's linked-channel
// configuration. Wire keys "linkedChannelType"/"primaryChannelArn" --
// verified against types.FollowerChannelSettings (request). The response
// shape's primary side (types.DescribePrimaryChannelSettings) additionally
// reports "followingChannelArns", a value MediaLive derives from every
// OTHER channel's follower settings; gopherstack derives it the same way at
// read time (see followingChannelArns in channels.go) rather than storing
// it -- see ChannelPrimarySettings below.
type ChannelFollowerSettings struct {
	LinkedChannelType string
	PrimaryChannelArn string
}

// ChannelPrimarySettings is a primary channel's linked-channel settings.
// FollowingChannelArns is derived (never accepted on a request -- extraction
// never populates it), same pattern as Cluster.ChannelIDs/
// ChannelPlacementGroup.Channels: computed at read time by
// followingChannelArns (channels.go) and stamped onto the returned domain
// object right before Describe/Create/Update/List/Start/Stop hand it back.
type ChannelPrimarySettings struct {
	LinkedChannelType    string
	FollowingChannelArns []string
}

// ChannelLinkedChannelSettings is a channel's linked-channel configuration
// (types.LinkedChannelSettings). Only one of Follower/Primary is normally
// set, matching AWS: a channel is either a follower or a primary, never
// both.
type ChannelLinkedChannelSettings struct {
	Follower ChannelFollowerSettings
	Primary  ChannelPrimarySettings
}

func (s ChannelLinkedChannelSettings) hasLinkedChannelSettings() bool {
	return s.Follower.LinkedChannelType != "" || s.Follower.PrimaryChannelArn != "" ||
		s.Primary.LinkedChannelType != ""
}

// OutputDestinationSetting is one standard-output destination endpoint
// (RTMP/etc). Wire keys "passwordParam"/"streamName"/"url"/"username" --
// verified against types.OutputDestinationSettings.
type OutputDestinationSetting struct {
	PasswordParam string
	StreamName    string
	URL           string
	Username      string
}

// MediaPackageDestinationSettings targets a MediaPackage channel. Wire keys
// "channelEndpointId"/"channelGroup"/"channelId"/"channelName"/
// "mediaPackageRegionName" -- verified against
// types.MediaPackageOutputDestinationSettings.
type MediaPackageDestinationSettings struct {
	ChannelEndpointID      string
	ChannelGroup           string
	ChannelID              string
	ChannelName            string
	MediaPackageRegionName string
}

// MultiplexDestinationSettings targets a Multiplex program. Wire keys
// "multiplexId"/"programName" -- verified against
// types.MultiplexProgramChannelDestinationSettings.
type MultiplexDestinationSettings struct {
	MultiplexID string
	ProgramName string
}

// MediaConnectRouterDestinationSettings targets a MediaConnect Router
// output. Wire keys "encryptionType"/"secretArn" -- verified against
// types.MediaConnectRouterOutputDestinationSettings.
type MediaConnectRouterDestinationSettings struct {
	EncryptionType string
	SecretArn      string
}

// SrtDestinationSettings targets an SRT output. Wire keys
// "connectionMode"/"encryptionPassphraseSecretArn"/"listenerPort"/
// "streamId"/"url" -- verified against types.SrtOutputDestinationSettings.
type SrtDestinationSettings struct {
	ConnectionMode                string
	EncryptionPassphraseSecretArn string
	StreamID                      string
	URL                           string
	ListenerPort                  int32
}

// ChannelOutputDestination is one entry of EncoderSettings' output routing
// table (types.OutputDestination). Modeled in full -- unlike EncoderSettings'
// per-output-group/per-output settings (see OutputGroup below), every
// OutputDestination member is itself a flat, small, non-recursive struct, so
// there is no "genuinely impractical union depth" carve-out needed here.
type ChannelOutputDestination struct {
	ID                         string
	LogicalInterfaceNames      []string
	MediaConnectRouterSettings []MediaConnectRouterDestinationSettings
	MediaPackageSettings       []MediaPackageDestinationSettings
	MultiplexSettings          *MultiplexDestinationSettings
	Settings                   []OutputDestinationSetting
	SrtSettings                []SrtDestinationSettings
}

// AudioSilenceFailoverSettings / InputLossFailoverSettings /
// VideoBlackFailoverSettings are the three named failover-condition variants
// (types.AudioSilenceFailoverSettings/types.InputLossFailoverSettings/
// types.VideoBlackFailoverSettings). Unlike the codec-settings unions
// (dozens of variants, each itself deep), this union has exactly 3 small
// flat variants, so it's modeled in full rather than treated as a gap.
type AudioSilenceFailoverSettings struct {
	AudioSelectorName         string
	AudioSilenceThresholdMsec int32
}

// InputLossFailoverSettings triggers failover after a period with no input
// detected.
type InputLossFailoverSettings struct {
	InputLossThresholdMsec int32
}

// VideoBlackFailoverSettings triggers failover after a period of black
// video.
type VideoBlackFailoverSettings struct {
	BlackDetectThreshold    float64
	VideoBlackThresholdMsec int32
}

// ChannelFailoverConditionSettings is the tagged union of failover-detection
// methods (types.FailoverConditionSettings); at most one variant is set.
type ChannelFailoverConditionSettings struct {
	AudioSilenceSettings *AudioSilenceFailoverSettings
	InputLossSettings    *InputLossFailoverSettings
	VideoBlackSettings   *VideoBlackFailoverSettings
}

// ChannelFailoverCondition wraps one FailoverConditionSettings entry
// (types.FailoverCondition -- a single-field wrapper struct on the real SDK
// too, kept here for wire-shape fidelity rather than flattened away).
type ChannelFailoverCondition struct {
	Settings ChannelFailoverConditionSettings
}

// ChannelAutomaticInputFailoverSettings configures automatic input failover
// for an InputAttachment. Wire keys "secondaryInputId"/"errorClearTimeMsec"/
// "failoverConditions"/"inputPreference" -- verified against
// types.AutomaticInputFailoverSettings.
type ChannelAutomaticInputFailoverSettings struct {
	SecondaryInputID   string
	InputPreference    string
	FailoverConditions []ChannelFailoverCondition
	ErrorClearTimeMsec int32
}

func (s ChannelAutomaticInputFailoverSettings) hasFailover() bool {
	return s.SecondaryInputID != ""
}

// ChannelInputAttachment attaches an Input to a Channel
// (types.InputAttachment). InputSettings (per-attachment audio/caption/video
// selector configuration -- itself a deep union comparable in size to
// EncoderSettings' codec settings) is deliberately NOT modeled; see
// PARITY.md's gaps entry for this family.
type ChannelInputAttachment struct {
	LogicalInterfaceNames          []string
	InputAttachmentName            string
	InputID                        string
	AutomaticInputFailoverSettings ChannelAutomaticInputFailoverSettings
}

// InputLocation is a URI plus optional Parameter-Store-backed credentials,
// reused by several EncoderSettings sub-shapes (avail/blackout slate
// images, input-loss slate). Wire keys "uri"/"passwordParam"/"username" --
// verified against types.InputLocation.
type InputLocation struct {
	URI           string
	PasswordParam string
	Username      string
}

// TimecodeConfig configures how EncoderSettings acquires/adjusts source
// timecodes. Wire keys "source"/"syncThreshold" -- verified against
// types.TimecodeConfig.
type TimecodeConfig struct {
	Source        string
	SyncThreshold int32
}

// AvailBlanking configures ad-avail blanking. Wire keys
// "availBlankingImage"/"state" -- verified against types.AvailBlanking.
type AvailBlanking struct {
	AvailBlankingImage InputLocation
	State              string
}

// BlackoutSlate configures SCTE-104/35 network-blackout behavior. Wire keys
// "blackoutSlateImage"/"networkEndBlackout"/"networkEndBlackoutImage"/
// "networkId"/"state" -- verified against types.BlackoutSlate.
type BlackoutSlate struct {
	BlackoutSlateImage      InputLocation
	NetworkEndBlackoutImage InputLocation
	NetworkEndBlackout      string
	NetworkID               string
	State                   string
}

// FeatureActivations toggles opt-in encoder features. Wire keys
// "inputPrepareScheduleActions"/"outputStaticImageOverlayScheduleActions" --
// verified against types.FeatureActivations.
type FeatureActivations struct {
	InputPrepareScheduleActions             string
	OutputStaticImageOverlayScheduleActions string
}

// InputLossBehavior configures encoder behavior when the input signal is
// lost. Wire keys "blackFrameMsec"/"inputLossImageColor"/
// "inputLossImageSlate"/"inputLossImageType"/"repeatFrameMsec" -- verified
// against types.InputLossBehavior.
type InputLossBehavior struct {
	InputLossImageSlate InputLocation
	InputLossImageColor string
	InputLossImageType  string
	BlackFrameMsec      int32
	RepeatFrameMsec     int32
}

// DisabledLockingSettings / EpochLockingSettings / PipelineLockingSettings
// are the three OutputLockingSettings variants (types.DisabledLockingSettings/
// types.EpochLockingSettings/types.PipelineLockingSettings) -- a 3-way union
// of small flat structs, modeled in full like the failover-condition union
// above.
type DisabledLockingSettings struct {
	CustomEpoch string
}

// EpochLockingSettings locks pipeline output to the Unix epoch (optionally a
// custom one).
type EpochLockingSettings struct {
	CustomEpoch string
	JamSyncTime string
}

// PipelineLockingSettings locks pipeline output to each other.
type PipelineLockingSettings struct {
	CustomEpoch           string
	PipelineLockingMethod string
}

// OutputLockingSettings is the tagged union of pipeline-locking strategies
// (types.OutputLockingSettings); at most one variant is set.
type OutputLockingSettings struct {
	Disabled *DisabledLockingSettings
	Epoch    *EpochLockingSettings
	Pipeline *PipelineLockingSettings
}

// GlobalConfiguration holds event-wide encoder settings. Wire keys
// "initialAudioGain"/"inputEndAction"/"inputLossBehavior"/
// "outputLockingMode"/"outputLockingSettings"/"outputTimingSource"/
// "supportLowFramerateInputs" -- verified against types.GlobalConfiguration.
type GlobalConfiguration struct {
	OutputLockingSettings     OutputLockingSettings
	InputEndAction            string
	OutputLockingMode         string
	OutputTimingSource        string
	SupportLowFramerateInputs string
	InputLossBehavior         InputLossBehavior
	InitialAudioGain          int32
}

// ThumbnailConfiguration enables/disables per-pipeline thumbnail generation.
// Wire key "state" -- verified against types.ThumbnailConfiguration.
type ThumbnailConfiguration struct {
	State string
}

// AudioDescription names one audio encode derived from an input audio
// selector. Wire keys "audioSelectorName"/"audioType"/"audioTypeControl"/
// "languageCode"/"languageCodeControl"/"name"/"streamName" -- verified
// against types.AudioDescription. CodecSettings/AudioNormalizationSettings/
// AudioWatermarkingSettings/RemixSettings/AudioDashRoles/
// DvbDashAccessibility (per-codec and DVB-DASH-accessibility unions) are
// deliberately NOT modeled; see PARITY.md's gaps entry.
type AudioDescription struct {
	Name                string
	AudioSelectorName   string
	LanguageCode        string
	LanguageCodeControl string
	AudioType           string
	AudioTypeControl    string
	StreamName          string
}

// VideoDescription names one video encode. Wire keys "name"/"height"/
// "respondToAfd"/"scalingBehavior"/"sharpness"/"width" -- verified against
// types.VideoDescription. CodecSettings (the H264/H265/AV1/MPEG2/etc union)
// and VideoPreprocessors are deliberately NOT modeled; see PARITY.md's gaps
// entry.
type VideoDescription struct {
	Name            string
	RespondToAfd    string
	ScalingBehavior string
	Height          int32
	Width           int32
	Sharpness       int32
}

// CaptionDescription names one caption output derived from an input caption
// selector. Wire keys "captionSelectorName"/"name"/"accessibility"/
// "dvbDashAccessibility"/"languageCode"/"languageDescription" -- verified
// against types.CaptionDescription. DestinationSettings (the ~15-variant
// per-format union: burn-in/DVB-Sub/SCTE-27/WebVTT/etc) and CaptionDashRoles
// are deliberately NOT modeled; see PARITY.md's gaps entry.
type CaptionDescription struct {
	CaptionSelectorName  string
	Name                 string
	Accessibility        string
	DvbDashAccessibility string
	LanguageCode         string
	LanguageDescription  string
}

// EncoderOutput names one encoder output and the AudioDescription/
// CaptionDescription/VideoDescription names it draws from (types.Output --
// named EncoderOutput here, not Output, to avoid colliding with this
// package's unrelated Output* helper types). Wire keys
// "audioDescriptionNames"/"captionDescriptionNames"/"outputName"/
// "videoDescriptionName" -- verified against types.Output. OutputSettings
// (the per-output-technology union: Archive/CmafIngest/FrameCapture/Hls/
// MediaConnectRouter/MediaPackage/MsSmooth/Multiplex/Rtmp/Srt/
// UdpOutputSettings, each itself large) is deliberately NOT modeled; see
// PARITY.md's gaps entry.
type EncoderOutput struct {
	OutputName              string
	VideoDescriptionName    string
	AudioDescriptionNames   []string
	CaptionDescriptionNames []string
}

// OutputGroup names one output group and its member Outputs. Wire keys
// "name"/"outputs" -- verified against types.OutputGroup. OutputGroupSettings
// (the ~13-variant per-technology union: Archive/CmafIngest/FrameCapture/Hls/
// MediaConnectRouter/MediaPackage/MsSmooth/Multiplex/Rtmp/Srt/
// UdpGroupSettings, each itself dozens of fields) is deliberately NOT
// modeled; see PARITY.md's gaps entry.
type OutputGroup struct {
	Name    string
	Outputs []EncoderOutput
}

// EsamSettings configures ESAM ad-avail signaling to a POIS server. Wire
// keys "acquisitionPointId"/"adAvailOffset"/"passwordParam"/"poisEndpoint"/
// "username"/"zoneIdentity" -- verified against types.Esam.
type EsamSettings struct {
	AcquisitionPointID string
	PoisEndpoint       string
	PasswordParam      string
	Username           string
	ZoneIdentity       string
	AdAvailOffset      int32
}

// Scte35SpliceInsertSettings is the "typical" SCTE-35 avail-insertion mode:
// all segmentation signals create breaks (types.Scte35SpliceInsert). Wire
// keys "adAvailOffset"/"noRegionalBlackoutFlag"/"webDeliveryAllowedFlag".
type Scte35SpliceInsertSettings struct {
	NoRegionalBlackoutFlag string
	WebDeliveryAllowedFlag string
	AdAvailOffset          int32
}

// Scte35TimeSignalAposSettings is the "atypical" SCTE-35 avail-insertion
// mode: only Time Signal Placement Opportunity/Break messages create breaks
// (types.Scte35TimeSignalApos). Same field shape as
// Scte35SpliceInsertSettings but a distinct wire object.
type Scte35TimeSignalAposSettings struct {
	NoRegionalBlackoutFlag string
	WebDeliveryAllowedFlag string
	AdAvailOffset          int32
}

// ChannelAvailSettings is the tagged union of ad-avail signaling methods
// (types.AvailSettings); at most one variant is set.
type ChannelAvailSettings struct {
	Esam                 *EsamSettings
	Scte35SpliceInsert   *Scte35SpliceInsertSettings
	Scte35TimeSignalApos *Scte35TimeSignalAposSettings
}

// ChannelAvailConfiguration configures how EncoderSettings creates SCTE-35
// ad-avail cues (types.AvailConfiguration). Wire keys "availSettings"/
// "scte35SegmentationScope".
type ChannelAvailConfiguration struct {
	AvailSettings           ChannelAvailSettings
	Scte35SegmentationScope string
}

func (a ChannelAvailConfiguration) hasAvailConfiguration() bool {
	return a.Scte35SegmentationScope != "" || a.AvailSettings.Esam != nil ||
		a.AvailSettings.Scte35SpliceInsert != nil || a.AvailSettings.Scte35TimeSignalApos != nil
}

// ChannelColorCorrection is one 3D-LUT color-space conversion entry (types.
// ColorCorrection). Wire keys "inputColorSpace"/"outputColorSpace"/"uri".
type ChannelColorCorrection struct {
	InputColorSpace  string
	OutputColorSpace string
	URI              string
}

// ChannelColorCorrectionSettings configures 3D-LUT-based color conversion
// (types.ColorCorrectionSettings). Wire key "globalColorCorrections".
type ChannelColorCorrectionSettings struct {
	GlobalColorCorrections []ChannelColorCorrection
}

func (s ChannelColorCorrectionSettings) hasColorCorrectionSettings() bool {
	return len(s.GlobalColorCorrections) > 0
}

// ChannelMotionGraphicsSettings is the tagged union of motion-graphics
// sources (types.MotionGraphicsSettings). The real SDK currently defines
// exactly one variant, HtmlMotionGraphicsSettings -- itself an empty marker
// struct on the wire (types.HtmlMotionGraphicsSettings has no fields) -- so
// a bool records "this variant is set" instead of an empty pointer-to-empty
// struct.
type ChannelMotionGraphicsSettings struct {
	HTMLMotionGraphicsSettings bool
}

// ChannelMotionGraphicsConfiguration configures motion-graphics overlay
// insertion (types.MotionGraphicsConfiguration). Wire keys
// "motionGraphicsInsertion"/"motionGraphicsSettings".
type ChannelMotionGraphicsConfiguration struct {
	MotionGraphicsInsertion string
	MotionGraphicsSettings  ChannelMotionGraphicsSettings
}

func (m ChannelMotionGraphicsConfiguration) hasMotionGraphicsConfiguration() bool {
	return m.MotionGraphicsInsertion != "" || m.MotionGraphicsSettings.HTMLMotionGraphicsSettings
}

// ChannelNielsenConfiguration configures Nielsen watermark-to-ID3 tagging
// (types.NielsenConfiguration). Wire keys "distributorId"/
// "nielsenPcmToId3Tagging".
type ChannelNielsenConfiguration struct {
	DistributorID          string
	NielsenPcmToID3Tagging string
}

func (n ChannelNielsenConfiguration) hasNielsenConfiguration() bool {
	return n.DistributorID != "" || n.NielsenPcmToID3Tagging != ""
}

// EncoderSettings is EncoderSettings' modeled subset -- see the per-type doc
// comments above and PARITY.md's gaps entry for exactly what's excluded (the
// per-codec AudioCodecSettings/VideoCodecSettings unions, the
// per-output-technology OutputGroupSettings/OutputSettings unions, and the
// per-caption-format CaptionDestinationSettings union). AvailConfiguration/
// ColorCorrectionSettings/MotionGraphicsConfiguration/NielsenConfiguration
// (gopherstack-sthr) ARE modeled in full below -- unlike the codec/
// output-technology unions, none of these four is itself a large per-format
// union (AvailConfiguration's AvailSettings is only a 3-way union of small
// flat structs, comparable to the failover-condition/output-locking unions
// already modeled above). AudioDescriptions/VideoDescriptions/OutputGroups/
// TimecodeConfig are required on a real CreateChannelInput's EncoderSettings;
// gopherstack accepts a partial value (matching every other
// optional-nested-object family in this service) since it does not perform
// AWS's own request validation.
type EncoderSettings struct {
	BlackoutSlate               BlackoutSlate
	AvailBlanking               AvailBlanking
	AvailConfiguration          ChannelAvailConfiguration
	FeatureActivations          FeatureActivations
	NielsenConfiguration        ChannelNielsenConfiguration
	MotionGraphicsConfiguration ChannelMotionGraphicsConfiguration
	ThumbnailConfiguration      ThumbnailConfiguration
	CaptionDescriptions         []CaptionDescription
	TimecodeConfig              TimecodeConfig
	OutputGroups                []OutputGroup
	ColorCorrectionSettings     ChannelColorCorrectionSettings
	VideoDescriptions           []VideoDescription
	AudioDescriptions           []AudioDescription
	GlobalConfiguration         GlobalConfiguration
}

// hasLegacyEncoderFields covers the EncoderSettings sub-fields modeled
// before gopherstack-sthr (see hasEncoderSettings, split out to stay under
// this repo's cyclomatic-complexity budget).
func (s EncoderSettings) hasLegacyEncoderFields() bool {
	return len(s.AudioDescriptions) > 0 || len(s.VideoDescriptions) > 0 ||
		len(s.CaptionDescriptions) > 0 || len(s.OutputGroups) > 0 ||
		s.TimecodeConfig.Source != "" || s.AvailBlanking.State != "" ||
		s.BlackoutSlate.State != "" ||
		s.FeatureActivations.InputPrepareScheduleActions != "" ||
		s.FeatureActivations.OutputStaticImageOverlayScheduleActions != "" ||
		s.ThumbnailConfiguration.State != "" ||
		s.GlobalConfiguration.InputEndAction != "" || s.GlobalConfiguration.InitialAudioGain != 0 ||
		s.GlobalConfiguration.OutputLockingMode != "" || s.GlobalConfiguration.OutputTimingSource != "" ||
		s.GlobalConfiguration.SupportLowFramerateInputs != ""
}

// hasSthrEncoderFields covers the four EncoderSettings sub-fields added by
// gopherstack-sthr (AvailConfiguration/ColorCorrectionSettings/
// MotionGraphicsConfiguration/NielsenConfiguration).
func (s EncoderSettings) hasSthrEncoderFields() bool {
	return s.AvailConfiguration.hasAvailConfiguration() ||
		s.ColorCorrectionSettings.hasColorCorrectionSettings() ||
		s.MotionGraphicsConfiguration.hasMotionGraphicsConfiguration() ||
		s.NielsenConfiguration.hasNielsenConfiguration()
}

func (s EncoderSettings) hasEncoderSettings() bool {
	return s.hasLegacyEncoderFields() || s.hasSthrEncoderFields()
}

// ChannelCreateExtras bundles the 11 CreateChannelInput members added by
// gopherstack-jb9i beyond name/channelClass/roleArn/tags/anywhereSettings
// (which remain direct CreateChannel parameters, matching the pre-existing
// convention) so CreateChannel's signature doesn't balloon to 15+ positional
// parameters. A zero-valued field means "not configured", matching a real
// CreateChannelInput that omits the corresponding member -- no presence
// tracking is needed for Create, unlike Update (see ChannelUpdateExtras).
type ChannelCreateExtras struct {
	InputSpecification    InputSpecification
	Maintenance           ChannelMaintenance
	ChannelEngineVersion  ChannelEngineVersion
	CdiInputSpecification CdiInputSpecification
	LogLevel              string
	LinkedChannelSettings ChannelLinkedChannelSettings
	Vpc                   ChannelVpcSettings
	InferenceSettings     ChannelInferenceSettings
	ChannelSecurityGroups []string
	Destinations          []ChannelOutputDestination
	InputAttachments      []ChannelInputAttachment
	EncoderSettings       EncoderSettings
}

// ChannelUpdateExtras is ChannelCreateExtras' Update-side counterpart. Each
// field is paired with a HasX flag so UpdateChannel can distinguish "the
// caller omitted this member" (leave unchanged) from "the caller sent an
// explicit, possibly zero, value" (overwrite) -- the same "include this
// parameter only if you want to change it" convention already used by
// UpdateChannel's anywhereSettings and UpdateCluster's networkSettings.
type ChannelUpdateExtras struct {
	Maintenance              ChannelMaintenance
	InputSpecification       InputSpecification
	ChannelEngineVersion     ChannelEngineVersion
	LogLevel                 string
	CdiInputSpecification    CdiInputSpecification
	Vpc                      ChannelVpcSettings
	LinkedChannelSettings    ChannelLinkedChannelSettings
	InferenceSettings        ChannelInferenceSettings
	Destinations             []ChannelOutputDestination
	InputAttachments         []ChannelInputAttachment
	ChannelSecurityGroups    []string
	EncoderSettings          EncoderSettings
	HasChannelSecurityGroups bool
	HasInputAttachments      bool
	HasDestinations          bool
	HasInputSpecification    bool
	HasEncoderSettings       bool
	HasLinkedChannelSettings bool
	HasInferenceSettings     bool
	HasLogLevel              bool
	HasChannelEngineVersion  bool
	HasMaintenance           bool
	HasCdiInputSpecification bool
	HasVpc                   bool
}

// Channel represents a MediaLive channel.
// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type Channel struct {
	Tags                  map[string]string
	Maintenance           ChannelMaintenance
	InputSpecification    InputSpecification
	AnywhereSettings      ChannelAnywhereSettings
	ChannelEngineVersion  ChannelEngineVersion
	CdiInputSpecification CdiInputSpecification
	Name                  string
	LogLevel              string
	RoleARN               string
	State                 string
	ChannelClass          string
	ARN                   string
	ID                    string
	LinkedChannelSettings ChannelLinkedChannelSettings
	Vpc                   ChannelVpcSettings
	InferenceSettings     ChannelInferenceSettings
	InputAttachments      []ChannelInputAttachment
	Destinations          []ChannelOutputDestination
	ChannelSecurityGroups []string
	EncoderSettings       EncoderSettings
}

// ChannelSummary is a channel in a list response. Same shape as Channel
// minus EncoderSettings -- a real ListChannelsOutput/ChannelSummary never
// returns the (potentially huge) encoder configuration, verified against
// types.ChannelSummary.
type ChannelSummary struct {
	ARN                   string
	ID                    string
	Name                  string
	ChannelClass          string
	State                 string
	LogLevel              string
	ChannelSecurityGroups []string
	Destinations          []ChannelOutputDestination
	InputAttachments      []ChannelInputAttachment
	AnywhereSettings      ChannelAnywhereSettings
	CdiInputSpecification CdiInputSpecification
	ChannelEngineVersion  ChannelEngineVersion
	InferenceSettings     ChannelInferenceSettings
	InputSpecification    InputSpecification
	LinkedChannelSettings ChannelLinkedChannelSettings
	Maintenance           ChannelMaintenance
	Vpc                   ChannelVpcSettings
}

// Input represents a MediaLive input.
// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type Input struct {
	Tags      map[string]string
	ARN       string
	ID        string
	Name      string
	InputType string
	RoleARN   string
	State     string
}

// InputSummary is an input in a list response.
type InputSummary struct {
	ARN       string
	ID        string
	Name      string
	InputType string
	State     string
}

// InputSecurityGroup represents a MediaLive input security group.
// Tags first, then strings, then slice: reduces GC pointer scan from 80 to 64 bytes.
type InputSecurityGroup struct {
	Tags           map[string]string
	ARN            string
	ID             string
	State          string
	WhitelistRules []WhitelistRule
}

// InputSecurityGroupSummary is a security group in a list response.
type InputSecurityGroupSummary struct {
	ARN            string
	ID             string
	State          string
	WhitelistRules []WhitelistRule
}

// WhitelistRule is a CIDR-based whitelist entry.
type WhitelistRule struct {
	Cidr string `json:"cidr"`
}

// InputDevice represents a MediaLive input device.
type InputDevice struct {
	Tags                    map[string]string
	ARN                     string
	ID                      string
	Name                    string
	SerialNumber            string
	MacAddress              string
	DeviceType              string
	ConnectionState         string
	DeviceSettingsSyncState string
	DeviceUpdateStatus      string
	MaintenanceWindowActive bool
}

// InputDeviceTransfer represents a pending input device transfer.
type InputDeviceTransfer struct {
	DeviceID         string
	TargetCustomerID string
	TransferType     string
	Message          string
}

// MultiplexSettings holds transport-stream parameters for a Multiplex.
type MultiplexSettings struct {
	TransportStreamBitrate              int
	TransportStreamID                   int
	TransportStreamReservedBitrate      int
	MaximumVideoBufferDelayMilliseconds int
}

// Multiplex represents a MediaLive Multiplex resource.
// Tags first, value struct last: reduces GC pointer scan.
type Multiplex struct {
	Tags              map[string]string
	ARN               string
	ID                string
	Name              string
	State             string
	AvailabilityZones []string
	Settings          MultiplexSettings
	ProgramCount      int
}

// MultiplexSummary is a Multiplex in a list response.
type MultiplexSummary struct {
	ARN               string
	ID                string
	Name              string
	State             string
	AvailabilityZones []string
	ProgramCount      int
}

// ServiceDescriptor holds provider/service name for a program.
type ServiceDescriptor struct {
	ProviderName string
	ServiceName  string
}

// MultiplexProgramSettings holds the settings for a MultiplexProgram.
type MultiplexProgramSettings struct {
	ServiceDescriptor        ServiceDescriptor
	ProgramName              string
	PreferredChannelPipeline string
	ProgramNumber            int
}

// MultiplexProgram represents a program within a Multiplex.
// Strings first, value struct last: reduces GC pointer scan.
type MultiplexProgram struct {
	ChannelID   string
	ProgramName string
	Settings    MultiplexProgramSettings
}

// MultiplexProgramSummary is a program in a list response.
type MultiplexProgramSummary struct {
	ProgramName string
	ChannelID   string
}

// InterfaceMapping logically connects one interface on every Node in a
// Cluster with one Network. Wire keys (networkSettings.interfaceMappings[])
// are lowerCamel "logicalInterfaceName"/"networkId" -- verified against
// aws-sdk-go-v2/service/medialive's types.InterfaceMapping.
type InterfaceMapping struct {
	LogicalInterfaceName string
	NetworkID            string
}

// ClusterNetworkSettings connects the Nodes in a Cluster to one or more of
// the Networks the Cluster is associated with. A real DescribeCluster/
// CreateCluster/UpdateCluster/ListClusters response's "networkSettings" is
// nil/absent until the caller configures it (verified against
// aws-sdk-go-v2/service/medialive's ClusterNetworkSettings type and
// DescribeClusterOutput's deserializer) -- gopherstack tracked NO fields for
// this at all before this fix, silently dropping every caller's
// networkSettings on Create/UpdateCluster.
type ClusterNetworkSettings struct {
	DefaultRoute      string
	InterfaceMappings []InterfaceMapping
}

// hasNetworkSettings reports whether ns has any real content, so callers can
// omit an empty "networkSettings" key the same way a real, never-configured
// Cluster omits it entirely.
func (ns ClusterNetworkSettings) hasNetworkSettings() bool {
	return ns.DefaultRoute != "" || len(ns.InterfaceMappings) > 0
}

// Cluster represents a MediaLive Anywhere Cluster resource.
// Tags first: reduces GC pointer scan.
type Cluster struct {
	Tags            map[string]string
	NetworkSettings ClusterNetworkSettings
	ARN             string
	ID              string
	Name            string
	ClusterType     string
	InstanceRoleArn string
	State           string
	ChannelIDs      []string
}

// ClusterSummary is a Cluster in a list response.
type ClusterSummary struct {
	NetworkSettings ClusterNetworkSettings
	ARN             string
	ID              string
	Name            string
	ClusterType     string
	InstanceRoleArn string
	State           string
	ChannelIDs      []string
}

// Node represents a MediaLive Anywhere Node within a Cluster.
// Tags first: reduces GC pointer scan.
type Node struct {
	Tags                   map[string]string
	ARN                    string
	ID                     string
	Name                   string
	ClusterID              string
	Role                   string
	State                  string
	ConnectionState        string
	ChannelPlacementGroups []string
}

// NodeSummary is a Node in a list response.
type NodeSummary struct {
	ARN                    string
	ID                     string
	Name                   string
	ClusterID              string
	Role                   string
	State                  string
	ConnectionState        string
	ChannelPlacementGroups []string
}

// SignalMap represents a MediaLive signal map resource.
type SignalMap struct {
	CreatedAt                       time.Time
	ModifiedAt                      time.Time
	Tags                            map[string]string
	Arn                             string
	ID                              string
	Name                            string
	Description                     string
	DiscoveryEntryPointArn          string
	Status                          string
	MonitorDeploymentStatus         string
	CloudWatchAlarmTemplateGroupIDs []string
	EventBridgeRuleTemplateGroupIDs []string
}

// CloudWatchAlarmTemplateGroup is a named group for CloudWatch alarm templates.
type CloudWatchAlarmTemplateGroup struct {
	CreatedAt   time.Time
	ModifiedAt  time.Time
	Tags        map[string]string
	Arn         string
	ID          string
	Name        string
	Description string
}

// CloudWatchAlarmTemplateGroupSummary is a CloudWatchAlarmTemplateGroup in a
// list response. The real ListCloudWatchAlarmTemplateGroupsOutput items use
// the CloudWatchAlarmTemplateGroupSummary shape, which has "templateCount"
// -- a field that does NOT exist on Get/Create/Update's response shape
// (verified against aws-sdk-go-v2/service/medialive's
// CloudWatchAlarmTemplateGroupSummary vs CloudWatchAlarmTemplateGroup
// types).
type CloudWatchAlarmTemplateGroupSummary struct {
	CloudWatchAlarmTemplateGroup
	TemplateCount int32
}

// CloudWatchAlarmTemplate is a template for generating CloudWatch alarms.
type CloudWatchAlarmTemplate struct {
	CreatedAt          time.Time
	ModifiedAt         time.Time
	Tags               map[string]string
	Arn                string
	ID                 string
	Name               string
	Description        string
	GroupID            string
	GroupIdentifier    string
	MetricName         string
	Namespace          string
	Statistic          string
	ComparisonOperator string
	TargetResourceType string
	TreatMissingData   string
	Threshold          float64
	EvaluationPeriods  int32
	DatapointsToAlarm  int32
	Period             int32
}

// EventBridgeRuleTemplateGroup is a named group for EventBridge rule templates.
type EventBridgeRuleTemplateGroup struct {
	CreatedAt   time.Time
	ModifiedAt  time.Time
	Tags        map[string]string
	Arn         string
	ID          string
	Name        string
	Description string
}

// EventBridgeRuleTemplateGroupSummary is an EventBridgeRuleTemplateGroup in
// a list response -- same "templateCount only on the List Summary shape"
// nuance as CloudWatchAlarmTemplateGroupSummary (see its doc comment).
type EventBridgeRuleTemplateGroupSummary struct {
	EventBridgeRuleTemplateGroup
	TemplateCount int32
}

// EventBridgeRuleTemplateTarget is a target ARN for an EventBridge rule.
type EventBridgeRuleTemplateTarget struct {
	Arn string `json:"arn"`
}

// EventBridgeRuleTemplate is a template for EventBridge rules.
type EventBridgeRuleTemplate struct {
	CreatedAt       time.Time
	ModifiedAt      time.Time
	Tags            map[string]string
	Arn             string
	ID              string
	Name            string
	Description     string
	GroupID         string
	GroupIdentifier string
	EventType       string
	EventTargets    []EventBridgeRuleTemplateTarget
}

// EventBridgeRuleTemplateSummary is an EventBridgeRuleTemplate in a list
// response. The real ListEventBridgeRuleTemplatesOutput items use the
// EventBridgeRuleTemplateSummary shape, which has "eventTargetCount"
// (an integer) instead of the full "eventTargets" array (verified against
// aws-sdk-go-v2/service/medialive's EventBridgeRuleTemplateSummary vs
// EventBridgeRuleTemplate types).
type EventBridgeRuleTemplateSummary struct {
	EventBridgeRuleTemplate
	EventTargetCount int32
}

// OfferingResourceSpecification describes the resource type for an offering.
type OfferingResourceSpecification struct {
	ResourceType     string `json:"resourceType"`
	VideoQuality     string `json:"videoQuality"`
	Resolution       string `json:"resolution"`
	SpecialFeature   string `json:"specialFeature"`
	MaximumBitrate   string `json:"maximumBitrate"`
	MaximumFramerate string `json:"maximumFramerate"`
	Codec            string `json:"codec"`
}

// Offering is a pre-defined reserved resource listing from the MediaLive catalog.
type Offering struct {
	ResourceSpecification OfferingResourceSpecification
	Arn                   string
	OfferingID            string
	OfferingDescription   string
	OfferingType          string
	CurrencyCode          string
	DurationUnits         string
	Region                string
	FixedPrice            float64
	UsagePrice            float64
	Duration              int32
}

// RenewalSettings holds a Reservation's renewal configuration. Wire keys
// (renewalSettings.automaticRenewal/renewalCount) verified against
// aws-sdk-go-v2/service/medialive's
// awsRestjson1_serializeDocumentRenewalSettings.
type RenewalSettings struct {
	AutomaticRenewal string
	RenewalCount     int32
}

// hasRenewalSettings reports whether rs has any real content, so callers can
// omit an empty "renewalSettings" key.
func (rs RenewalSettings) hasRenewalSettings() bool {
	return rs.AutomaticRenewal != "" || rs.RenewalCount != 0
}

// Reservation is a purchased Offering.
type Reservation struct {
	Tags                  map[string]string
	ResourceSpecification OfferingResourceSpecification
	OfferingType          string
	End                   string
	Start                 string
	Name                  string
	OfferingID            string
	OfferingDescription   string
	DurationUnits         string
	Arn                   string
	ReservationID         string
	CurrencyCode          string
	Region                string
	State                 string
	RenewalSettings       RenewalSettings
	UsagePrice            float64
	FixedPrice            float64
	Duration              int32
	Count                 int32
}

// BatchSuccessfulResult is a successful result in a batch operation.
type BatchSuccessfulResult struct {
	Arn   string
	ID    string
	State string
}

// BatchFailedResult is a failed result in a batch operation.
type BatchFailedResult struct {
	Arn     string
	ID      string
	Code    string
	Message string
}

// BatchResult holds results of a batch start/stop/delete.
type BatchResult struct {
	Successful []BatchSuccessfulResult
	Failed     []BatchFailedResult
}

// ScheduleAction represents a single schedule action for BatchUpdateSchedule.
type ScheduleAction struct {
	ActionName string
	ActionType string
}

// BatchUpdateScheduleResult holds the result of BatchUpdateSchedule.
type BatchUpdateScheduleResult struct {
	Creates []ScheduleAction
	Deletes []ScheduleAction
}

var _ StorageBackend = (*InMemoryBackend)(nil)
