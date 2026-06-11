package medialive

// StorageBackend is the interface for MediaLive storage operations.
type StorageBackend interface {
	// Channels
	CreateChannel(name, channelClass, roleArn string, tags map[string]string) (*Channel, error)
	DescribeChannel(channelID string) (*Channel, error)
	UpdateChannel(channelID, name, roleArn string) (*Channel, error)
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
	CreateCluster(name, clusterType, instanceRoleArn string, tags map[string]string) (*Cluster, error)
	DescribeCluster(clusterID string) (*Cluster, error)
	UpdateCluster(clusterID, name string) (*Cluster, error)
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
	ListClusterAlerts(clusterID string, maxResults int, nextToken string) ([]map[string]any, string, error)

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
	) ([]*CloudWatchAlarmTemplateGroup, string, error)
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
	) ([]*EventBridgeRuleTemplateGroup, string, error)
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
	) ([]*EventBridgeRuleTemplate, string, error)
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
		tags map[string]string,
	) (*Reservation, error)
	ListReservations(maxResults int, nextToken string) ([]*Reservation, string, error)
	DescribeReservation(reservationID string) (*Reservation, error)
	DeleteReservation(reservationID string) (*Reservation, error)
	UpdateReservation(reservationID, name string) (*Reservation, error)

	// Batch ops
	BatchStart(channelIDs, inputIDs, multiplexIDs []string) (*BatchResult, error)
	BatchStop(channelIDs, inputIDs, multiplexIDs []string) (*BatchResult, error)
	BatchDelete(channelIDs, inputIDs, multiplexIDs []string) (*BatchResult, error)
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
	Snapshot() []byte
	Restore(data []byte) error
}

// IPPool is a CIDR pool for a Network.
type IPPool struct {
	Cidr string `json:"Cidr"`
}

// Route is a static route for a Network.
type Route struct {
	Cidr    string `json:"Cidr"`
	Gateway string `json:"Gateway"`
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

// Channel represents a MediaLive channel.
// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type Channel struct {
	Tags         map[string]string
	ARN          string
	ID           string
	Name         string
	ChannelClass string
	RoleARN      string
	State        string
}

// ChannelSummary is a channel in a list response.
type ChannelSummary struct {
	ARN          string
	ID           string
	Name         string
	ChannelClass string
	State        string
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
}

// MultiplexSummary is a Multiplex in a list response.
type MultiplexSummary struct {
	ARN               string
	ID                string
	Name              string
	State             string
	AvailabilityZones []string
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

// Cluster represents a MediaLive Anywhere Cluster resource.
// Tags first: reduces GC pointer scan.
type Cluster struct {
	Tags            map[string]string
	ARN             string
	ID              string
	Name            string
	ClusterType     string
	InstanceRoleArn string
	State           string
}

// ClusterSummary is a Cluster in a list response.
type ClusterSummary struct {
	ARN             string
	ID              string
	Name            string
	ClusterType     string
	InstanceRoleArn string
	State           string
}

// Node represents a MediaLive Anywhere Node within a Cluster.
// Tags first: reduces GC pointer scan.
type Node struct {
	Tags            map[string]string
	ARN             string
	ID              string
	Name            string
	ClusterID       string
	Role            string
	State           string
	ConnectionState string
}

// NodeSummary is a Node in a list response.
type NodeSummary struct {
	ARN             string
	ID              string
	Name            string
	ClusterID       string
	Role            string
	State           string
	ConnectionState string
}

// SignalMap represents a MediaLive signal map resource.
type SignalMap struct {
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
	Tags        map[string]string
	Arn         string
	ID          string
	Name        string
	Description string
}

// CloudWatchAlarmTemplate is a template for generating CloudWatch alarms.
type CloudWatchAlarmTemplate struct {
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
	Tags        map[string]string
	Arn         string
	ID          string
	Name        string
	Description string
}

// EventBridgeRuleTemplateTarget is a target ARN for an EventBridge rule.
type EventBridgeRuleTemplateTarget struct {
	Arn string `json:"arn"`
}

// EventBridgeRuleTemplate is a template for EventBridge rules.
type EventBridgeRuleTemplate struct {
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
	FixedPrice            float64
	UsagePrice            float64
	Duration              int32
}

// Reservation is a purchased Offering.
type Reservation struct {
	Tags                  map[string]string
	ResourceSpecification OfferingResourceSpecification
	CurrencyCode          string
	Start                 string
	Name                  string
	OfferingID            string
	OfferingDescription   string
	OfferingType          string
	Arn                   string
	ReservationID         string
	End                   string
	Region                string
	State                 string
	DurationUnits         string
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
	Arn  string
	ID   string
	Code string
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
