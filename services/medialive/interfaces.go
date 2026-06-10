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

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
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
	ARN   string
	ID    string
	State string
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

var _ StorageBackend = (*InMemoryBackend)(nil)
