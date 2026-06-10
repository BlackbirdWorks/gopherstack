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

var _ StorageBackend = (*InMemoryBackend)(nil)
