package connect

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// Stamp is the LastModified pair most Connect resources carry.
type Stamp struct {
	LastModifiedTime   *awstime.EpochTime `json:"LastModifiedTime,omitempty"`
	LastModifiedRegion string             `json:"LastModifiedRegion,omitempty"`
}

// Instance is an Amazon Connect instance.
type Instance struct {
	CreatedTime            awstime.EpochTime `json:"CreatedTime"`
	Tags                   map[string]string `json:"Tags,omitempty"`
	Arn                    string            `json:"Arn"`
	ID                     string            `json:"Id"`
	IdentityManagementType string            `json:"IdentityManagementType"`
	InstanceAccessURL      string            `json:"InstanceAccessUrl,omitempty"`
	InstanceAlias          string            `json:"InstanceAlias,omitempty"`
	InstanceStatus         string            `json:"InstanceStatus"`
	ServiceRole            string            `json:"ServiceRole,omitempty"`
	InboundCallsEnabled    bool              `json:"InboundCallsEnabled"`
	OutboundCallsEnabled   bool              `json:"OutboundCallsEnabled"`
}

// InstanceSummary is the list shape of an Instance.
type InstanceSummary struct {
	CreatedTime            awstime.EpochTime `json:"CreatedTime"`
	Arn                    string            `json:"Arn"`
	ID                     string            `json:"Id"`
	IdentityManagementType string            `json:"IdentityManagementType"`
	InstanceAccessURL      string            `json:"InstanceAccessUrl,omitempty"`
	InstanceAlias          string            `json:"InstanceAlias,omitempty"`
	InstanceStatus         string            `json:"InstanceStatus"`
	ServiceRole            string            `json:"ServiceRole,omitempty"`
	InboundCallsEnabled    bool              `json:"InboundCallsEnabled"`
	OutboundCallsEnabled   bool              `json:"OutboundCallsEnabled"`
}

// Attribute is one instance attribute.
type Attribute struct {
	AttributeType string `json:"AttributeType"`
	Value         string `json:"Value"`
}

// EncryptionConfig names the KMS key protecting stored data.
type EncryptionConfig struct {
	EncryptionType string `json:"EncryptionType"`
	KeyID          string `json:"KeyId"`
}

// S3Config is an S3 storage target.
type S3Config struct {
	EncryptionConfig *EncryptionConfig `json:"EncryptionConfig,omitempty"`
	BucketName       string            `json:"BucketName"`
	BucketPrefix     string            `json:"BucketPrefix"`
}

// KinesisVideoStreamConfig is a Kinesis Video Streams storage target.
type KinesisVideoStreamConfig struct {
	EncryptionConfig     *EncryptionConfig `json:"EncryptionConfig,omitempty"`
	Prefix               string            `json:"Prefix"`
	RetentionPeriodHours int32             `json:"RetentionPeriodHours"`
}

// KinesisStreamConfig is a Kinesis Data Streams storage target.
type KinesisStreamConfig struct {
	StreamArn string `json:"StreamArn"`
}

// KinesisFirehoseConfig is a Firehose storage target.
type KinesisFirehoseConfig struct {
	FirehoseArn string `json:"FirehoseArn"`
}

// InstanceStorageConfig is where one kind of instance data is stored.
type InstanceStorageConfig struct {
	S3Config                 *S3Config                 `json:"S3Config,omitempty"`
	KinesisVideoStreamConfig *KinesisVideoStreamConfig `json:"KinesisVideoStreamConfig,omitempty"`
	KinesisStreamConfig      *KinesisStreamConfig      `json:"KinesisStreamConfig,omitempty"`
	KinesisFirehoseConfig    *KinesisFirehoseConfig    `json:"KinesisFirehoseConfig,omitempty"`
	AssociationID            string                    `json:"AssociationId"`
	StorageType              string                    `json:"StorageType"`
}

// StorageAssociation is a storage config together with its resource type.
type StorageAssociation struct {
	InstanceStorageConfig
	ResourceType string `json:"ResourceType"`
}

// LexBot is a Lex V1 bot reference.
type LexBot struct {
	LexRegion string `json:"LexRegion"`
	Name      string `json:"Name"`
}

// LexV2Bot is a Lex V2 bot alias reference.
type LexV2Bot struct {
	AliasArn string `json:"AliasArn"`
}

// LexBotConfig is a Lex V1 or V2 bot association.
type LexBotConfig struct {
	LexBot   *LexBot   `json:"LexBot,omitempty"`
	LexV2Bot *LexV2Bot `json:"LexV2Bot,omitempty"`
}

// SecurityKey is a public key an instance trusts.
type SecurityKey struct {
	CreationTime  awstime.EpochTime `json:"CreationTime"`
	AssociationID string            `json:"AssociationId"`
	Key           string            `json:"Key"`
}

// IntegrationAssociation links an instance to an integration such as an AppIntegrations event.
type IntegrationAssociation struct {
	InstanceID                string `json:"InstanceId"`
	IntegrationArn            string `json:"IntegrationArn"`
	IntegrationAssociationArn string `json:"IntegrationAssociationArn"`
	IntegrationAssociationID  string `json:"IntegrationAssociationId"`
	IntegrationType           string `json:"IntegrationType"`
	SourceApplicationName     string `json:"SourceApplicationName,omitempty"`
	SourceApplicationURL      string `json:"SourceApplicationUrl,omitempty"`
	SourceType                string `json:"SourceType,omitempty"`
}

// UseCase is a use case of an integration association.
type UseCase struct {
	UseCaseArn  string `json:"UseCaseArn"`
	UseCaseID   string `json:"UseCaseId"`
	UseCaseType string `json:"UseCaseType"`
}

// UserIdentityInfo is a user's name and email.
type UserIdentityInfo struct {
	Email          string `json:"Email,omitempty"`
	FirstName      string `json:"FirstName,omitempty"`
	LastName       string `json:"LastName,omitempty"`
	Mobile         string `json:"Mobile,omitempty"`
	SecondaryEmail string `json:"SecondaryEmail,omitempty"`
}

// UserPhoneConfig is how a user takes calls.
type UserPhoneConfig struct {
	PersistentConnection      *bool  `json:"PersistentConnection,omitempty"`
	DeskPhoneNumber           string `json:"DeskPhoneNumber,omitempty"`
	PhoneType                 string `json:"PhoneType"`
	AfterContactWorkTimeLimit int32  `json:"AfterContactWorkTimeLimit"`
	AutoAccept                bool   `json:"AutoAccept"`
}

// User is an Amazon Connect agent or administrator.
type User struct {
	Stamp
	IdentityInfo                *UserIdentityInfo `json:"IdentityInfo,omitempty"`
	PhoneConfig                 *UserPhoneConfig  `json:"PhoneConfig,omitempty"`
	Tags                        map[string]string `json:"Tags,omitempty"`
	Arn                         string            `json:"Arn"`
	DirectoryUserID             string            `json:"DirectoryUserId,omitempty"`
	HierarchyGroupID            string            `json:"HierarchyGroupId,omitempty"`
	ID                          string            `json:"Id"`
	RoutingProfileID            string            `json:"RoutingProfileId"`
	Username                    string            `json:"Username"`
	SecurityProfileIDs          []string          `json:"SecurityProfileIds"`
	AfterContactWorkConfigs     json.RawMessage   `json:"AfterContactWorkConfigs,omitempty"`
	AutoAcceptConfigs           json.RawMessage   `json:"AutoAcceptConfigs,omitempty"`
	PersistentConnectionConfigs json.RawMessage   `json:"PersistentConnectionConfigs,omitempty"`
	PhoneNumberConfigs          json.RawMessage   `json:"PhoneNumberConfigs,omitempty"`
	VoiceEnhancementConfigs     json.RawMessage   `json:"VoiceEnhancementConfigs,omitempty"`
}

// Summary is the list shape shared by most resources.
type Summary struct {
	Stamp
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// UserSummary is the list shape of a User.
type UserSummary struct {
	Stamp
	Arn      string `json:"Arn"`
	ID       string `json:"Id"`
	Username string `json:"Username"`
}

// MediaConcurrency limits concurrent contacts of one channel.
type MediaConcurrency struct {
	Channel                   string          `json:"Channel"`
	CrossChannelBehavior      json.RawMessage `json:"CrossChannelBehavior,omitempty"`
	WorkloadTypeConcurrencies json.RawMessage `json:"WorkloadTypeConcurrencies,omitempty"`
	Concurrency               int32           `json:"Concurrency"`
}

// QueueReference names a queue and channel in a routing profile.
type QueueReference struct {
	QueueID string `json:"QueueId"`
	Channel string `json:"Channel"`
}

// RoutingProfileQueueConfig places a queue in a routing profile.
type RoutingProfileQueueConfig struct {
	QueueReference QueueReference `json:"QueueReference"`
	Delay          int32          `json:"Delay"`
	Priority       int32          `json:"Priority"`
}

// RoutingProfileQueueConfigSummary is the list shape of a RoutingProfileQueueConfig.
type RoutingProfileQueueConfigSummary struct {
	QueueArn  string `json:"QueueArn"`
	QueueID   string `json:"QueueId"`
	QueueName string `json:"QueueName"`
	Channel   string `json:"Channel"`
	Delay     int32  `json:"Delay"`
	Priority  int32  `json:"Priority"`
}

// RoutingProfile routes contacts to agents.
type RoutingProfile struct {
	Stamp
	Tags                                     map[string]string  `json:"Tags,omitempty"`
	AgentAvailabilityTimer                   string             `json:"AgentAvailabilityTimer,omitempty"`
	DefaultOutboundQueueID                   string             `json:"DefaultOutboundQueueId"`
	Description                              string             `json:"Description,omitempty"`
	InstanceID                               string             `json:"InstanceId"`
	Name                                     string             `json:"Name"`
	RoutingProfileArn                        string             `json:"RoutingProfileArn"`
	RoutingProfileID                         string             `json:"RoutingProfileId"`
	MediaConcurrencies                       []MediaConcurrency `json:"MediaConcurrencies"`
	AssociatedQueueIDs                       []string           `json:"AssociatedQueueIds,omitempty"`
	AssociatedManualAssignmentQueueIDs       []string           `json:"AssociatedManualAssignmentQueueIds,omitempty"`
	NumberOfAssociatedQueues                 int64              `json:"NumberOfAssociatedQueues"`
	NumberOfAssociatedUsers                  int64              `json:"NumberOfAssociatedUsers"`
	NumberOfAssociatedManualAssignmentQueues int64              `json:"NumberOfAssociatedManualAssignmentQueues"`
	IsDefault                                bool               `json:"IsDefault"`
}

// SecurityProfile grants permissions to users.
type SecurityProfile struct {
	Stamp
	AllowedAccessControlTags             map[string]string `json:"AllowedAccessControlTags,omitempty"`
	Tags                                 map[string]string `json:"Tags,omitempty"`
	GranularAccessControlConfiguration   json.RawMessage   `json:"GranularAccessControlConfiguration,omitempty"`
	AllowedAccessControlHierarchyGroupID string            `json:"AllowedAccessControlHierarchyGroupId,omitempty"`
	Arn                                  string            `json:"Arn"`
	Description                          string            `json:"Description,omitempty"`
	ID                                   string            `json:"Id"`
	SecurityProfileName                  string            `json:"SecurityProfileName"`
	HierarchyRestrictedResources         []string          `json:"HierarchyRestrictedResources,omitempty"`
	TagRestrictedResources               []string          `json:"TagRestrictedResources,omitempty"`
}

// HierarchyGroupSummary names a hierarchy group.
type HierarchyGroupSummary struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// HierarchyPath is the chain of groups above (and including) a group.
type HierarchyPath struct {
	LevelOne   *HierarchyGroupSummary `json:"LevelOne,omitempty"`
	LevelTwo   *HierarchyGroupSummary `json:"LevelTwo,omitempty"`
	LevelThree *HierarchyGroupSummary `json:"LevelThree,omitempty"`
	LevelFour  *HierarchyGroupSummary `json:"LevelFour,omitempty"`
	LevelFive  *HierarchyGroupSummary `json:"LevelFive,omitempty"`
}

// HierarchyGroup is a node of the agent hierarchy.
type HierarchyGroup struct {
	Stamp
	HierarchyPath *HierarchyPath    `json:"HierarchyPath,omitempty"`
	Tags          map[string]string `json:"Tags,omitempty"`
	Arn           string            `json:"Arn"`
	ID            string            `json:"Id"`
	LevelID       string            `json:"LevelId"`
	Name          string            `json:"Name"`
}

// HierarchyLevel is one named level of the hierarchy structure.
type HierarchyLevel struct {
	Stamp
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// HierarchyStructure names the levels of the agent hierarchy.
type HierarchyStructure struct {
	LevelOne   *HierarchyLevel `json:"LevelOne,omitempty"`
	LevelTwo   *HierarchyLevel `json:"LevelTwo,omitempty"`
	LevelThree *HierarchyLevel `json:"LevelThree,omitempty"`
	LevelFour  *HierarchyLevel `json:"LevelFour,omitempty"`
	LevelFive  *HierarchyLevel `json:"LevelFive,omitempty"`
}

// OutboundCallerConfig sets caller ID for outbound calls from a queue.
type OutboundCallerConfig struct {
	OutboundCallerIDName     string `json:"OutboundCallerIdName,omitempty"`
	OutboundCallerIDNumberID string `json:"OutboundCallerIdNumberId,omitempty"`
	OutboundFlowID           string `json:"OutboundFlowId,omitempty"`
}

// OutboundEmailConfig sets the from address of outbound email from a queue.
type OutboundEmailConfig struct {
	OutboundEmailAddressID string `json:"OutboundEmailAddressId,omitempty"`
}

// Queue holds contacts waiting for agents.
type Queue struct {
	Stamp
	OutboundCallerConfig *OutboundCallerConfig `json:"OutboundCallerConfig,omitempty"`
	OutboundEmailConfig  *OutboundEmailConfig  `json:"OutboundEmailConfig,omitempty"`
	MaxContacts          *int32                `json:"MaxContacts,omitempty"`
	Tags                 map[string]string     `json:"Tags,omitempty"`
	Description          string                `json:"Description,omitempty"`
	HoursOfOperationID   string                `json:"HoursOfOperationId"`
	Name                 string                `json:"Name"`
	QueueArn             string                `json:"QueueArn"`
	QueueID              string                `json:"QueueId"`
	Status               string                `json:"Status"`
}

// QueueSummary is the list shape of a Queue.
type QueueSummary struct {
	Stamp
	Arn       string `json:"Arn"`
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	QueueType string `json:"QueueType"`
}

// QuickConnectSummary is the list shape of a QuickConnect.
type QuickConnectSummary struct {
	Stamp
	Arn              string `json:"Arn"`
	ID               string `json:"Id"`
	Name             string `json:"Name"`
	QuickConnectType string `json:"QuickConnectType"`
}

// TimeSlice is an hour and minute of day.
type TimeSlice struct {
	Hours   int32 `json:"Hours"`
	Minutes int32 `json:"Minutes"`
}

// HoursOfOperationConfig is the open window of one day.
type HoursOfOperationConfig struct {
	Day       string    `json:"Day"`
	EndTime   TimeSlice `json:"EndTime"`
	StartTime TimeSlice `json:"StartTime"`
}

// HoursOfOperation is a weekly schedule.
type HoursOfOperation struct {
	Stamp
	Tags                map[string]string        `json:"Tags,omitempty"`
	Description         string                   `json:"Description,omitempty"`
	HoursOfOperationArn string                   `json:"HoursOfOperationArn"`
	HoursOfOperationID  string                   `json:"HoursOfOperationId"`
	Name                string                   `json:"Name"`
	TimeZone            string                   `json:"TimeZone"`
	Config              []HoursOfOperationConfig `json:"Config"`

	ParentHoursOfOperations []HoursIdentifier `json:"ParentHoursOfOperations,omitempty"`
}

// QuickConnectConfig is what a quick connect transfers to.
type QuickConnectConfig struct {
	PhoneConfig *struct {
		PhoneNumber string `json:"PhoneNumber"`
	} `json:"PhoneConfig,omitempty"`
	QueueConfig *struct {
		ContactFlowID string `json:"ContactFlowId"`
		QueueID       string `json:"QueueId"`
	} `json:"QueueConfig,omitempty"`
	UserConfig *struct {
		ContactFlowID string `json:"ContactFlowId"`
		UserID        string `json:"UserId"`
	} `json:"UserConfig,omitempty"`
	QuickConnectType string          `json:"QuickConnectType"`
	FlowConfig       json.RawMessage `json:"FlowConfig,omitempty"`
}

// QuickConnect is a saved transfer destination.
type QuickConnect struct {
	Tags map[string]string `json:"Tags,omitempty"`
	Stamp
	Description        string             `json:"Description,omitempty"`
	Name               string             `json:"Name"`
	QuickConnectARN    string             `json:"QuickConnectARN"`
	QuickConnectID     string             `json:"QuickConnectId"`
	QuickConnectConfig QuickConnectConfig `json:"QuickConnectConfig"`
}

// ContactFlow is a contact flow.
type ContactFlow struct {
	Stamp
	Version            *int64            `json:"Version,omitempty"`
	Tags               map[string]string `json:"Tags,omitempty"`
	Arn                string            `json:"Arn"`
	Content            string            `json:"Content"`
	Description        string            `json:"Description,omitempty"`
	FlowContentSha256  string            `json:"FlowContentSha256,omitempty"`
	ID                 string            `json:"Id"`
	Name               string            `json:"Name"`
	State              string            `json:"State"`
	Status             string            `json:"Status"`
	Type               string            `json:"Type"`
	VersionDescription string            `json:"VersionDescription,omitempty"`
}

// ContactFlowSummary is the list shape of a ContactFlow.
type ContactFlowSummary struct {
	Arn               string `json:"Arn"`
	ContactFlowState  string `json:"ContactFlowState"`
	ContactFlowStatus string `json:"ContactFlowStatus"`
	ContactFlowType   string `json:"ContactFlowType"`
	ID                string `json:"Id"`
	Name              string `json:"Name"`
}

// ContactFlowModule is a reusable flow fragment.
type ContactFlowModule struct {
	Version                         *int64            `json:"Version,omitempty"`
	Tags                            map[string]string `json:"Tags,omitempty"`
	FlowModuleContentSha256         string            `json:"FlowModuleContentSha256,omitempty"`
	Arn                             string            `json:"Arn"`
	Content                         string            `json:"Content"`
	Description                     string            `json:"Description,omitempty"`
	ID                              string            `json:"Id"`
	Name                            string            `json:"Name"`
	Settings                        string            `json:"Settings,omitempty"`
	State                           string            `json:"State"`
	Status                          string            `json:"Status"`
	VersionDescription              string            `json:"VersionDescription,omitempty"`
	ExternalInvocationConfiguration json.RawMessage   `json:"ExternalInvocationConfiguration,omitempty"`
}

// ContactFlowModuleSummary is the list shape of a ContactFlowModule.
type ContactFlowModuleSummary struct {
	Arn   string `json:"Arn"`
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	State string `json:"State"`
}

// PhoneNumberStatus is the claim workflow state of a phone number.
type PhoneNumberStatus struct {
	Message string `json:"Message,omitempty"`
	Status  string `json:"Status"`
}

// ClaimedPhoneNumberSummary is a claimed phone number.
type ClaimedPhoneNumberSummary struct {
	PhoneNumberStatus      *PhoneNumberStatus `json:"PhoneNumberStatus,omitempty"`
	Tags                   map[string]string  `json:"Tags,omitempty"`
	InstanceID             string             `json:"InstanceId,omitempty"`
	PhoneNumber            string             `json:"PhoneNumber"`
	PhoneNumberArn         string             `json:"PhoneNumberArn"`
	PhoneNumberCountryCode string             `json:"PhoneNumberCountryCode"`
	PhoneNumberDescription string             `json:"PhoneNumberDescription,omitempty"`
	PhoneNumberID          string             `json:"PhoneNumberId"`
	PhoneNumberType        string             `json:"PhoneNumberType"`
	TargetArn              string             `json:"TargetArn,omitempty"`
}

// PhoneNumberSummary is the instance-scoped list shape of a phone number.
type PhoneNumberSummary struct {
	Arn                    string `json:"Arn"`
	ID                     string `json:"Id"`
	PhoneNumber            string `json:"PhoneNumber"`
	PhoneNumberCountryCode string `json:"PhoneNumberCountryCode"`
	PhoneNumberType        string `json:"PhoneNumberType"`
}

// ListPhoneNumbersSummary is the V2 list shape of a phone number.
type ListPhoneNumbersSummary struct {
	InstanceID             string `json:"InstanceId,omitempty"`
	PhoneNumber            string `json:"PhoneNumber"`
	PhoneNumberArn         string `json:"PhoneNumberArn"`
	PhoneNumberCountryCode string `json:"PhoneNumberCountryCode"`
	PhoneNumberDescription string `json:"PhoneNumberDescription,omitempty"`
	PhoneNumberID          string `json:"PhoneNumberId"`
	PhoneNumberType        string `json:"PhoneNumberType"`
	TargetArn              string `json:"TargetArn,omitempty"`
}

// AvailableNumberSummary is an unclaimed number of the pool.
type AvailableNumberSummary struct {
	PhoneNumber            string `json:"PhoneNumber"`
	PhoneNumberCountryCode string `json:"PhoneNumberCountryCode"`
	PhoneNumberType        string `json:"PhoneNumberType"`
}

// AgentStatus is a status an agent can set.
type AgentStatus struct {
	Stamp
	DisplayOrder   *int32            `json:"DisplayOrder,omitempty"`
	Tags           map[string]string `json:"Tags,omitempty"`
	AgentStatusARN string            `json:"AgentStatusARN"`
	AgentStatusID  string            `json:"AgentStatusId"`
	Description    string            `json:"Description,omitempty"`
	Name           string            `json:"Name"`
	State          string            `json:"State"`
	Type           string            `json:"Type"`
}

// AgentStatusSummary is the list shape of an AgentStatus.
type AgentStatusSummary struct {
	Stamp
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
	Type string `json:"Type"`
}

// Prompt is an audio prompt.
type Prompt struct {
	Stamp
	Tags        map[string]string `json:"Tags,omitempty"`
	Description string            `json:"Description,omitempty"`
	Name        string            `json:"Name"`
	PromptARN   string            `json:"PromptARN"`
	PromptID    string            `json:"PromptId"`
}

// EndpointInfo is a customer or system endpoint of a contact.
type EndpointInfo struct {
	Address string `json:"Address,omitempty"`
	Type    string `json:"Type,omitempty"`
}

// ContactQueueInfo records when a contact entered a queue.
type ContactQueueInfo struct {
	EnqueueTimestamp *awstime.EpochTime `json:"EnqueueTimestamp,omitempty"`
	ID               string             `json:"Id,omitempty"`
}

// ContactAgentInfo records the agent a contact reached.
type ContactAgentInfo struct {
	ConnectedToAgentTimestamp *awstime.EpochTime `json:"ConnectedToAgentTimestamp,omitempty"`
	ID                        string             `json:"Id,omitempty"`
}

// Campaign links a contact to an outbound campaign.
type Campaign struct {
	CampaignID string `json:"CampaignId,omitempty"`
}

// Contact is a customer interaction.
type Contact struct {
	InitiationTimestamp *awstime.EpochTime `json:"InitiationTimestamp,omitempty"`
	LastUpdateTimestamp *awstime.EpochTime `json:"LastUpdateTimestamp,omitempty"`
	DisconnectTimestamp *awstime.EpochTime `json:"DisconnectTimestamp,omitempty"`
	ScheduledTimestamp  *awstime.EpochTime `json:"ScheduledTimestamp,omitempty"`
	RingStartTimestamp  *awstime.EpochTime `json:"RingStartTimestamp,omitempty"`
	QueueInfo           *ContactQueueInfo  `json:"QueueInfo,omitempty"`
	AgentInfo           *ContactAgentInfo  `json:"AgentInfo,omitempty"`
	CustomerEndpoint    *EndpointInfo      `json:"CustomerEndpoint,omitempty"`
	SystemEndpoint      *EndpointInfo      `json:"SystemEndpoint,omitempty"`
	Campaign            *Campaign          `json:"Campaign,omitempty"`
	Attributes          map[string]string  `json:"Attributes,omitempty"`
	Tags                map[string]string  `json:"Tags,omitempty"`
	DisconnectReason    string             `json:"DisconnectReason,omitempty"`
	Arn                 string             `json:"Arn"`
	Channel             string             `json:"Channel"`
	CustomerID          string             `json:"CustomerId,omitempty"`
	Description         string             `json:"Description,omitempty"`
	ID                  string             `json:"Id"`
	InitialContactID    string             `json:"InitialContactId"`
	InitiationMethod    string             `json:"InitiationMethod"`
	Name                string             `json:"Name,omitempty"`
	PreviousContactID   string             `json:"PreviousContactId,omitempty"`
	RelatedContactID    string             `json:"RelatedContactId,omitempty"`
	OutboundStrategy    json.RawMessage    `json:"OutboundStrategy,omitempty"`
	SegmentAttributes   json.RawMessage    `json:"SegmentAttributes,omitempty"`
}
