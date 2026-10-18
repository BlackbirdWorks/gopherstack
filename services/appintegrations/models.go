package appintegrations

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// EventFilter selects the partner event source of an event integration.
type EventFilter struct {
	Source string `json:"Source"`
}

// EventIntegration is an AppIntegrations event integration; the JSON names are the wire names.
type EventIntegration struct {
	Tags                map[string]string `json:"Tags,omitempty"`
	Name                string            `json:"Name"`
	EventIntegrationArn string            `json:"EventIntegrationArn"`
	Description         string            `json:"Description,omitempty"`
	EventBridgeBus      string            `json:"EventBridgeBus"`
	EventFilter         EventFilter       `json:"EventFilter"`
}

// EventIntegrationAssociation links an event integration to a client such as a Connect instance.
type EventIntegrationAssociation struct {
	ClientAssociationMetadata      map[string]string `json:"ClientAssociationMetadata,omitempty"`
	ClientID                       string            `json:"ClientId"`
	EventBridgeRuleName            string            `json:"EventBridgeRuleName"`
	EventIntegrationAssociationArn string            `json:"EventIntegrationAssociationArn"`
	EventIntegrationAssociationID  string            `json:"EventIntegrationAssociationId"`
	EventIntegrationName           string            `json:"EventIntegrationName"`
}

// ScheduleConfiguration schedules a data integration.
type ScheduleConfiguration struct {
	FirstExecutionFrom string `json:"FirstExecutionFrom,omitempty"`
	Object             string `json:"Object,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression"`
}

// FileConfiguration selects the files a data integration pulls.
type FileConfiguration struct {
	Filters map[string][]string `json:"Filters,omitempty"`
	Folders []string            `json:"Folders"`
}

// DataIntegration is an AppIntegrations data integration.
type DataIntegration struct {
	ObjectConfiguration   map[string]map[string][]string `json:"ObjectConfiguration,omitempty"`
	Tags                  map[string]string              `json:"Tags,omitempty"`
	FileConfiguration     *FileConfiguration             `json:"FileConfiguration,omitempty"`
	ScheduleConfiguration *ScheduleConfiguration         `json:"ScheduleConfiguration,omitempty"`
	Arn                   string                         `json:"Arn"`
	ID                    string                         `json:"Id"`
	Name                  string                         `json:"Name"`
	Description           string                         `json:"Description,omitempty"`
	KmsKey                string                         `json:"KmsKey"`
	SourceURI             string                         `json:"SourceURI,omitempty"`
}

// OnDemandConfiguration bounds an on-demand execution.
type OnDemandConfiguration struct {
	StartTime string `json:"StartTime"`
	EndTime   string `json:"EndTime,omitempty"`
}

// ExecutionConfiguration says how a data integration association runs.
type ExecutionConfiguration struct {
	OnDemandConfiguration *OnDemandConfiguration `json:"OnDemandConfiguration,omitempty"`
	ScheduleConfiguration *ScheduleConfiguration `json:"ScheduleConfiguration,omitempty"`
	ExecutionMode         string                 `json:"ExecutionMode"`
}

// DataIntegrationAssociation binds a data integration to a client.
type DataIntegrationAssociation struct {
	ClientAssociationMetadata     map[string]string              `json:"ClientAssociationMetadata,omitempty"`
	ObjectConfiguration           map[string]map[string][]string `json:"ObjectConfiguration,omitempty"`
	ExecutionConfiguration        *ExecutionConfiguration        `json:"ExecutionConfiguration,omitempty"`
	ClientID                      string                         `json:"ClientId"`
	DataIntegrationArn            string                         `json:"DataIntegrationArn"`
	DataIntegrationAssociationArn string                         `json:"DataIntegrationAssociationArn"`
	DataIntegrationAssociationID  string                         `json:"DataIntegrationAssociationId"`
	DataIntegrationID             string                         `json:"DataIntegrationId"`
	DestinationURI                string                         `json:"DestinationURI,omitempty"`
}

// ExternalURLConfig locates an application.
type ExternalURLConfig struct {
	AccessURL       string   `json:"AccessUrl"`
	ApprovedOrigins []string `json:"ApprovedOrigins,omitempty"`
}

// ApplicationSourceConfig is where the application is served from.
type ApplicationSourceConfig struct {
	ExternalURLConfig *ExternalURLConfig `json:"ExternalUrlConfig,omitempty"`
}

// ContactHandling scopes the application to contacts.
type ContactHandling struct {
	Scope string `json:"Scope"`
}

// ApplicationConfig holds application behaviour settings.
type ApplicationConfig struct {
	ContactHandling *ContactHandling `json:"ContactHandling,omitempty"`
}

// IframeConfig lists iframe permissions.
type IframeConfig struct {
	Allow   []string `json:"Allow,omitempty"`
	Sandbox []string `json:"Sandbox,omitempty"`
}

// AuthConfig describes how the application authenticates.
type AuthConfig struct {
	AudienceOverride             string `json:"AudienceOverride,omitempty"`
	AuthType                     string `json:"AuthType,omitempty"`
	CredentialProviderIdentifier string `json:"CredentialProviderIdentifier,omitempty"`
}

// Publication is an event an application publishes.
type Publication struct {
	Event       string `json:"Event"`
	Schema      string `json:"Schema"`
	Description string `json:"Description,omitempty"`
}

// Subscription is an event an application consumes.
type Subscription struct {
	Event       string `json:"Event"`
	Description string `json:"Description,omitempty"`
}

// Application is an AppIntegrations application; the JSON names are the wire names.
type Application struct {
	CreatedTime             awstime.EpochTime        `json:"CreatedTime"`
	LastModifiedTime        awstime.EpochTime        `json:"LastModifiedTime"`
	ApplicationSourceConfig *ApplicationSourceConfig `json:"ApplicationSourceConfig,omitempty"`
	ApplicationConfig       *ApplicationConfig       `json:"ApplicationConfig,omitempty"`
	IframeConfig            *IframeConfig            `json:"IframeConfig,omitempty"`
	AuthConfig              *AuthConfig              `json:"AuthConfig,omitempty"`
	InitializationTimeout   *int32                   `json:"InitializationTimeout,omitempty"`
	Tags                    map[string]string        `json:"Tags,omitempty"`
	Arn                     string                   `json:"Arn"`
	ID                      string                   `json:"Id"`
	Name                    string                   `json:"Name"`
	Namespace               string                   `json:"Namespace"`
	Description             string                   `json:"Description,omitempty"`
	ApplicationType         string                   `json:"ApplicationType"`
	Permissions             []string                 `json:"Permissions,omitempty"`
	Publications            []Publication            `json:"Publications,omitempty"`
	Subscriptions           []Subscription           `json:"Subscriptions,omitempty"`
	IsService               bool                     `json:"IsService"`
}

// ApplicationAssociation binds an application to a client such as a Connect instance.
type ApplicationAssociation struct {
	ApplicationArn            string `json:"ApplicationArn"`
	ApplicationAssociationArn string `json:"ApplicationAssociationArn"`
	ApplicationID             string `json:"ApplicationId"`
	ClientID                  string `json:"ClientId"`
	AssociationID             string `json:"AssociationId"`
}

// ApplicationAssociationSummary is the list shape of an ApplicationAssociation.
type ApplicationAssociationSummary struct {
	ApplicationArn            string `json:"ApplicationArn"`
	ApplicationAssociationArn string `json:"ApplicationAssociationArn"`
	ClientID                  string `json:"ClientId"`
}

// DataIntegrationAssociationSummary is the list shape of a DataIntegrationAssociation.
type DataIntegrationAssociationSummary struct {
	ExecutionConfiguration        *ExecutionConfiguration `json:"ExecutionConfiguration,omitempty"`
	ClientID                      string                  `json:"ClientId"`
	DataIntegrationArn            string                  `json:"DataIntegrationArn"`
	DataIntegrationAssociationArn string                  `json:"DataIntegrationAssociationArn"`
	DestinationURI                string                  `json:"DestinationURI,omitempty"`
}

// DataIntegrationSummary is the list shape of a DataIntegration.
type DataIntegrationSummary struct {
	Arn       string `json:"Arn"`
	Name      string `json:"Name"`
	SourceURI string `json:"SourceURI,omitempty"`
}

// ApplicationSummary is the list shape of an Application.
type ApplicationSummary struct {
	CreatedTime      awstime.EpochTime `json:"CreatedTime"`
	LastModifiedTime awstime.EpochTime `json:"LastModifiedTime"`
	ApplicationType  string            `json:"ApplicationType"`
	Arn              string            `json:"Arn"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	Namespace        string            `json:"Namespace"`
	IsService        bool              `json:"IsService"`
}
