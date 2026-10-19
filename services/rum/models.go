package rum

import "time"

const (
	platformWeb     = "Web"
	platformAndroid = "Android"
	platformIOS     = "iOS"

	stateCreated = "CREATED"
	stateActive  = "ACTIVE"

	statusEnabled  = "ENABLED"
	statusDisabled = "DISABLED"

	destCloudWatch = "CloudWatch"
	destEvidently  = "Evidently"

	defaultSampleRate = 0.1
	logGroupPrefix    = "/aws/vendedlogs/RUMService_"
	logGroupIDChars   = 8
)

// MonitorConfig is the AppMonitorConfiguration structure.
type MonitorConfig struct {
	GuestRoleArn      *string  `json:"GuestRoleArn,omitempty"`
	IdentityPoolID    *string  `json:"IdentityPoolId,omitempty"`
	AllowCookies      *bool    `json:"AllowCookies,omitempty"`
	EnableXRay        *bool    `json:"EnableXRay,omitempty"`
	ExcludedPages     []string `json:"ExcludedPages,omitempty"`
	FavoritePages     []string `json:"FavoritePages,omitempty"`
	IncludedPages     []string `json:"IncludedPages,omitempty"`
	Telemetries       []string `json:"Telemetries,omitempty"`
	SessionSampleRate float64  `json:"SessionSampleRate"`
}

// SourceMaps is the JavaScript source map deobfuscation setting.
type SourceMaps struct {
	Status string `json:"Status"`
	S3URI  string `json:"S3Uri,omitempty"`
}

// AppMonitor is a stored CloudWatch RUM app monitor.
type AppMonitor struct {
	Created        time.Time         `json:"created"`
	LastModified   time.Time         `json:"lastModified"`
	Tags           map[string]string `json:"tags"`
	Config         *MonitorConfig    `json:"config,omitempty"`
	SourceMaps     *SourceMaps       `json:"sourceMaps,omitempty"`
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Account        string            `json:"account"`
	Region         string            `json:"region"`
	Platform       string            `json:"platform"`
	Domain         string            `json:"domain,omitempty"`
	CustomEvents   string            `json:"customEvents"`
	Policy         string            `json:"policy,omitempty"`
	PolicyRevision string            `json:"policyRevision,omitempty"`
	DomainList     []string          `json:"domainList,omitempty"`
	CwLogEnabled   bool              `json:"cwLogEnabled"`
	Active         bool              `json:"active"`
}

// Event is one stored RUM telemetry event.
type Event struct {
	Key       string  `json:"key"`
	AppID     string  `json:"appId"`
	Raw       string  `json:"raw"`
	Type      string  `json:"type"`
	Browser   string  `json:"browser,omitempty"`
	Device    string  `json:"device,omitempty"`
	OS        string  `json:"os,omitempty"`
	Page      string  `json:"page,omitempty"`
	Country   string  `json:"country,omitempty"`
	Timestamp float64 `json:"timestamp"`
	Seq       int64   `json:"seq"`
}

// Destination is a metrics destination of one app monitor.
type Destination struct {
	AppID          string `json:"appId"`
	Destination    string `json:"destination"`
	DestinationARN string `json:"destinationArn,omitempty"`
	IAMRoleARN     string `json:"iamRoleArn,omitempty"`
}

// MetricDef is an extended or custom metric definition.
type MetricDef struct {
	DimensionKeys  map[string]string `json:"dimensionKeys,omitempty"`
	ID             string            `json:"id"`
	AppID          string            `json:"appId"`
	Destination    string            `json:"destination"`
	DestinationARN string            `json:"destinationArn,omitempty"`
	Name           string            `json:"name"`
	Namespace      string            `json:"namespace,omitempty"`
	EventPattern   string            `json:"eventPattern,omitempty"`
	UnitLabel      string            `json:"unitLabel,omitempty"`
	ValueKey       string            `json:"valueKey,omitempty"`
	Order          int64             `json:"order"`
}
