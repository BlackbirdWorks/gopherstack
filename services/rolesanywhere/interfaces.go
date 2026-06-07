package rolesanywhere

// StorageBackend defines the interface for Roles Anywhere backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Trust anchor operations
	CreateTrustAnchor(name string, source TrustAnchorSource, tags []TagEntry) (*TrustAnchor, error)
	GetTrustAnchor(id string) (*TrustAnchor, error)
	ListTrustAnchors(pageToken string, maxResults int) ([]*TrustAnchor, string, error)
	DeleteTrustAnchor(id string) error
	UpdateTrustAnchor(id, name string, source *TrustAnchorSource) (*TrustAnchor, error)
	EnableTrustAnchor(id string) (*TrustAnchor, error)
	DisableTrustAnchor(id string) (*TrustAnchor, error)

	// Profile operations
	CreateProfile(
		name string,
		roleArns []string,
		tags []TagEntry,
		durationSeconds *int32,
		managedPolicyArns []string,
		sessionPolicy string,
		requireInstanceProperties bool,
	) (*Profile, error)
	GetProfile(id string) (*Profile, error)
	ListProfiles(pageToken string, maxResults int) ([]*Profile, string, error)
	DeleteProfile(id string) error
	UpdateProfile(
		id, name string,
		roleArns []string,
		durationSeconds *int32,
		managedPolicyArns []string,
		sessionPolicy string,
		requireInstanceProperties *bool,
	) (*Profile, error)
	EnableProfile(id string) (*Profile, error)
	DisableProfile(id string) (*Profile, error)

	// CRL operations
	ImportCrl(name string, crlData []byte, trustAnchorArn string, enabled bool, tags []TagEntry) (*Crl, error)
	GetCrl(id string) (*Crl, error)
	ListCrls(pageToken string, maxResults int) ([]*Crl, string, error)
	UpdateCrl(id, name string, crlData []byte) (*Crl, error)
	DeleteCrl(id string) (*Crl, error)
	EnableCrl(id string) (*Crl, error)
	DisableCrl(id string) (*Crl, error)

	// Subject operations
	GetSubject(id string) (*Subject, error)
	ListSubjects(pageToken string, maxResults int) ([]*Subject, string, error)

	// Attribute mapping operations
	PutAttributeMapping(profileID, certificateField string, rules []MappingRule) (*Profile, error)
	DeleteAttributeMapping(profileID, certificateField string, specifiers []string) (*Profile, error)
	GetAttributeMappings(profileID string) []AttributeMapping

	// Notification settings operations
	PutNotificationSettings(trustAnchorID string, settings []NotificationSetting) (*TrustAnchor, error)
	ResetNotificationSettings(trustAnchorID string, keys []NotificationSettingKey) (*TrustAnchor, error)
	GetNotificationSettings(trustAnchorID string) []NotificationSetting

	// Tag operations
	TagResource(resourceARN string, tags []TagEntry) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) ([]TagEntry, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion.
var _ StorageBackend = (*InMemoryBackend)(nil)
