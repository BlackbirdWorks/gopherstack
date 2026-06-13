package rolesanywhere

import "context"

// StorageBackend defines the interface for Roles Anywhere backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Trust anchor operations
	CreateTrustAnchor(ctx context.Context, name string, source TrustAnchorSource, tags []TagEntry) (*TrustAnchor, error)
	GetTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error)
	ListTrustAnchors(ctx context.Context, pageToken string, maxResults int) ([]*TrustAnchor, string, error)
	DeleteTrustAnchor(ctx context.Context, id string) error
	UpdateTrustAnchor(ctx context.Context, id, name string, source *TrustAnchorSource) (*TrustAnchor, error)
	EnableTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error)
	DisableTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error)

	// Profile operations
	CreateProfile(
		ctx context.Context,
		name string,
		roleArns []string,
		tags []TagEntry,
		durationSeconds *int32,
		managedPolicyArns []string,
		sessionPolicy string,
		requireInstanceProperties bool,
	) (*Profile, error)
	GetProfile(ctx context.Context, id string) (*Profile, error)
	ListProfiles(ctx context.Context, pageToken string, maxResults int) ([]*Profile, string, error)
	DeleteProfile(ctx context.Context, id string) error
	UpdateProfile(
		ctx context.Context,
		id, name string,
		roleArns []string,
		durationSeconds *int32,
		managedPolicyArns []string,
		sessionPolicy string,
		requireInstanceProperties *bool,
	) (*Profile, error)
	EnableProfile(ctx context.Context, id string) (*Profile, error)
	DisableProfile(ctx context.Context, id string) (*Profile, error)

	// CRL operations
	ImportCrl(
		ctx context.Context,
		name string,
		crlData []byte,
		trustAnchorArn string,
		enabled bool,
		tags []TagEntry,
	) (*Crl, error)
	GetCrl(ctx context.Context, id string) (*Crl, error)
	ListCrls(ctx context.Context, pageToken string, maxResults int) ([]*Crl, string, error)
	UpdateCrl(ctx context.Context, id, name string, crlData []byte) (*Crl, error)
	DeleteCrl(ctx context.Context, id string) (*Crl, error)
	EnableCrl(ctx context.Context, id string) (*Crl, error)
	DisableCrl(ctx context.Context, id string) (*Crl, error)

	// Subject operations
	GetSubject(ctx context.Context, id string) (*Subject, error)
	ListSubjects(ctx context.Context, pageToken string, maxResults int) ([]*Subject, string, error)

	// Attribute mapping operations
	PutAttributeMapping(ctx context.Context, profileID, certificateField string, rules []MappingRule) (*Profile, error)
	DeleteAttributeMapping(
		ctx context.Context,
		profileID, certificateField string,
		specifiers []string,
	) (*Profile, error)
	GetAttributeMappings(ctx context.Context, profileID string) []AttributeMapping

	// Notification settings operations
	PutNotificationSettings(
		ctx context.Context,
		trustAnchorID string,
		settings []NotificationSetting,
	) (*TrustAnchor, error)
	ResetNotificationSettings(
		ctx context.Context,
		trustAnchorID string,
		keys []NotificationSettingKey,
	) (*TrustAnchor, error)
	GetNotificationSettings(ctx context.Context, trustAnchorID string) []NotificationSetting

	// Tag operations
	TagResource(ctx context.Context, resourceARN string, tags []TagEntry) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, resourceARN string) ([]TagEntry, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion.
var _ StorageBackend = (*InMemoryBackend)(nil)
