package cognitoidp

import (
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	// bcryptCost is deliberately low (not DefaultCost): this is a mock backend
	// with no real secrets to protect, and DefaultCost under -race measured
	// ~1.3s per hash (vs ~95ms unraced), which was the dominant cost across
	// the package's -race test time (gopherstack CI flake investigation).
	bcryptCost = bcrypt.MinCost

	// poolIDSuffixLen is the length of the random suffix in pool IDs.
	poolIDSuffixLen = 8

	// clientIDLen is the length of randomly generated client IDs.
	clientIDLen = 26

	// clientSecretLen is the length of randomly generated client secrets.
	clientSecretLen = 51

	// clientSecretIDLen is the length of the random suffix used for
	// AddUserPoolClientSecret's ClientSecretId (real AWS's format is
	// documented as "--", an opaque identifier this emulator does not
	// attempt to reproduce structurally).
	clientSecretIDLen = 20

	// alphanumChars contains characters used for random ID generation.
	alphanumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// UserStatusUnconfirmed indicates the user has signed up but not confirmed their account.
	UserStatusUnconfirmed = "UNCONFIRMED"

	// UserStatusConfirmed indicates the user has confirmed their account.
	UserStatusConfirmed = "CONFIRMED"

	// UserStatusForceChangePassword indicates the user must change their password on next login.
	UserStatusForceChangePassword = "FORCE_CHANGE_PASSWORD"

	// defaultRefreshTokenTTL is the lifetime for refresh tokens.
	defaultRefreshTokenTTL = 30 * 24 * time.Hour
)

// InMemoryBackend is the in-memory store for Cognito IDP resources.
//
// Most resource collections are *store.Table[T] registered on b.registry (see
// store_setup.go for keyFns/composite keys and the full per-field rationale).
// A handful of fields remain plain maps because their value carries no pure
// identity for a store.Table key; store_setup.go's registerAllTables doc
// comment lists each one and why.
type InMemoryBackend struct {
	lambdaInvoker               LambdaTriggerInvoker
	domains                     *store.Table[UserPoolDomain]
	resourceServers             *store.Table[ResourceServer]
	poolsByName                 *store.Index[UserPool]
	clients                     *store.Table[UserPoolClient]
	clientsByPool               *store.Index[UserPoolClient]
	users                       *store.Table[User]
	usersByPool                 *store.Index[User]
	usersBySub                  *store.Index[User]
	refreshTokens               map[string]*refreshTokenEntry
	refreshTokensByClient       map[string]map[string]struct{}
	refreshTokensByUser         map[string]map[string]struct{}
	mfaSessions                 map[string]*mfaSessionEntry
	groups                      *store.Table[Group]
	logDeliveryConfigs          map[string]*LogDeliveryConfig
	groupMembers                map[string]map[string]map[string]struct{}
	riskConfigurations          map[string]*RiskConfiguration
	resourceServersByPool       *store.Index[ResourceServer]
	tokenRevokedBeforeSeq       map[string]int64
	tokenRevokedBefore          map[string]time.Time
	registry                    *store.Registry
	identityProviders           *store.Table[IdentityProvider]
	identityProvidersByPool     *store.Index[IdentityProvider]
	mu                          *lockmetrics.RWMutex
	pools                       *store.Table[UserPool]
	resourceTags                map[string]map[string]string
	groupsByPool                *store.Index[Group]
	uiCustomizations            *store.Table[UICustomization]
	managedLoginBrandings       *store.Table[ManagedLoginBranding]
	managedLoginBrandingsByPool *store.Index[ManagedLoginBranding]
	terms                       *store.Table[Terms]
	termsByPool                 *store.Index[Terms]
	userImportJobs              *store.Table[UserImportJob]
	userImportJobsByPool        *store.Index[UserImportJob]
	poolMfaConfigs              map[string]*UserPoolMfaFullConfig
	attrVerificationCodes       map[string]*attrVerificationEntry
	typedRiskConfigurations     *store.Table[TypedRiskConfiguration]
	devices                     map[string]map[string]*Device
	webauthnCredentials         map[string]map[string]*WebAuthnCredential
	authEvents                  map[string]map[string]*AuthEvent
	userPoolReplicas            *store.Table[UserPoolReplica]
	userPoolReplicasByPool      *store.Index[UserPoolReplica]
	provisionedLimits           map[string]int32
	accountID                   string
	region                      string
	endpoint                    string
	tokenSeq                    int64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                    lockmetrics.New("cognitoidp"),
		registry:              store.NewRegistry(),
		refreshTokens:         make(map[string]*refreshTokenEntry),
		refreshTokensByClient: make(map[string]map[string]struct{}),
		refreshTokensByUser:   make(map[string]map[string]struct{}),
		mfaSessions:           make(map[string]*mfaSessionEntry),
		groupMembers:          make(map[string]map[string]map[string]struct{}),
		tokenRevokedBeforeSeq: make(map[string]int64),
		tokenRevokedBefore:    make(map[string]time.Time),
		resourceTags:          make(map[string]map[string]string),
		riskConfigurations:    make(map[string]*RiskConfiguration),
		logDeliveryConfigs:    make(map[string]*LogDeliveryConfig),
		poolMfaConfigs:        make(map[string]*UserPoolMfaFullConfig),
		attrVerificationCodes: make(map[string]*attrVerificationEntry),
		devices:               make(map[string]map[string]*Device),
		webauthnCredentials:   make(map[string]map[string]*WebAuthnCredential),
		authEvents:            make(map[string]map[string]*AuthEvent),
		provisionedLimits:     make(map[string]int32),
		accountID:             accountID,
		region:                region,
		endpoint:              endpoint,
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state. Useful for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.refreshTokens = make(map[string]*refreshTokenEntry)
	b.refreshTokensByClient = make(map[string]map[string]struct{})
	b.refreshTokensByUser = make(map[string]map[string]struct{})
	b.mfaSessions = make(map[string]*mfaSessionEntry)
	b.groupMembers = make(map[string]map[string]map[string]struct{})
	b.tokenRevokedBeforeSeq = make(map[string]int64)
	b.tokenRevokedBefore = make(map[string]time.Time)
	b.tokenSeq = 0
	b.resourceTags = make(map[string]map[string]string)
	b.riskConfigurations = make(map[string]*RiskConfiguration)
	b.logDeliveryConfigs = make(map[string]*LogDeliveryConfig)
	b.poolMfaConfigs = make(map[string]*UserPoolMfaFullConfig)
	b.attrVerificationCodes = make(map[string]*attrVerificationEntry)
	b.devices = make(map[string]map[string]*Device)
	b.webauthnCredentials = make(map[string]map[string]*WebAuthnCredential)
	b.authEvents = make(map[string]map[string]*AuthEvent)
	b.provisionedLimits = make(map[string]int32)
}
