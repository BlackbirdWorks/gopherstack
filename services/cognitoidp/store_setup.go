package cognitoidp

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (and composite-keyed map[string]map[string]*T) resource field
// on InMemoryBackend that has a pure key function is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/sqs pilot (commit 0f09d77c) / services/ec2 sweep (commit
// 12e611a4) for the pattern this follows.
//
// cognitoidp's resource maps nest two levels deep (poolID -> subKey -> *T)
// far more often than ec2's flat maps do, since almost every resource in this
// backend belongs to exactly one user pool. Each such map is flattened into a
// single store.Table keyed by a composite "poolID:subKey" string, with a
// secondary store.Index keyed by poolID alone standing in for the "give me
// every X in pool P" access pattern the nested map used to serve directly
// (e.g. ListUsers, ListGroups).
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment block above registerAllTables for the list and why.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// userKey builds the composite primary key for the users table: poolID+":"+username.
func userKey(poolID, username string) string { return poolID + ":" + username }

// userSubKey builds the secondary-index key for looking up a user by pool+sub.
func userSubKey(poolID, sub string) string { return poolID + ":" + sub }

// groupKey builds the composite primary key for the groups table.
func groupKey(poolID, groupName string) string { return poolID + ":" + groupName }

// resourceServerKey builds the composite primary key for the resourceServers table.
func resourceServerKey(poolID, identifier string) string { return poolID + ":" + identifier }

// identityProviderKey builds the composite primary key for the identityProviders table.
func identityProviderKey(poolID, providerName string) string { return poolID + ":" + providerName }

// userImportJobKey builds the composite primary key for the userImportJobs table.
func userImportJobKey(poolID, jobID string) string { return poolID + ":" + jobID }

// managedLoginBrandingKey builds the composite primary key for the managedLoginBrandings table.
func managedLoginBrandingKey(poolID, brandingID string) string { return poolID + ":" + brandingID }

func poolsKeyFn(v *UserPool) string       { return v.ID }
func poolsNameIndexFn(v *UserPool) string { return v.Name }

func clientsKeyFn(v *UserPoolClient) string       { return v.ClientID }
func clientsPoolIndexFn(v *UserPoolClient) string { return v.UserPoolID }

func usersKeyFn(v *User) string       { return userKey(v.UserPoolID, v.Username) }
func usersPoolIndexFn(v *User) string { return v.UserPoolID }
func usersSubIndexFn(v *User) string  { return userSubKey(v.UserPoolID, v.Sub) }

func groupsKeyFn(v *Group) string       { return groupKey(v.UserPoolID, v.GroupName) }
func groupsPoolIndexFn(v *Group) string { return v.UserPoolID }

func resourceServersKeyFn(v *ResourceServer) string {
	return resourceServerKey(v.UserPoolID, v.Identifier)
}
func resourceServersPoolIndexFn(v *ResourceServer) string { return v.UserPoolID }

func identityProvidersKeyFn(v *IdentityProvider) string {
	return identityProviderKey(v.UserPoolID, v.ProviderName)
}
func identityProvidersPoolIndexFn(v *IdentityProvider) string { return v.UserPoolID }

func domainsKeyFn(v *UserPoolDomain) string { return v.Domain }

func termsKeyFn(v *Terms) string { return v.UserPoolID }

func userImportJobsKeyFn(v *UserImportJob) string       { return userImportJobKey(v.UserPoolID, v.JobID) }
func userImportJobsPoolIndexFn(v *UserImportJob) string { return v.UserPoolID }

func managedLoginBrandingsKeyFn(v *ManagedLoginBranding) string {
	return managedLoginBrandingKey(v.UserPoolID, v.ManagedLoginBrandingID)
}
func managedLoginBrandingsPoolIndexFn(v *ManagedLoginBranding) string { return v.UserPoolID }

func uiCustomizationsKeyFn(v *UICustomization) string { return uiKey(v.UserPoolID, v.ClientID) }

func typedRiskConfigurationsKeyFn(v *TypedRiskConfiguration) string {
	return v.UserPoolID + ":" + v.ClientID
}

// poolNameExists reports whether a pool with the given (globally unique) Name
// already exists, via the poolsByName secondary index. Caller must hold at
// least a read lock.
func (b *InMemoryBackend) poolNameExists(name string) bool {
	return len(b.poolsByName.Get(name)) > 0
}

// userBySub looks up a user by poolID+sub via the usersBySub secondary index.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) userBySub(poolID, sub string) (*User, bool) {
	matches := b.usersBySub.Get(userSubKey(poolID, sub))
	if len(matches) == 0 {
		return nil, false
	}

	return matches[0], true
}

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because their value carries no pure identity for the key
// store.Table requires, or (groupMembers/resourceTags) because the value
// itself is a bare set/string map rather than a *T resource struct:
//   - refreshTokens / refreshTokensByClient / refreshTokensByUser:
//     refreshTokenEntry stores the pool/client/username a token belongs to
//     but not the token string itself (the map key) -- not a pure function of
//     the value. The By* maps are index-of-index views derived from the same
//     raw map and share this constraint.
//   - mfaSessions: mfaSessionEntry stores PoolID/ClientID/Username but not the
//     session token itself (the map key) -- same constraint as refreshTokens.
//   - attrVerificationCodes: attrVerificationEntry (ExpiresAt, Code) carries
//     no identity fields at all; the poolID:username:attrName key is entirely
//     external to the value.
//   - tokenRevokedBefore: value type is time.Time, not a resource struct with
//     an identity field -- the poolID:username key is external to the value.
//   - resourceTags: value type is map[string]string (tag key -> tag value),
//     not a *T resource struct.
//   - riskConfigurations / logDeliveryConfigs / poolMfaConfigs: RiskConfiguration
//     and LogDeliveryConfig hold only an opaque Raw map[string]any blob, and
//     UserPoolMfaFullConfig holds only sub-config pointers -- none carry the
//     poolID (+clientID) identity that is their map key.
//   - devices / webauthnCredentials / authEvents: Device, WebAuthnCredential,
//     and AuthEvent carry their own leaf identity (DeviceKey/CredentialID/
//     EventID) but not the poolID+":"+username of the user they belong to, so
//     the outer nesting key is not reconstructable from the value alone.
//   - groupMembers: map[string]map[string]map[string]struct{} is a pure
//     membership set (poolID -> groupName -> set of usernames), not a
//     collection of *T resource structs.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (and any secondary store.AddIndex calls) to the concrete field.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.pools = store.Register(b.registry, "pools", store.New(poolsKeyFn))
		b.poolsByName = b.pools.AddIndex("byName", poolsNameIndexFn)
	},
	func(b *InMemoryBackend) {
		b.clients = store.Register(b.registry, "clients", store.New(clientsKeyFn))
		b.clientsByPool = b.clients.AddIndex("byPool", clientsPoolIndexFn)
	},
	func(b *InMemoryBackend) {
		b.users = store.Register(b.registry, "users", store.New(usersKeyFn))
		b.usersByPool = b.users.AddIndex("byPool", usersPoolIndexFn)
		b.usersBySub = b.users.AddIndex("bySub", usersSubIndexFn)
	},
	func(b *InMemoryBackend) {
		b.groups = store.Register(b.registry, "groups", store.New(groupsKeyFn))
		b.groupsByPool = b.groups.AddIndex("byPool", groupsPoolIndexFn)
	},
	func(b *InMemoryBackend) {
		b.resourceServers = store.Register(b.registry, "resourceServers", store.New(resourceServersKeyFn))
		b.resourceServersByPool = b.resourceServers.AddIndex("byPool", resourceServersPoolIndexFn)
	},
	func(b *InMemoryBackend) {
		b.identityProviders = store.Register(b.registry, "identityProviders", store.New(identityProvidersKeyFn))
		b.identityProvidersByPool = b.identityProviders.AddIndex("byPool", identityProvidersPoolIndexFn)
	},
	func(b *InMemoryBackend) {
		b.domains = store.Register(b.registry, "domains", store.New(domainsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.terms = store.Register(b.registry, "terms", store.New(termsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.userImportJobs = store.Register(b.registry, "userImportJobs", store.New(userImportJobsKeyFn))
		b.userImportJobsByPool = b.userImportJobs.AddIndex("byPool", userImportJobsPoolIndexFn)
	},
	func(b *InMemoryBackend) {
		b.managedLoginBrandings = store.Register(
			b.registry,
			"managedLoginBrandings",
			store.New(managedLoginBrandingsKeyFn),
		)
		b.managedLoginBrandingsByPool = b.managedLoginBrandings.AddIndex("byPool", managedLoginBrandingsPoolIndexFn)
	},
	func(b *InMemoryBackend) {
		b.uiCustomizations = store.Register(b.registry, "uiCustomizations", store.New(uiCustomizationsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.typedRiskConfigurations = store.Register(
			b.registry,
			"typedRiskConfigurations",
			store.New(typedRiskConfigurationsKeyFn),
		)
	},
}
