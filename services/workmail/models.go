package workmail

import "time"

const (
	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"
	stateDeleted  = "DELETED"
	stateActive   = "ACTIVE"

	memberTypeUser  = "USER"
	memberTypeGroup = "GROUP"

	roleUser       = "USER"
	roleResource   = "RESOURCE"
	roleSystemUser = "SYSTEM_USER"

	providerEWS    = "EWS"
	providerLambda = "LAMBDA"

	defaultMailboxQuota = int32(50000)

	effectAllow = "ALLOW"
	effectDeny  = "DENY"

	// dnsVerificationPending/dnsVerificationVerified mirror
	// types.DnsRecordVerificationStatus (PENDING/VERIFIED/FAILED -- FAILED is
	// never produced by this simulation, so it has no constant here) and are
	// shared by OwnershipVerificationStatus and DkimVerificationStatus on
	// MailDomain (see mail_domains.go/organizations.go).
	dnsVerificationPending  = "PENDING"
	dnsVerificationVerified = "VERIFIED"
)

// trackedAlias stores an alias along with its entity reference for conflict
// detection. It is an unexported (package-private) type, so its fields are
// exported purely for JSON round-tripping through store.Table -- see
// pkgs/store's package doc: encoding/json silently drops unexported fields,
// and since trackedAlias itself is never visible outside this package,
// exporting its fields does not change the package's public API.
type trackedAlias struct {
	Alias    string
	OrgID    string
	EntityID string
}

// issuedImpersonationToken stores metadata for an issued impersonation
// token. Like trackedAlias, it is unexported, so its fields are exported
// purely to survive JSON round-tripping.
type issuedImpersonationToken struct {
	Token     string
	ExpiresAt time.Time
	OrgID     string
	RoleID    string
}
