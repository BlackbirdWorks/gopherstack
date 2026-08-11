package account

// RegionOptStatus represents the opt-in status of an AWS region.
type RegionOptStatus string

const (
	RegionOptStatusEnabled        RegionOptStatus = "ENABLED"
	RegionOptStatusDisabled       RegionOptStatus = "DISABLED"
	RegionOptStatusEnabling       RegionOptStatus = "ENABLING"
	RegionOptStatusDisabling      RegionOptStatus = "DISABLING"
	RegionOptStatusEnabledDefault RegionOptStatus = "ENABLED_BY_DEFAULT"
)

// ContactType represents the type of alternate contact.
type ContactType string

const (
	ContactTypeBilling    ContactType = "BILLING"
	ContactTypeOperations ContactType = "OPERATIONS"
	ContactTypeSecurity   ContactType = "SECURITY"
)

// isValidContactType reports whether ct is one of the three accepted values.
func isValidContactType(ct ContactType) bool {
	return ct == ContactTypeBilling || ct == ContactTypeOperations || ct == ContactTypeSecurity
}

// State represents the lifecycle state of an AWS account as reported by
// GetAccountInformation. Real Account Management never transitions an
// account to SUSPENDED/CLOSED/PENDING_ACTIVATION from this service --
// account closure is an AWS Organizations concept (see the CloseAccount
// operation already implemented by services/organizations), so this
// service's backend always reports ACTIVE.
type State string

const (
	StateActive            State = "ACTIVE"
	StatePendingActivation State = "PENDING_ACTIVATION"
	StateSuspended         State = "SUSPENDED"
	StateClosed            State = "CLOSED"
)

// PrimaryEmailUpdateStatus represents the status of the most recent primary
// email update request, as reported by GetPrimaryEmailUpdateStatus.
type PrimaryEmailUpdateStatus string

const (
	PrimaryEmailUpdateStatusPending   PrimaryEmailUpdateStatus = "PENDING"
	PrimaryEmailUpdateStatusAccepted  PrimaryEmailUpdateStatus = "ACCEPTED"
	PrimaryEmailUpdateStatusCompleted PrimaryEmailUpdateStatus = "COMPLETED"
	PrimaryEmailUpdateStatusFailed    PrimaryEmailUpdateStatus = "FAILED"
)

// Info holds the fields returned by GetAccountInformation.
type Info struct {
	AccountID          string `json:"AccountId"`
	AccountName        string `json:"AccountName"`
	AccountCreatedDate string `json:"AccountCreatedDate"`
	AccountState       State  `json:"AccountState"`
}

// Region represents an AWS region and its opt-in status.
type Region struct {
	RegionName      string          `json:"RegionName"`
	RegionOptStatus RegionOptStatus `json:"RegionOptStatus"`
}

// AlternateContact holds alternate contact information.
type AlternateContact struct {
	AlternateContactType ContactType `json:"AlternateContactType"`
	EmailAddress         string      `json:"EmailAddress,omitempty"`
	Name                 string      `json:"Name,omitempty"`
	PhoneNumber          string      `json:"PhoneNumber,omitempty"`
	Title                string      `json:"Title,omitempty"`
}

// ContactInformation holds primary contact information for the account.
type ContactInformation struct {
	AddressLine1     string `json:"AddressLine1"`
	AddressLine2     string `json:"AddressLine2,omitempty"`
	AddressLine3     string `json:"AddressLine3,omitempty"`
	City             string `json:"City"`
	CompanyName      string `json:"CompanyName,omitempty"`
	CountryCode      string `json:"CountryCode"`
	DistrictOrCounty string `json:"DistrictOrCounty,omitempty"`
	FullName         string `json:"FullName"`
	PhoneNumber      string `json:"PhoneNumber"`
	PostalCode       string `json:"PostalCode"`
	StateOrRegion    string `json:"StateOrRegion,omitempty"`
	WebsiteURL       string `json:"WebsiteUrl,omitempty"`
}
