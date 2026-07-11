package account

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	errNoAlternateContact = errors.New("ResourceNotFoundException: no alternate contact found")
	errNoContactInfo      = errors.New("ResourceNotFoundException: no contact information set")
	errRegionNotFound     = errors.New("ResourceNotFoundException: region not found")
	errRegionNotOptIn     = errors.New("ValidationException: only opt-in regions can be enabled or disabled")
	errNoPendingUpdate    = errors.New("ResourceNotFoundException: no primary email update in progress")
	errInvalidOTP         = errors.New("ValidationException: invalid OTP")
	// errInvalidNextToken is returned when ListRegions receives an undecodable cursor.
	errInvalidNextToken = errors.New("ValidationException: invalid nextToken")
)

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

// Details holds information about the AWS account.
type Details struct {
	Arn             string `json:"Arn,omitempty"`
	Email           string `json:"Email,omitempty"`
	ID              string `json:"Id,omitempty"`
	Name            string `json:"Name,omitempty"`
	Status          string `json:"Status,omitempty"`
	JoinedMethod    string `json:"JoinedMethod,omitempty"`
	JoinedTimestamp string `json:"JoinedTimestamp,omitempty"`
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

// StorageBackend defines the interface for the account service backend.
type StorageBackend interface {
	Reset()
	DescribeAccount() (*Details, error)
	ListRegions(statusFilter []RegionOptStatus, maxResults int, nextToken string) ([]*Region, string, error)
	EnableRegion(regionName string) error
	DisableRegion(regionName string) error
	GetRegionOptStatus(regionName string) (RegionOptStatus, error)
	GetAlternateContact(ContactType) (*AlternateContact, error)
	PutAlternateContact(*AlternateContact) error
	DeleteAlternateContact(ContactType) error
	GetContactInformation() (*ContactInformation, error)
	PutContactInformation(*ContactInformation) error
	GetPrimaryEmail() string
	StartPrimaryEmailUpdate(email string) (string, error)
	AcceptPrimaryEmailUpdate(otp, email string) error
	PutAccountName(name string) error
	CloseAccount() error

	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so a persistence manager that
	// registers Account can pick it up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	registry          *store.Registry
	alternateContacts *store.Table[AlternateContact]
	contactInfo       *ContactInformation
	accountID         string
	region            string
	accountName       string
	primaryEmail      string
	pendingEmail      string
	pendingOTP        string
	regions           []*Region
	closed            bool
	mu                sync.RWMutex
}

// simOTP is a fixed OTP used for simulation — callers pass it back to AcceptPrimaryEmailUpdate.
const simOTP = "123456"

// defaultPrimaryEmail is the initial primary email for all new backends.
const defaultPrimaryEmail = "admin@example.com"

// defaultAccountName is the initial account display name for all new
// backends.
const defaultAccountName = "Test Account"

// NewInMemoryBackend creates a new in-memory backend for the account service.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:    accountID,
		region:       region,
		accountName:  defaultAccountName,
		primaryEmail: defaultPrimaryEmail,
		registry:     store.NewRegistry(),
	}
	registerAllTables(b)
	b.initDefaultRegions()

	return b
}

func (b *InMemoryBackend) initDefaultRegions() {
	b.regions = []*Region{
		{RegionName: "us-east-1", RegionOptStatus: RegionOptStatusEnabledDefault},
		{RegionName: "us-east-2", RegionOptStatus: RegionOptStatusEnabledDefault},
		{RegionName: "us-west-1", RegionOptStatus: RegionOptStatusEnabledDefault},
		{RegionName: "us-west-2", RegionOptStatus: RegionOptStatusEnabledDefault},
		{RegionName: "eu-west-1", RegionOptStatus: RegionOptStatusEnabledDefault},
		{RegionName: "eu-central-1", RegionOptStatus: RegionOptStatusEnabledDefault},
		// Opt-in regions: already ENABLED but can be disabled via DisableRegion.
		{RegionName: "ap-southeast-1", RegionOptStatus: RegionOptStatusEnabled},
		{RegionName: "ap-northeast-1", RegionOptStatus: RegionOptStatusEnabled},
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.contactInfo = nil
	b.accountName = defaultAccountName
	b.primaryEmail = defaultPrimaryEmail
	b.pendingEmail = ""
	b.pendingOTP = ""
	b.closed = false
	b.initDefaultRegions()
}

// DescribeAccount returns account details.
func (b *InMemoryBackend) DescribeAccount() (*Details, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return &Details{
		Arn:          arn.Build("organizations", "", b.accountID, fmt.Sprintf("account/o-fake/%s", b.accountID)),
		Email:        b.primaryEmail,
		ID:           b.accountID,
		Name:         b.accountName,
		Status:       "ACTIVE",
		JoinedMethod: "CREATED",
	}, nil
}

// ListRegions returns regions filtered by opt-in status, honouring AWS's
// maxResults/nextToken pagination. nextToken is an opaque cursor (base64 of the
// exclusive-start RegionName); when maxResults <= 0 the full filtered list is returned.
func (b *InMemoryBackend) ListRegions(
	statusFilter []RegionOptStatus,
	maxResults int,
	nextToken string,
) ([]*Region, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	filtered := make([]*Region, 0, len(b.regions))

	for _, r := range b.regions {
		if len(statusFilter) == 0 || slices.Contains(statusFilter, r.RegionOptStatus) {
			filtered = append(filtered, r)
		}
	}

	// Order deterministically by RegionName so the name-based pagination cursor is
	// stable across pages (AWS returns regions in alphabetical order).
	slices.SortFunc(filtered, func(a, b *Region) int {
		return strings.Compare(a.RegionName, b.RegionName)
	})

	// Apply the exclusive-start cursor: skip everything up to and including the
	// region named by the decoded token.
	if nextToken != "" {
		start, decErr := decodeRegionToken(nextToken)
		if decErr != nil {
			return nil, "", errInvalidNextToken
		}

		idx := 0
		for idx < len(filtered) && filtered[idx].RegionName <= start {
			idx++
		}

		filtered = filtered[idx:]
	}

	if maxResults <= 0 || maxResults >= len(filtered) {
		return filtered, "", nil
	}

	page := filtered[:maxResults]

	return page, encodeRegionToken(page[len(page)-1].RegionName), nil
}

// EnableRegion transitions an opt-in region from DISABLED to ENABLED.
// ENABLED_BY_DEFAULT regions return a ValidationException per AWS semantics.
func (b *InMemoryBackend) EnableRegion(regionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, r := range b.regions {
		if r.RegionName != regionName {
			continue
		}

		if r.RegionOptStatus == RegionOptStatusEnabledDefault {
			return fmt.Errorf("%w: %s", errRegionNotOptIn, regionName)
		}

		r.RegionOptStatus = RegionOptStatusEnabled

		return nil
	}

	return fmt.Errorf("%w: %s", errRegionNotFound, regionName)
}

// DisableRegion transitions an opt-in region from ENABLED to DISABLED.
// ENABLED_BY_DEFAULT regions return a ValidationException per AWS semantics.
func (b *InMemoryBackend) DisableRegion(regionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, r := range b.regions {
		if r.RegionName != regionName {
			continue
		}

		if r.RegionOptStatus == RegionOptStatusEnabledDefault {
			return fmt.Errorf("%w: %s", errRegionNotOptIn, regionName)
		}

		r.RegionOptStatus = RegionOptStatusDisabled

		return nil
	}

	return fmt.Errorf("%w: %s", errRegionNotFound, regionName)
}

// GetRegionOptStatus returns the current opt-in status for a single region.
func (b *InMemoryBackend) GetRegionOptStatus(regionName string) (RegionOptStatus, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, r := range b.regions {
		if r.RegionName == regionName {
			return r.RegionOptStatus, nil
		}
	}

	return "", fmt.Errorf("%w: %s", errRegionNotFound, regionName)
}

// encodeRegionToken produces an opaque pagination cursor for the given RegionName.
func encodeRegionToken(regionName string) string {
	return base64.StdEncoding.EncodeToString([]byte(regionName))
}

// decodeRegionToken reverses encodeRegionToken.
func decodeRegionToken(token string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}

	return string(raw), nil
}

// GetAlternateContact retrieves an alternate contact by type.
func (b *InMemoryBackend) GetAlternateContact(ct ContactType) (*AlternateContact, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	contact, ok := b.alternateContacts.Get(string(ct))
	if !ok {
		return nil, fmt.Errorf("%w: type %s", errNoAlternateContact, ct)
	}

	cp := *contact

	return &cp, nil
}

// PutAlternateContact creates or updates an alternate contact.
func (b *InMemoryBackend) PutAlternateContact(contact *AlternateContact) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *contact
	b.alternateContacts.Put(&cp)

	return nil
}

// DeleteAlternateContact removes an alternate contact by type.
func (b *InMemoryBackend) DeleteAlternateContact(ct ContactType) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.alternateContacts.Delete(string(ct)) {
		return fmt.Errorf("%w: type %s", errNoAlternateContact, ct)
	}

	return nil
}

// GetContactInformation retrieves primary contact information.
func (b *InMemoryBackend) GetContactInformation() (*ContactInformation, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.contactInfo == nil {
		return nil, errNoContactInfo
	}

	cp := *b.contactInfo

	return &cp, nil
}

// PutContactInformation sets primary contact information.
func (b *InMemoryBackend) PutContactInformation(info *ContactInformation) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *info
	b.contactInfo = &cp

	return nil
}

// GetPrimaryEmail returns the current primary email address.
func (b *InMemoryBackend) GetPrimaryEmail() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.primaryEmail
}

// StartPrimaryEmailUpdate initiates a primary email change.
// Returns a fixed simulation OTP that the caller must pass to AcceptPrimaryEmailUpdate.
func (b *InMemoryBackend) StartPrimaryEmailUpdate(email string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pendingEmail = email
	b.pendingOTP = simOTP

	return simOTP, nil
}

// AcceptPrimaryEmailUpdate confirms a pending email change using the OTP.
func (b *InMemoryBackend) AcceptPrimaryEmailUpdate(otp, email string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pendingEmail == "" {
		return errNoPendingUpdate
	}

	if otp != b.pendingOTP || email != b.pendingEmail {
		return errInvalidOTP
	}

	b.primaryEmail = b.pendingEmail
	b.pendingEmail = ""
	b.pendingOTP = ""

	return nil
}

// PutAccountName updates the account's display name.
func (b *InMemoryBackend) PutAccountName(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.accountName = name

	return nil
}

// CloseAccount marks the account as closed.
func (b *InMemoryBackend) CloseAccount() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	return nil
}
