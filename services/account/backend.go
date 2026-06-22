package account

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
	errNoAlternateContact = errors.New("ResourceNotFoundException: no alternate contact found")
	errNoContactInfo      = errors.New("ResourceNotFoundException: no contact information set")
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
	GetAlternateContact(ContactType) (*AlternateContact, error)
	PutAlternateContact(*AlternateContact) error
	DeleteAlternateContact(ContactType) error
	GetContactInformation() (*ContactInformation, error)
	PutContactInformation(*ContactInformation) error
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	accountID         string
	region            string
	alternateContacts map[ContactType]*AlternateContact
	contactInfo       *ContactInformation
	regions           []*Region
	mu                sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory backend for the account service.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:         accountID,
		region:            region,
		alternateContacts: make(map[ContactType]*AlternateContact),
	}
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
		{RegionName: "ap-southeast-1", RegionOptStatus: RegionOptStatusEnabled},
		{RegionName: "ap-northeast-1", RegionOptStatus: RegionOptStatusEnabled},
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.alternateContacts = make(map[ContactType]*AlternateContact)
	b.contactInfo = nil
	b.initDefaultRegions()
}

// DescribeAccount returns account details.
func (b *InMemoryBackend) DescribeAccount() (*Details, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return &Details{
		Arn:          arn.Build("organizations", "", b.accountID, fmt.Sprintf("account/o-fake/%s", b.accountID)),
		Email:        "admin@example.com",
		ID:           b.accountID,
		Name:         "Test Account",
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

	contact, ok := b.alternateContacts[ct]
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
	b.alternateContacts[contact.AlternateContactType] = &cp

	return nil
}

// DeleteAlternateContact removes an alternate contact by type.
func (b *InMemoryBackend) DeleteAlternateContact(ct ContactType) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.alternateContacts[ct]; !ok {
		return fmt.Errorf("%w: type %s", errNoAlternateContact, ct)
	}

	delete(b.alternateContacts, ct)

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
