package account

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	errNoAlternateContact = errors.New("ResourceNotFoundException: no alternate contact found")
	errNoContactInfo      = errors.New("ResourceNotFoundException: no contact information set")
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
	WebsiteUrl       string `json:"WebsiteUrl,omitempty"`
}

// StorageBackend defines the interface for the account service backend.
type StorageBackend interface {
	Reset()
	DescribeAccount() (*AccountDetails, error)
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
func (b *InMemoryBackend) DescribeAccount() (*AccountDetails, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return &AccountDetails{
		Arn:          fmt.Sprintf("arn:aws:organizations::%s:account/o-fake/%s", b.accountID, b.accountID),
		Email:        "admin@example.com",
		Id:           b.accountID,
		Name:         "Test Account",
		Status:       "ACTIVE",
		JoinedMethod: "CREATED",
	}, nil
}

// ListRegions returns regions filtered by opt-in status.
func (b *InMemoryBackend) ListRegions(statusFilter []RegionOptStatus, maxResults int, nextToken string) ([]*Region, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	filtered := make([]*Region, 0, len(b.regions))

	for _, r := range b.regions {
		if len(statusFilter) == 0 || slices.Contains(statusFilter, r.RegionOptStatus) {
			filtered = append(filtered, r)
		}
	}

	return filtered, "", nil
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
