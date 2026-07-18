package account

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// StorageBackend defines the interface for the account service backend.
type StorageBackend interface {
	Reset()
	GetAccountInformation() (*Info, error)
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

	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so a persistence manager that
	// registers Account can pick it up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	accountCreatedDate time.Time
	registry           *store.Registry
	alternateContacts  *store.Table[AlternateContact]
	contactInfo        *ContactInformation
	accountID          string
	region             string
	accountName        string
	primaryEmail       string
	pendingEmail       string
	pendingOTP         string
	regions            []*Region
	mu                 sync.RWMutex
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
		accountID:          accountID,
		region:             region,
		accountName:        defaultAccountName,
		primaryEmail:       defaultPrimaryEmail,
		registry:           store.NewRegistry(),
		accountCreatedDate: time.Now().UTC(),
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
	b.initDefaultRegions()
}
