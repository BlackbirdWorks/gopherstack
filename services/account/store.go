package account

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
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
	GetPrimaryEmailUpdateStatus() (PrimaryEmailUpdateStatus, time.Time, error)
	PutAccountName(name string) error
	GetGovCloudAccountInformation() (string, State, error)

	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so a persistence manager that
	// registers Account can pick it up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	primaryEmailUpdateAt     time.Time
	accountCreatedDate       time.Time
	registry                 *store.Registry
	alternateContacts        *store.Table[AlternateContact]
	contactInfo              *ContactInformation
	mu                       *lockmetrics.RWMutex
	work                     *worker.Group
	accountName              string
	primaryEmail             string
	pendingEmail             string
	pendingOTP               string
	primaryEmailUpdateStatus PrimaryEmailUpdateStatus
	region                   string
	accountID                string
	regions                  []*Region
	transitionDelay          time.Duration
}

// simOTP is a fixed OTP used for simulation — callers pass it back to AcceptPrimaryEmailUpdate.
const simOTP = "123456"

// defaultPrimaryEmail is the initial primary email for all new backends.
const defaultPrimaryEmail = "admin@example.com"

// defaultAccountName is the initial account display name for all new
// backends.
const defaultAccountName = "Test Account"

// regionTransitionDelay is how long a region stays ENABLING/DISABLING and a
// primary email update stays ACCEPTED before settling.
const regionTransitionDelay = 100 * time.Millisecond

// NewInMemoryBackend creates a backend whose timers are rooted at context.Background().
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a backend whose async transitions run
// on a worker.Group rooted at ctx.
func NewInMemoryBackendWithContext(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:          accountID,
		region:             region,
		accountName:        defaultAccountName,
		primaryEmail:       defaultPrimaryEmail,
		registry:           store.NewRegistry(),
		accountCreatedDate: time.Now().UTC(),
		mu:                 lockmetrics.New("account"),
		work:               worker.NewGroup(ctx, "account"),
		transitionDelay:    regionTransitionDelay,
	}
	registerAllTables(b)
	b.initDefaultRegions()

	return b
}

func defaultRegions() []*Region {
	defaults := []string{
		"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-west-2", "eu-west-3",
		"eu-central-1", "eu-north-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
		"ap-northeast-3", "ap-south-1", "ca-central-1", "sa-east-1",
	}
	seededEnabled := []string{"af-south-1", "ap-east-1"}
	optIn := []string{
		"ap-east-2", "ap-south-2", "ap-southeast-3", "ap-southeast-4", "ap-southeast-5", "ap-southeast-6",
		"ap-southeast-7", "ca-west-1", "eu-central-2", "eu-south-1", "eu-south-2", "il-central-1",
		"me-central-1", "me-south-1", "mx-central-1",
	}

	regions := make([]*Region, 0, len(defaults)+len(optIn)+len(seededEnabled))
	for _, name := range defaults {
		regions = append(regions, &Region{RegionName: name, RegionOptStatus: RegionOptStatusEnabledDefault})
	}

	// af-south-1 and ap-east-1 are seeded ENABLED so existing callers can exercise DisableRegion.
	for _, name := range seededEnabled {
		regions = append(regions, &Region{RegionName: name, RegionOptStatus: RegionOptStatusEnabled})
	}

	for _, name := range optIn {
		regions = append(regions, &Region{RegionName: name, RegionOptStatus: RegionOptStatusDisabled})
	}

	return regions
}

func (b *InMemoryBackend) initDefaultRegions() { b.regions = defaultRegions() }

// addMissingRegionsLocked tops up a restored snapshot taken before the region catalog grew.
func (b *InMemoryBackend) addMissingRegionsLocked() {
	for _, r := range defaultRegions() {
		if !b.knownRegionLocked(r.RegionName) {
			b.regions = append(b.regions, r)
		}
	}
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.contactInfo = nil
	b.accountName = defaultAccountName
	b.primaryEmail = defaultPrimaryEmail
	b.pendingEmail = ""
	b.pendingOTP = ""
	b.primaryEmailUpdateStatus = ""
	b.primaryEmailUpdateAt = time.Time{}
	b.initDefaultRegions()
}

// SetLifecycleDelay sets how long opt-in regions stay ENABLING/DISABLING and a primary email
// update stays ACCEPTED. The default is 100ms; a negative value is ignored.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	if d < 0 {
		return
	}

	b.mu.Lock("SetLifecycleDelay")
	b.transitionDelay = d
	b.mu.Unlock()
}

// Close stops pending transition timers. Safe to call more than once.
func (b *InMemoryBackend) Close() { b.work.Stop() }
