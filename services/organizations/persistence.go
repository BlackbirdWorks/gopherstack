package organizations

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	DelegatedAdmins  map[string]map[string]*DelegatedAdmin `json:"delegatedAdmins"`
	Handshakes       map[string]*Handshake                 `json:"handshakes"`
	Org              *Organization                         `json:"org"`
	Root             *Root                                 `json:"root"`
	ResourcePolicy   *ResourcePolicy                       `json:"resourcePolicy,omitempty"`
	Accounts         map[string]*Account                   `json:"accounts"`
	OUs              map[string]*OrganizationalUnit        `json:"ous"`
	Policies         map[string]*Policy                    `json:"policies"`
	ServiceAccess    map[string]time.Time                  `json:"serviceAccess"`
	CreateStatuses   map[string]*CreateAccountStatus       `json:"createStatuses"`
	PolicyTargets    map[string][]string                   `json:"policyTargets"`
	TargetPolicies   map[string][]string                   `json:"targetPolicies"`
	AccountParent    map[string]string                     `json:"accountParent"`
	OUParent         map[string]string                     `json:"ouParent"`
	Tags             map[string]map[string]string          `json:"tags"`
	EmailToAccountID map[string]string                     `json:"emailToAccountID"`
	AccountID        string                                `json:"accountID"`
	Region           string                                `json:"region"`
	AccountCounter   int                                   `json:"accountCounter"`
	StatusCounter    int                                   `json:"statusCounter"`
}

// ensureNonNilMaps replaces nil maps in snap with empty allocations.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Accounts == nil {
		snap.Accounts = make(map[string]*Account)
	}

	if snap.OUs == nil {
		snap.OUs = make(map[string]*OrganizationalUnit)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]*Policy)
	}

	if snap.PolicyTargets == nil {
		snap.PolicyTargets = make(map[string][]string)
	}

	if snap.TargetPolicies == nil {
		snap.TargetPolicies = make(map[string][]string)
	}

	if snap.AccountParent == nil {
		snap.AccountParent = make(map[string]string)
	}

	if snap.OUParent == nil {
		snap.OUParent = make(map[string]string)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.CreateStatuses == nil {
		snap.CreateStatuses = make(map[string]*CreateAccountStatus)
	}

	if snap.ServiceAccess == nil {
		snap.ServiceAccess = make(map[string]time.Time)
	}

	if snap.DelegatedAdmins == nil {
		snap.DelegatedAdmins = make(map[string]map[string]*DelegatedAdmin)
	}

	if snap.Handshakes == nil {
		snap.Handshakes = make(map[string]*Handshake)
	}

	if snap.EmailToAccountID == nil {
		snap.EmailToAccountID = make(map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Org:              b.org,
		Root:             b.root,
		Accounts:         b.accounts,
		OUs:              b.ous,
		Policies:         b.policies,
		PolicyTargets:    b.policyTargets,
		TargetPolicies:   b.targetPolicies,
		AccountParent:    b.accountParent,
		OUParent:         b.ouParent,
		Tags:             b.tags,
		CreateStatuses:   b.createStatuses,
		ServiceAccess:    b.serviceAccess,
		DelegatedAdmins:  b.delegatedAdmins,
		Handshakes:       b.handshakes,
		EmailToAccountID: b.emailToAccountID,
		ResourcePolicy:   b.resourcePolicy,
		AccountID:        b.accountID,
		Region:           b.region,
		AccountCounter:   b.accountCounter,
		StatusCounter:    b.statusCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "organizations: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "organizations", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.org = snap.Org
	b.root = snap.Root
	b.accounts = snap.Accounts
	b.ous = snap.OUs
	b.policies = snap.Policies
	b.policyTargets = snap.PolicyTargets
	b.targetPolicies = snap.TargetPolicies
	b.accountParent = snap.AccountParent
	b.ouParent = snap.OUParent
	b.tags = snap.Tags
	b.createStatuses = snap.CreateStatuses
	b.serviceAccess = snap.ServiceAccess
	b.delegatedAdmins = snap.DelegatedAdmins
	b.handshakes = snap.Handshakes
	b.emailToAccountID = snap.EmailToAccountID
	b.resourcePolicy = snap.ResourcePolicy
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.accountCounter = snap.AccountCounter
	b.statusCounter = snap.StatusCounter

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
