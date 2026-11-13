package ocivault

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxDisplayName = 255

// CreateVaultIn is the CreateVault request body.
type CreateVaultIn struct {
	RetryToken    string                    `json:"-"`
	FreeformTags  map[string]string         `json:"freeformTags"`
	DefinedTags   map[string]map[string]any `json:"definedTags"`
	CompartmentID string                    `json:"compartmentId"`
	DisplayName   string                    `json:"displayName"`
	VaultType     string                    `json:"vaultType"`
}

// UpdateVaultIn is the UpdateVault request body.
type UpdateVaultIn struct {
	DisplayName  *string                   `json:"displayName"`
	FreeformTags map[string]string         `json:"freeformTags"`
	DefinedTags  map[string]map[string]any `json:"definedTags"`
}

func (b *InMemoryBackend) newPrefix() string {
	for {
		p := awsid.NewN("", prefixLen)
		dup := false

		for _, v := range b.vaults {
			dup = dup || v.Prefix == p
		}

		if !dup {
			return p
		}
	}
}

func validateDisplayName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("displayName is required")
	}

	if len(name) > maxDisplayName {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

// CreateVault creates a vault that is CREATING until the transition delay elapses.
func (b *InMemoryBackend) CreateVault(in CreateVaultIn, hostPort string) (vaultWire, string, error) {
	if in.CompartmentID == "" {
		return vaultWire{}, "", errMissing("compartmentId is required")
	}

	if aerr := validateDisplayName(in.DisplayName); aerr != nil {
		return vaultWire{}, "", aerr
	}

	if in.VaultType != "DEFAULT" && in.VaultType != vaultTypePrivate {
		return vaultWire{}, "", errParam("vaultType must be DEFAULT or VIRTUAL_PRIVATE")
	}

	b.mu.Lock("CreateVault")
	defer b.mu.Unlock()

	if id, ok := b.retry.Lookup("CreateVault", in.RetryToken, b.nowUTC()); ok {
		v, live := b.vaults[id]
		if !live || v.State == stateDeleted {
			return vaultWire{}, "", errInvalidatedToken()
		}

		return vaultView(v, hostPort), v.Etag, nil
	}

	if aerr := b.checkTags(in.FreeformTags, in.DefinedTags); aerr != nil {
		return vaultWire{}, "", aerr
	}

	prefix := b.newPrefix()
	v := &Vault{
		ID: newOCID("vault", prefix), Prefix: prefix, CompartmentID: in.CompartmentID, DisplayName: in.DisplayName,
		VaultType: in.VaultType, WrappingKeyID: newOCID("key", prefix),
		TimeCreated: b.nowUTC(), FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags,
		Etag: newEtag(), Seq: b.nextSeq(),
	}
	b.begin(&v.State, &v.transition, stateCreating, stateActive)
	b.vaults[v.ID] = v
	b.retry.Remember("CreateVault", in.RetryToken, v.ID, v.TimeCreated)

	return vaultView(v, hostPort), v.Etag, nil
}

// GetVault returns one vault.
func (b *InMemoryBackend) GetVault(id, hostPort string) (vaultWire, string, error) {
	b.mu.RLock("GetVault")
	defer b.mu.RUnlock()

	v, aerr := b.vaultByID(id)
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	return vaultView(v, hostPort), v.Etag, nil
}

// ListVaults lists the vaults of a compartment.
func (b *InMemoryBackend) ListVaults(compartment, hostPort string, o ListOpts) (page.Page[vaultSummaryWire], error) {
	if compartment == "" {
		return page.Page[vaultSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListVaults")
	defer b.mu.RUnlock()

	var all []*Vault

	for _, v := range b.vaults {
		if v.CompartmentID == compartment {
			all = append(all, v)
		}
	}

	pg := paginate(all, o, func(v *Vault) string { return v.DisplayName },
		func(v *Vault) (time.Time, int64) { return v.TimeCreated, v.Seq })

	out := make([]vaultSummaryWire, 0, len(pg.Data))
	for _, v := range pg.Data {
		out = append(out, vaultSummary(v, hostPort))
	}

	return page.Page[vaultSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateVault changes display name and tags.
func (b *InMemoryBackend) UpdateVault(id, ifMatch, hostPort string, in UpdateVaultIn) (vaultWire, string, error) {
	b.mu.Lock("UpdateVault")
	defer b.mu.Unlock()

	v, aerr := b.activeVault(id)
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	if aerr = checkEtag(v.Etag, ifMatch); aerr != nil {
		return vaultWire{}, "", aerr
	}

	if aerr = b.checkTags(in.FreeformTags, in.DefinedTags); aerr != nil {
		return vaultWire{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validateDisplayName(*in.DisplayName); aerr != nil {
			return vaultWire{}, "", aerr
		}

		v.DisplayName = *in.DisplayName
	}

	if in.FreeformTags != nil {
		v.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		v.DefinedTags = in.DefinedTags
	}

	v.Etag = newEtag()

	return vaultView(v, hostPort), v.Etag, nil
}

// ScheduleVaultDeletion moves an ACTIVE vault to PENDING_DELETION.
func (b *InMemoryBackend) ScheduleVaultDeletion(
	id, ifMatch, hostPort, token string,
	at *time.Time,
) (vaultWire, string, error) {
	b.mu.Lock("ScheduleVaultDeletion")
	defer b.mu.Unlock()

	scope := "ScheduleVaultDeletion/" + id
	if _, ok := b.retrySeen(scope, token); ok {
		return b.replayVault(id, hostPort)
	}

	v, aerr := b.activeVault(id)
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	if aerr = checkEtag(v.Etag, ifMatch); aerr != nil {
		return vaultWire{}, "", aerr
	}

	if v.hasReplicas() {
		return vaultWire{}, "", errState("Vault " + id + " has replicas; delete them before scheduling deletion")
	}

	t, aerr := b.deletionTime(at, kmsMinDeletion)
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	v.TimeOfDeletion, v.Etag = &t, newEtag()
	b.begin(&v.State, &v.transition, stateSchedulingDelete, statePendingDeletion)
	b.retryNote(scope, token, "")

	return vaultView(v, hostPort), v.Etag, nil
}

// CancelVaultDeletion returns a PENDING_DELETION vault to ACTIVE.
func (b *InMemoryBackend) CancelVaultDeletion(id, ifMatch, hostPort, token string) (vaultWire, string, error) {
	b.mu.Lock("CancelVaultDeletion")
	defer b.mu.Unlock()

	scope := "CancelVaultDeletion/" + id
	if _, ok := b.retrySeen(scope, token); ok {
		return b.replayVault(id, hostPort)
	}

	v, aerr := b.vaultByID(id)
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	if v.State != statePendingDeletion {
		return vaultWire{}, "", errState("Vault " + id + " is not pending deletion")
	}

	if aerr = checkEtag(v.Etag, ifMatch); aerr != nil {
		return vaultWire{}, "", aerr
	}

	v.TimeOfDeletion, v.Etag = nil, newEtag()
	b.begin(&v.State, &v.transition, stateCancellingDelete, stateActive)
	b.retryNote(scope, token, "")

	return vaultView(v, hostPort), v.Etag, nil
}

// ChangeVaultCompartment moves a vault; its keys and secrets follow.
func (b *InMemoryBackend) ChangeVaultCompartment(id, ifMatch, compartment, token string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeVaultCompartment")
	defer b.mu.Unlock()

	scope := "ChangeVaultCompartment/" + id
	if _, ok := b.retrySeen(scope, token); ok {
		return nil
	}

	v, aerr := b.activeVault(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(v.Etag, ifMatch); aerr != nil {
		return aerr
	}

	v.CompartmentID, v.Etag = compartment, newEtag()
	b.retryNote(scope, token, "")

	return nil
}

type vaultUsageWire struct {
	KeyCount                int `json:"keyCount"`
	KeyVersionCount         int `json:"keyVersionCount"`
	SoftwareKeyCount        int `json:"softwareKeyCount"`
	SoftwareKeyVersionCount int `json:"softwareKeyVersionCount"`
}

// GetVaultUsage counts the keys and key versions of a vault.
func (b *InMemoryBackend) GetVaultUsage(id string) (vaultUsageWire, error) {
	b.mu.RLock("GetVaultUsage")
	defer b.mu.RUnlock()

	if _, aerr := b.vaultByID(id); aerr != nil {
		return vaultUsageWire{}, aerr
	}

	var u vaultUsageWire

	for _, k := range b.keys {
		if k.VaultID != id || k.State == stateDeleted {
			continue
		}

		u.KeyCount++
		u.KeyVersionCount += len(k.Versions)

		if k.ProtectionMode == "SOFTWARE" {
			u.SoftwareKeyCount++
			u.SoftwareKeyVersionCount += len(k.Versions)
		}
	}

	return u, nil
}
