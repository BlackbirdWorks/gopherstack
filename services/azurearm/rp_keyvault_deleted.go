package azurearm

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	kvFieldEnableSoftDelete = "enableSoftDelete"
	kvFieldNameAvailable    = "nameAvailable"
	fieldMessage            = "message"
	kvLocationsIdx          = 4
	kvDeletedVaultsIdx      = 6
	kvHoursPerDay           = 24
	kvNameMinLen            = 3
	kvNameMaxLen            = 24
	kvReasonInvalid         = "AccountNameInvalid"
	kvReasonAlreadyExists   = "AlreadyExists"
	kvNameInvalidMessage    = "Vault name must be between 3-24 alphanumeric characters. " +
		"The name must begin with a letter, end with a letter or digit, and not contain consecutive hyphens."
)

var kvNameRe = regexp.MustCompile(`^[A-Za-z](?:[A-Za-z0-9]|-[A-Za-z0-9])*$`)

type deletedKeyVault struct {
	deletionDate   time.Time
	scheduledPurge time.Time
	vault          storedKeyVault
}

func (p *KeyVaultProvider) softDeleteEnabled(v *storedKeyVault) bool {
	enabled, ok := v.properties[kvFieldEnableSoftDelete].(bool)

	return !ok || enabled
}

// retireLocked removes v from the live vaults and, when soft-delete is on,
// records it as deleted. The caller holds p.mu.
func (p *KeyVaultProvider) retireLocked(v *storedKeyVault) bool {
	key := strings.ToLower(v.name)
	delete(p.vaults, key)

	if !p.softDeleteEnabled(v) {
		return false
	}

	days := intProp(v.properties, "softDeleteRetentionInDays", kvDefaultRetentionDays)
	now := p.now().UTC()
	p.deleted[key] = &deletedKeyVault{
		vault:          *v,
		deletionDate:   now,
		scheduledPurge: now.Add(time.Duration(days) * kvHoursPerDay * time.Hour),
	}

	return true
}

func (p *KeyVaultProvider) releaseDataPlane(name string, soft bool) {
	if soft {
		p.dataPlane.SoftDeleteVault(name)

		return
	}

	p.dataPlane.DeleteVault(name)
}

func deletedVaultID(sub, location, name string) string {
	return "/subscriptions/" + sub + "/providers/" + namespaceMicrosoftKeyVault +
		"/locations/" + location + "/" + kvDeletedVaultsType + "/" + name
}

func (d *deletedKeyVault) body() map[string]any {
	v := &d.vault
	protect, _ := v.properties["enablePurgeProtection"].(bool)

	return map[string]any{
		"id":      deletedVaultID(v.subscription, v.location, v.name),
		fieldName: v.name,
		fieldType: namespaceMicrosoftKeyVault + "/" + kvDeletedVaultsType,
		fieldProperties: map[string]any{
			"vaultId": "/subscriptions/" + v.subscription + "/resourceGroups/" + v.resourceGroup +
				"/providers/" + namespaceMicrosoftKeyVault + "/" + kvVaultsType + "/" + v.name,
			fieldLocation:            v.location,
			"deletionDate":           d.deletionDate.Format(time.RFC3339),
			"scheduledPurgeDate":     d.scheduledPurge.Format(time.RFC3339),
			fieldTags:                tagsOrEmpty(v.tags),
			"purgeProtectionEnabled": protect,
		},
	}
}

func (p *KeyVaultProvider) listDeleted(subscription string) []map[string]any {
	p.mu.RLock("listDeleted")
	defer p.mu.RUnlock()

	found := make([]*deletedKeyVault, 0, len(p.deleted))

	for _, d := range p.deleted {
		if subscription == "" || strings.EqualFold(d.vault.subscription, subscription) {
			found = append(found, d)
		}
	}

	sort.Slice(found, func(i, j int) bool { return found[i].vault.name < found[j].vault.name })

	out := make([]map[string]any, 0, len(found))
	for _, d := range found {
		out = append(out, d.body())
	}

	return out
}

func (p *KeyVaultProvider) deletedAt(sub, location, name string) (*deletedKeyVault, bool) {
	d, ok := p.deleted[strings.ToLower(name)]
	if !ok || !strings.EqualFold(d.vault.location, location) || !strings.EqualFold(d.vault.subscription, sub) {
		return nil, false
	}

	return d, true
}

// GetDeleted returns the wire body of a soft-deleted vault.
func (p *KeyVaultProvider) GetDeleted(sub, location, name string) (map[string]any, error) {
	p.mu.RLock("GetDeleted")
	defer p.mu.RUnlock()

	d, ok := p.deletedAt(sub, location, name)
	if !ok {
		return nil, ErrDeletedVaultNotFound
	}

	return d.body(), nil
}

// PurgeDeleted permanently removes a soft-deleted vault, unless purge
// protection is enabled.
func (p *KeyVaultProvider) PurgeDeleted(sub, location, name string) error {
	p.mu.Lock("PurgeDeleted")

	d, ok := p.deletedAt(sub, location, name)
	if !ok {
		p.mu.Unlock()

		return ErrDeletedVaultNotFound
	}

	if protect, _ := d.vault.properties["enablePurgeProtection"].(bool); protect {
		p.mu.Unlock()

		return ErrVaultPurgeProtected
	}

	delete(p.deleted, strings.ToLower(name))
	p.mu.Unlock()

	p.dataPlane.PurgeVault(d.vault.name)

	return nil
}

// CheckNameAvailability implements the checkNameAvailability action.
func (p *KeyVaultProvider) CheckNameAvailability(name string) map[string]any {
	if len(name) < kvNameMinLen || len(name) > kvNameMaxLen || !kvNameRe.MatchString(name) {
		return map[string]any{
			kvFieldNameAvailable: false, "reason": kvReasonInvalid, fieldMessage: kvNameInvalidMessage,
		}
	}

	key := strings.ToLower(name)

	p.mu.RLock("CheckNameAvailability")
	_, live := p.vaults[key]
	_, gone := p.deleted[key]
	p.mu.RUnlock()

	if live || gone {
		return map[string]any{
			kvFieldNameAvailable: false,
			"reason":             kvReasonAlreadyExists,
			fieldMessage:         fmt.Sprintf("The vault name '%s' is already in use.", name),
		}
	}

	return map[string]any{kvFieldNameAvailable: true}
}

const (
	kvCheckNameSegs    = 5
	kvDeletedGetSegs   = 8
	kvDeletedPurgeSegs = 9
	kvOperationSegs    = 8

	kvOperationResultsType = "operationResults"
)

func isKeyVaultScope(segs []string) bool {
	return isSubscriptionScoped(segs) && segAt(segs, providersOrResourceGroupsIdx, providersSegment) &&
		strings.EqualFold(segs[3], namespaceMicrosoftKeyVault)
}

func isDeletedVaultPath(segs []string, n int) bool {
	return len(segs) >= n && isKeyVaultScope(segs) && segAt(segs, kvLocationsIdx, "locations") &&
		segAt(segs, kvDeletedVaultsIdx, kvDeletedVaultsType)
}

//nolint:gochecknoglobals // static route table, read-only after init
var keyVaultRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == kvCheckNameSegs && r.Method == http.MethodPost && isKeyVaultScope(segs) &&
				strings.EqualFold(segs[4], "checkNameAvailability")
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleKVCheckName(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == kvDeletedGetSegs && r.Method == http.MethodGet &&
				isDeletedVaultPath(segs, kvDeletedGetSegs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleKVGetDeleted(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == kvDeletedPurgeSegs && r.Method == http.MethodPost &&
				isDeletedVaultPath(segs, kvDeletedPurgeSegs) && strings.EqualFold(segs[8], "purge")
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleKVPurge(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == kvOperationSegs && r.Method == http.MethodGet && isKeyVaultScope(segs) &&
				segAt(segs, kvLocationsIdx, "locations") && segAt(segs, kvDeletedVaultsIdx, kvOperationResultsType)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleKVOperationResult(c) },
	},
}

func (h *Handler) keyVaultProvider() (*KeyVaultProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftKeyVault)
	if !ok {
		return nil, false
	}

	kv, ok := p.(*KeyVaultProvider)

	return kv, ok
}

func (h *Handler) handleKVCheckName(c *echo.Context) error {
	kv, ok := h.keyVaultProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	name, _ := body[fieldName].(string)
	typ, _ := body[fieldType].(string)

	if typ != "" && !strings.EqualFold(typ, namespaceMicrosoftKeyVault+"/"+kvVaultsType) {
		return h.writeAPIError(c, fmt.Errorf("%w: unsupported type %q", ErrInvalidRequestBody, typ))
	}

	return h.writeJSON(c, http.StatusOK, kv.CheckNameAvailability(name))
}

func (h *Handler) handleKVGetDeleted(c *echo.Context, segs []string) error {
	kv, ok := h.keyVaultProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	out, err := kv.GetDeleted(segs[1], segs[5], segs[7])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleKVPurge(c *echo.Context, segs []string) error {
	kv, ok := h.keyVaultProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	if err := kv.PurgeDeleted(segs[1], segs[5], segs[7]); err != nil {
		return h.writeAPIError(c, err)
	}

	loc := h.baseURLFor(c.Request()) + "/subscriptions/" + segs[1] + "/providers/" + namespaceMicrosoftKeyVault +
		"/locations/" + segs[5] + "/" + kvOperationResultsType + "/purge-" + segs[7]
	c.Response().Header().Set("Location", loc)
	c.Response().Header().Set("Retry-After", "1")

	return h.writeJSON(c, http.StatusAccepted, map[string]any{})
}

// handleKVOperationResult completes the purge long-running operation polled
// via the Location header handleKVPurge returns.
func (h *Handler) handleKVOperationResult(c *echo.Context) error {
	return h.writeJSON(c, http.StatusOK, map[string]any{sbPropStatus: "Succeeded"})
}
