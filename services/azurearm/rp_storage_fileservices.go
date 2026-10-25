package azurearm

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	fileServicesType     = "fileServices"
	fileServicesDefault  = "default"
	fileServicesListLen  = 9
	fileServicesItemLen  = 10
	storageTypeIdx       = 6
	storageChildTypeIdx  = 8
	storageChildNameIdx  = 9
	maxShareRetentionDay = 365
)

// FileServices is the seam through which the file service's share
// soft-delete policy reaches the Azure Files data plane.
type FileServices interface {
	SetShareRetention(account string, enabled bool, days int32)
	ShareRetention(account string) (bool, int32)
}

// memFileServices is the standalone default: it only remembers the policy.
type memFileServices struct {
	mu       *lockmetrics.RWMutex
	policies map[string]retentionState
}

type retentionState struct {
	days    int32
	enabled bool
}

func newMemFileServices() *memFileServices {
	return &memFileServices{mu: lockmetrics.New("azurearm.memfileservices"), policies: map[string]retentionState{}}
}

func (m *memFileServices) SetShareRetention(account string, enabled bool, days int32) {
	m.mu.Lock("SetShareRetention")
	defer m.mu.Unlock()

	m.policies[strings.ToLower(account)] = retentionState{enabled: enabled, days: days}
}

func (m *memFileServices) ShareRetention(account string) (bool, int32) {
	m.mu.RLock("ShareRetention")
	defer m.mu.RUnlock()

	s := m.policies[strings.ToLower(account)]

	return s.enabled, s.days
}

// SetFileServices gives the Microsoft.Storage provider (if any) a data-plane
// adapter for file service properties.
func (r *Registry) SetFileServices(fs FileServices) {
	p, ok := r.ProviderNamed(namespaceMicrosoftStorage)
	if !ok {
		return
	}

	if sp, spOk := p.(*StorageProvider); spOk {
		sp.fileServices = fs
	}
}

func (p *StorageProvider) ownsAccount(id ResourceID) bool {
	p.mu.RLock("ownsAccount")
	defer p.mu.RUnlock()

	_, ok := p.lookupOwnedAccount(id)

	return ok
}

func fileServiceBody(id ResourceID, enabled bool, days int32) map[string]any {
	policy := map[string]any{"enabled": enabled}
	if enabled {
		policy["days"] = days
	}

	return map[string]any{
		"id":   id.ARMID() + "/" + fileServicesType + "/" + fileServicesDefault,
		"name": fileServicesDefault,
		"type": namespaceMicrosoftStorage + "/" + storageAccountsType + "/" + fileServicesType,
		"sku":  map[string]any{"name": "Standard_LRS", "tier": skuTierStandard},
		"properties": map[string]any{
			"cors":                       map[string]any{"corsRules": []any{}},
			"shareDeleteRetentionPolicy": policy,
		},
	}
}

// parseRetentionPolicy reads properties.shareDeleteRetentionPolicy; ok is
// false when the body does not carry it.
func parseRetentionPolicy(body map[string]any) (enabled bool, days int32, ok bool, err error) {
	props, _ := body[fieldProperties].(map[string]any)

	policy, present := props["shareDeleteRetentionPolicy"].(map[string]any)
	if !present {
		return false, 0, false, nil
	}

	enabled, _ = policy["enabled"].(bool)

	if raw, has := policy["days"].(float64); has {
		days = int32(raw)
	}

	if enabled && (days < 1 || days > maxShareRetentionDay) {
		return false, 0, true, fmt.Errorf(
			"%w: shareDeleteRetentionPolicy.days must be between 1 and %d", ErrInvalidRequestBody, maxShareRetentionDay)
	}

	return enabled, days, true, nil
}

func isStorageFileServiceScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) && segAt(segs, acrGroupsSegIdx, resourceGroupsSegment) &&
		segAt(segs, acrProvidersInGroupSegIdx, providersSegment) &&
		segAt(segs, acrProviderNsIdx, namespaceMicrosoftStorage) && segAt(segs, storageTypeIdx, storageAccountsType) &&
		segAt(segs, storageChildTypeIdx, fileServicesType)
}

//nolint:gochecknoglobals // static route table, read-only after init
var storageFileServiceRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isStorageFileServiceScope(segs, fileServicesListLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFileServices(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			switch r.Method {
			case http.MethodGet, http.MethodPut, http.MethodPatch:
				return isStorageFileServiceScope(segs, fileServicesItemLen)
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFileServices(c, segs) },
	},
}

func (h *Handler) handleFileServices(c *echo.Context, segs []string) error {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftStorage)

	sp, spOk := p.(*StorageProvider)
	if !ok || !spOk {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:storageChildTypeIdx]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if !sp.ownsAccount(id) {
		return h.writeAPIError(c, ErrStorageAccountNotFound)
	}

	if len(segs) == fileServicesItemLen && !strings.EqualFold(segs[storageChildNameIdx], fileServicesDefault) {
		return h.writeAPIError(c, ErrResourceNotFound)
	}

	account := id.LeafName()

	if m := c.Request().Method; m == http.MethodPut || m == http.MethodPatch {
		body, bodyErr := decodeJSONBody(c.Request())
		if bodyErr != nil {
			return h.writeAPIError(c, bodyErr)
		}

		enabled, days, present, polErr := parseRetentionPolicy(body)
		if polErr != nil {
			return h.writeAPIError(c, polErr)
		}

		if present {
			sp.fileServices.SetShareRetention(account, enabled, days)
		}
	}

	enabled, days := sp.fileServices.ShareRetention(account)
	out := fileServiceBody(id, enabled, days)

	if len(segs) == fileServicesListLen {
		return h.writeJSON(c, http.StatusOK, map[string]any{"value": []any{out}})
	}

	return h.writeJSON(c, http.StatusOK, out)
}
