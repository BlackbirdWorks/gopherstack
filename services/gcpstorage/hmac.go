package gcpstorage

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	hmacActive   = "ACTIVE"
	hmacInactive = "INACTIVE"
	hmacDeleted  = "DELETED"

	hmacAccessPrefix  = "GOOG1E"
	hmacAccessLen     = 61
	hmacSecretBytes   = 30
	maxHMACKeysPerSA  = 5
	hmacKeyIDRandSize = hmacAccessLen - len(hmacAccessPrefix)
)

// HMACKey is a Cloud Storage HMAC key (storage#hmacKeyMetadata plus its secret).
type HMACKey struct {
	Created             time.Time `json:"created"`
	Updated             time.Time `json:"updated"`
	AccessID            string    `json:"accessId"`
	Secret              string    `json:"secret"`
	Project             string    `json:"project"`
	ServiceAccountEmail string    `json:"serviceAccountEmail"`
	State               string    `json:"state"`
}

func newAccessID() string {
	raw := make([]byte, hmacKeyIDRandSize)
	_, _ = rand.Read(raw)

	return hmacAccessPrefix + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)[:hmacKeyIDRandSize]
}

func newHMACSecret() string {
	raw := make([]byte, hmacSecretBytes)
	_, _ = rand.Read(raw)

	return base64.StdEncoding.EncodeToString(raw)
}

// CreateHMACKey creates an ACTIVE key for a service account; the returned copy carries the secret.
func (b *InMemoryBackend) CreateHMACKey(project, email string) (*HMACKey, error) {
	if email == "" {
		return nil, errRequired("serviceAccountEmail")
	}

	b.mu.Lock("CreateHMACKey")
	defer b.mu.Unlock()

	n := 0

	for _, k := range b.hmac {
		if k.Project == project && k.ServiceAccountEmail == email && k.State != hmacDeleted {
			n++
		}
	}

	if n >= maxHMACKeysPerSA {
		return nil, errInvalid("The service account has reached the maximum number of HMAC keys (5).")
	}

	now := b.now().UTC()
	k := &HMACKey{
		AccessID: newAccessID(), Secret: newHMACSecret(), Project: project,
		ServiceAccountEmail: email, State: hmacActive, Created: now, Updated: now,
	}
	b.hmac[k.AccessID] = k
	out := *k

	return &out, nil
}

func (b *InMemoryBackend) hmacLocked(project, id string) (*HMACKey, error) {
	k, ok := b.hmac[id]
	if !ok || k.Project != project {
		return nil, errNotFound("HMAC key not found: " + id)
	}

	return k, nil
}

// GetHMACKey returns key metadata (the secret is blanked).
func (b *InMemoryBackend) GetHMACKey(project, id string) (*HMACKey, error) {
	b.mu.RLock("GetHMACKey")
	defer b.mu.RUnlock()

	k, err := b.hmacLocked(project, id)
	if err != nil {
		return nil, err
	}

	out := *k
	out.Secret = ""

	return &out, nil
}

// ListHMACKeys lists a project's keys, optionally for one service account and including DELETED ones.
func (b *InMemoryBackend) ListHMACKeys(
	project, email, token string, limit int, showDeleted bool,
) page.Page[*HMACKey] {
	b.mu.RLock("ListHMACKeys")
	defer b.mu.RUnlock()

	var all []*HMACKey

	for _, k := range b.hmac {
		if k.Project != project || (email != "" && k.ServiceAccountEmail != email) ||
			(k.State == hmacDeleted && !showDeleted) {
			continue
		}

		out := *k
		out.Secret = ""
		all = append(all, &out)
	}

	slices.SortFunc(all, func(x, y *HMACKey) int { return strings.Compare(x.AccessID, y.AccessID) })

	return page.New(all, token, limit, defaultMaxResults)
}

// UpdateHMACKey switches a key between ACTIVE and INACTIVE.
func (b *InMemoryBackend) UpdateHMACKey(project, id, state string) (*HMACKey, error) {
	if state != hmacActive && state != hmacInactive {
		return nil, errInvalid("Invalid HMAC key state '" + state + "'; must be ACTIVE or INACTIVE.")
	}

	b.mu.Lock("UpdateHMACKey")
	defer b.mu.Unlock()

	k, err := b.hmacLocked(project, id)
	if err != nil {
		return nil, err
	}

	if k.State == hmacDeleted {
		return nil, errInvalid("Cannot update a deleted HMAC key.")
	}

	k.State = state
	k.Updated = b.now().UTC()
	out := *k
	out.Secret = ""

	return &out, nil
}

// DeleteHMACKey marks an INACTIVE key DELETED.
func (b *InMemoryBackend) DeleteHMACKey(project, id string) error {
	b.mu.Lock("DeleteHMACKey")
	defer b.mu.Unlock()

	k, err := b.hmacLocked(project, id)
	if err != nil {
		return err
	}

	switch k.State {
	case hmacActive:
		return errInvalid("Cannot delete an ACTIVE HMAC key; set it to INACTIVE first.")
	case hmacDeleted:
		return errNotFound("HMAC key not found: " + id)
	}

	k.State = hmacDeleted
	k.Updated = b.now().UTC()

	return nil
}

func (b *InMemoryBackend) activeHMACSecret(id string) (string, bool) {
	b.mu.RLock("activeHMACSecret")
	defer b.mu.RUnlock()

	k, ok := b.hmac[id]
	if !ok || k.State != hmacActive {
		return "", false
	}

	return k.Secret, true
}

func hmacMetadata(base string, k *HMACKey) map[string]any {
	return map[string]any{
		keyKind: "storage#hmacKeyMetadata", "id": k.Project + "/" + k.AccessID, "accessId": k.AccessID,
		"projectId": k.Project, "serviceAccountEmail": k.ServiceAccountEmail, "state": k.State,
		"timeCreated": fmtTime(k.Created), "updated": fmtTime(k.Updated),
		"etag":     base64.StdEncoding.EncodeToString([]byte(itoa64(k.Updated.UnixMicro()))),
		"selfLink": base + "/storage/v1/projects/" + k.Project + "/hmacKeys/" + k.AccessID,
	}
}

func (h *Handler) createHMACKey(c *echo.Context, rt route) error {
	k, err := h.Backend.CreateHMACKey(rt.project, c.QueryParam("serviceAccountEmail"))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyKind: "storage#hmacKey", "secret": k.Secret, "metadata": hmacMetadata(baseURL(c.Request()), k),
	})
}

func (h *Handler) getHMACKey(c *echo.Context, rt route) error {
	k, err := h.Backend.GetHMACKey(rt.project, rt.entity)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, hmacMetadata(baseURL(c.Request()), k))
}

func (h *Handler) listHMACKeys(c *echo.Context, rt route) error {
	q := c.QueryParams()
	limit, _ := strconv.Atoi(q.Get("maxResults"))
	pg := h.Backend.ListHMACKeys(rt.project, q.Get("serviceAccountEmail"), q.Get("pageToken"), limit,
		q.Get("showDeletedKeys") == "true")
	base := baseURL(c.Request())
	items := make([]map[string]any, 0, len(pg.Data))

	for _, k := range pg.Data {
		items = append(items, hmacMetadata(base, k))
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyKind: "storage#hmacKeysMetadata", "items": items, "nextPageToken": pg.Next,
	})
}

func (h *Handler) updateHMACKey(c *echo.Context, rt route) error {
	p, err := readPatch(c)
	if err != nil {
		return err
	}

	k, err := h.Backend.UpdateHMACKey(rt.project, rt.entity, p.str("state"))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, hmacMetadata(baseURL(c.Request()), k))
}

func (h *Handler) deleteHMACKey(c *echo.Context, rt route) error {
	if err := h.Backend.DeleteHMACKey(rt.project, rt.entity); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}
