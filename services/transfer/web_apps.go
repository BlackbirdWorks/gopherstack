package transfer

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreateWebApp creates a Transfer web application.
func (b *InMemoryBackend) CreateWebApp(tags map[string]string) (*WebApp, error) {
	b.mu.Lock("CreateWebApp")
	defer b.mu.Unlock()

	webAppID := "webapp-" + uuid.NewString()[:20]

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	w := &WebApp{
		WebAppID:  webAppID,
		CreatedAt: time.Now(),
		Tags:      merged,
		AccountID: b.accountID,
		Region:    b.region,
	}
	b.webApps.Put(w)
	b.initTagsStore(webAppARN(b.accountID, b.region, webAppID), merged)

	return cloneWebApp(w), nil
}

// DeleteWebApp removes a web app by ID.
func (b *InMemoryBackend) DeleteWebApp(webAppID string) error {
	b.mu.Lock("DeleteWebApp")
	defer b.mu.Unlock()

	if !b.webApps.Has(webAppID) {
		return fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	b.webApps.Delete(webAppID)

	return nil
}

// DescribeWebApp returns a web app by ID.
func (b *InMemoryBackend) DescribeWebApp(webAppID string) (*WebApp, error) {
	b.mu.RLock("DescribeWebApp")
	defer b.mu.RUnlock()

	w, ok := b.webApps.Get(webAppID)
	if !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	return cloneWebApp(w), nil
}

// ListWebApps returns all web apps sorted by webAppID.
func (b *InMemoryBackend) ListWebApps() []*WebApp {
	b.mu.RLock("ListWebApps")
	defer b.mu.RUnlock()

	all := b.webApps.All()
	out := make([]*WebApp, 0, len(all))

	for _, w := range all {
		out = append(out, cloneWebApp(w))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].WebAppID < out[j].WebAppID
	})

	return out
}

// UpdateWebApp updates mutable fields on a web app.
func (b *InMemoryBackend) UpdateWebApp(
	webAppID string,
	identityProviderDetails *WebAppIdentityProviderDetails,
) (*WebApp, error) {
	b.mu.Lock("UpdateWebApp")
	defer b.mu.Unlock()

	w, ok := b.webApps.Get(webAppID)
	if !ok {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	if identityProviderDetails != nil {
		w.IdentityProviderDetails = identityProviderDetails
	}

	return cloneWebApp(w), nil
}

// DescribeWebAppCustomization returns the customization for a web app.
// Returns empty customization (not an error) when none has been set.
func (b *InMemoryBackend) DescribeWebAppCustomization(webAppID string) (*WebAppCustomization, error) {
	b.mu.RLock("DescribeWebAppCustomization")
	defer b.mu.RUnlock()

	if !b.webApps.Has(webAppID) {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	if c, ok := b.webAppCustomizations.Get(webAppID); ok {
		cp := *c

		return &cp, nil
	}

	return &WebAppCustomization{WebAppID: webAppID}, nil
}

// UpdateWebAppCustomization sets or overwrites the customization for a web app.
func (b *InMemoryBackend) UpdateWebAppCustomization(
	webAppID, title, logoFile, faviconFile string,
) (*WebAppCustomization, error) {
	b.mu.Lock("UpdateWebAppCustomization")
	defer b.mu.Unlock()

	if !b.webApps.Has(webAppID) {
		return nil, fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	c := &WebAppCustomization{
		WebAppID:    webAppID,
		Title:       title,
		LogoFile:    logoFile,
		FaviconFile: faviconFile,
	}
	b.webAppCustomizations.Put(c)

	cp := *c

	return &cp, nil
}

// DeleteWebAppCustomization clears the customization for a web app.
func (b *InMemoryBackend) DeleteWebAppCustomization(webAppID string) error {
	b.mu.Lock("DeleteWebAppCustomization")
	defer b.mu.Unlock()

	if !b.webApps.Has(webAppID) {
		return fmt.Errorf("%w: web app %s not found", ErrWebAppNotFound, webAppID)
	}

	b.webAppCustomizations.Delete(webAppID)

	return nil
}

// AddWebAppInternal seeds a web app for testing purposes.
func (b *InMemoryBackend) AddWebAppInternal(webAppID string) {
	b.mu.Lock("AddWebAppInternal")
	defer b.mu.Unlock()

	b.webApps.Put(&WebApp{
		WebAppID:  webAppID,
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
		AccountID: b.accountID,
		Region:    b.region,
	})
}
