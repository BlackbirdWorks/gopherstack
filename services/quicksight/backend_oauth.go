package quicksight

import (
	"maps"
	"sort"
	"strings"
	"time"
)

// storedOAuthApp is the persisted representation of a QuickSight OAuth client
// application.
type storedOAuthApp struct {
	CreatedTime     time.Time      `json:"createdTime"`
	LastUpdatedTime time.Time      `json:"lastUpdatedTime"`
	Extra           map[string]any `json:"extra,omitempty"`
	ClientID        string         `json:"clientId"`
	Arn             string         `json:"arn"`
	Name            string         `json:"name"`
	Status          string         `json:"status"`
}

func (a *storedOAuthApp) toOAuthClientApplication() *OAuthClientApplication {
	return &OAuthClientApplication{
		CreatedTime:     a.CreatedTime,
		LastUpdatedTime: a.LastUpdatedTime,
		Extra:           a.Extra,
		ClientID:        a.ClientID,
		Arn:             a.Arn,
		Name:            a.Name,
		Status:          a.Status,
	}
}

// ---- OAuth client applications ----

func (b *InMemoryBackend) CreateOAuthClientApplication(
	accountID, clientID, name string,
	fields map[string]any,
) (*OAuthClientApplication, error) {
	if clientID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateOAuthClientApplication")
	defer b.mu.Unlock()

	key := oauthAppKey(accountID, clientID)
	if _, exists := b.oauthClientApps[key]; exists {
		return nil, ErrOAuthClientAppAlreadyExists
	}

	now := time.Now().UTC()
	app := &storedOAuthApp{
		CreatedTime:     now,
		LastUpdatedTime: now,
		ClientID:        clientID,
		Arn:             b.buildARN("application", clientID),
		Name:            name,
		Status:          statusCreationSuccessful,
		Extra:           fields,
	}
	b.oauthClientApps[key] = app

	return app.toOAuthClientApplication(), nil
}

func (b *InMemoryBackend) DescribeOAuthClientApplication(accountID, clientID string) (*OAuthClientApplication, error) {
	b.mu.RLock("DescribeOAuthClientApplication")
	defer b.mu.RUnlock()

	app, ok := b.oauthClientApps[oauthAppKey(accountID, clientID)]
	if !ok {
		return nil, ErrOAuthClientAppNotFound
	}

	return app.toOAuthClientApplication(), nil
}

func (b *InMemoryBackend) UpdateOAuthClientApplication(
	accountID, clientID, name string,
	fields map[string]any,
) (*OAuthClientApplication, error) {
	b.mu.Lock("UpdateOAuthClientApplication")
	defer b.mu.Unlock()

	app, ok := b.oauthClientApps[oauthAppKey(accountID, clientID)]
	if !ok {
		return nil, ErrOAuthClientAppNotFound
	}

	if name != "" {
		app.Name = name
	}
	if len(fields) > 0 {
		if app.Extra == nil {
			app.Extra = make(map[string]any, len(fields))
		}
		maps.Copy(app.Extra, fields)
	}
	app.Status = statusUpdateSuccessful
	app.LastUpdatedTime = time.Now().UTC()

	return app.toOAuthClientApplication(), nil
}

func (b *InMemoryBackend) DeleteOAuthClientApplication(accountID, clientID string) (*OAuthClientApplication, error) {
	b.mu.Lock("DeleteOAuthClientApplication")
	defer b.mu.Unlock()

	key := oauthAppKey(accountID, clientID)
	app, ok := b.oauthClientApps[key]
	if !ok {
		return nil, ErrOAuthClientAppNotFound
	}
	delete(b.oauthClientApps, key)

	return app.toOAuthClientApplication(), nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListOAuthClientApplications(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*OAuthClientApplication, string, error) {
	b.mu.RLock("ListOAuthClientApplications")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*storedOAuthApp, 0, len(b.oauthClientApps))
	for k, app := range b.oauthClientApps {
		if strings.HasPrefix(k, prefix) {
			all = append(all, app)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ClientID < all[j].ClientID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, app := range all {
			if app.ClientID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].ClientID
	} else {
		end = len(all)
	}

	result := make([]*OAuthClientApplication, 0, end-start)
	for _, app := range all[start:end] {
		result = append(result, app.toOAuthClientApplication())
	}

	return result, next, nil
}
