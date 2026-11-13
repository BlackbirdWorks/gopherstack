package azuregraph

import (
	"encoding/json/jsontext"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

const (
	maxFederatedCredentials = 20
	maxFICText              = 600
	maxFICName              = 120
	codeConflict            = "Request_MultipleObjectsWithSameKeyValue"
)

var ficName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

// FederatedCredential is an application's federatedIdentityCredential.
type FederatedCredential struct {
	Description *string  `json:"description"`
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Issuer      string   `json:"issuer"`
	Subject     string   `json:"subject"`
	Audiences   []string `json:"audiences"`
}

func (*FederatedCredential) normalize() {}

func conflict(format string, a ...any) *Error {
	return newErr(http.StatusConflict, codeConflict, format, a...)
}

func ficReadOnly() []string { return []string{"id"} }

func (b *Backend) ficHolder(appID string) (string, error) {
	if !isGUID(appID) {
		return "", badRequest("Invalid object identifier '%s'.", appID)
	}

	appID = lc(appID)
	if _, ok := b.apps.get(appID); !ok {
		return "", notFound(appID)
	}

	return appID, nil
}

func validateFIC(f *FederatedCredential) error {
	switch {
	case len(f.Name) < 1 || len(f.Name) > maxFICName || !ficName.MatchString(f.Name):
		return badRequest("Invalid value specified for property 'name' of resource 'FederatedIdentityCredential'.")
	case f.Issuer == "" || len(f.Issuer) > maxFICText:
		return badRequest("Invalid value specified for property 'issuer' of resource 'FederatedIdentityCredential'.")
	case f.Subject == "" || len(f.Subject) > maxFICText:
		return badRequest("Invalid value specified for property 'subject' of resource 'FederatedIdentityCredential'.")
	case len(f.Audiences) != 1 || f.Audiences[0] == "" || len(f.Audiences[0]) > maxFICText:
		return badRequest("Invalid value specified for property 'audiences' of resource 'FederatedIdentityCredential'.")
	case f.Description != nil && len(*f.Description) > maxFICText:
		return badRequest(
			"Invalid value specified for property 'description' of resource 'FederatedIdentityCredential'.",
		)
	}

	return nil
}

func (b *Backend) ficUnique(appID string, f *FederatedCredential) error {
	for _, o := range b.fics[appID] {
		if o.ID == f.ID {
			continue
		}

		if strings.EqualFold(o.Name, f.Name) {
			return conflict("A federated identity credential with the name '%s' already exists.", f.Name)
		}

		if o.Issuer == f.Issuer && o.Subject == f.Subject {
			return conflict("A federated identity credential with the same issuer and subject already exists.")
		}
	}

	return nil
}

// CreateFederatedCredential adds a federated identity credential to an application.
func (b *Backend) CreateFederatedCredential(
	appID string,
	body map[string]jsontext.Value,
) (FederatedCredential, error) {
	b.mu.Lock("CreateFederatedCredential")
	defer b.mu.Unlock()

	id, err := b.ficHolder(appID)
	if err != nil {
		return FederatedCredential{}, err
	}

	if err = requireProps("FederatedIdentityCredential", body, "name", "issuer", "subject", "audiences"); err != nil {
		return FederatedCredential{}, err
	}

	f, err := applyBody(&FederatedCredential{ID: newID(), Audiences: []string{}}, body, ficReadOnly()...)
	if err != nil {
		return FederatedCredential{}, err
	}

	if err = validateFIC(f); err != nil {
		return FederatedCredential{}, err
	}

	if len(b.fics[id]) >= maxFederatedCredentials {
		return FederatedCredential{}, badRequest(
			"The maximum number of federated identity credentials (%d) for an application has been reached.",
			maxFederatedCredentials)
	}

	if err = b.ficUnique(id, f); err != nil {
		return FederatedCredential{}, err
	}

	b.fics[id] = append(slices.Clone(b.fics[id]), *f)

	return *f, nil
}

func (b *Backend) findFIC(appID, key string) (string, int, error) {
	id, err := b.ficHolder(appID)
	if err != nil {
		return "", 0, err
	}

	i := slices.IndexFunc(b.fics[id], func(f FederatedCredential) bool {
		return strings.EqualFold(f.ID, key) || f.Name == key
	})
	if i < 0 {
		return "", 0, notFound(key)
	}

	return id, i, nil
}

// ListFederatedCredentials lists an application's federated identity credentials.
func (b *Backend) ListFederatedCredentials(appID string) ([]FederatedCredential, error) {
	b.mu.RLock("ListFederatedCredentials")
	defer b.mu.RUnlock()

	id, err := b.ficHolder(appID)
	if err != nil {
		return nil, err
	}

	return slices.Clone(nonNil(b.fics[id])), nil
}

// GetFederatedCredential returns one credential by ID or name.
func (b *Backend) GetFederatedCredential(appID, key string) (FederatedCredential, error) {
	b.mu.RLock("GetFederatedCredential")
	defer b.mu.RUnlock()

	id, i, err := b.findFIC(appID, key)
	if err != nil {
		return FederatedCredential{}, err
	}

	return b.fics[id][i], nil
}

// UpdateFederatedCredential patches a credential; its name is immutable.
func (b *Backend) UpdateFederatedCredential(appID, key string, body map[string]jsontext.Value) error {
	b.mu.Lock("UpdateFederatedCredential")
	defer b.mu.Unlock()

	id, i, err := b.findFIC(appID, key)
	if err != nil {
		return err
	}

	cur := b.fics[id][i]

	next, err := applyBody(&cur, body, ficReadOnly()...)
	if err != nil {
		return err
	}

	if next.Name != cur.Name {
		return badRequest("Property 'name' of resource 'FederatedIdentityCredential' is immutable.")
	}

	if err = validateFIC(next); err != nil {
		return err
	}

	if err = b.ficUnique(id, next); err != nil {
		return err
	}

	list := slices.Clone(b.fics[id])
	list[i] = *next
	b.fics[id] = list

	return nil
}

// DeleteFederatedCredential removes a credential.
func (b *Backend) DeleteFederatedCredential(appID, key string) error {
	b.mu.Lock("DeleteFederatedCredential")
	defer b.mu.Unlock()

	id, i, err := b.findFIC(appID, key)
	if err != nil {
		return err
	}

	b.fics[id] = slices.Delete(slices.Clone(b.fics[id]), i, i+1)

	return nil
}
