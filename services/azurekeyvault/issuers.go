package azurekeyvault

import (
	"net/http"
	"sort"
)

// IssuerCredentials are the issuer account credentials.
type IssuerCredentials struct {
	AccountID string `json:"account_id,omitempty"`
	Password  string `json:"pwd,omitempty"`
}

// AdminDetails is an issuer organisation administrator.
type AdminDetails struct {
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
	Email     string `json:"email_address,omitempty"`
	Phone     string `json:"phone,omitempty"`
}

// OrgDetails are the issuer organisation details.
type OrgDetails struct {
	ID           string         `json:"id,omitempty"`
	AdminDetails []AdminDetails `json:"admin_details,omitempty"`
}

// Issuer is a stored certificate issuer.
type Issuer struct {
	Credentials *IssuerCredentials `json:"credentials,omitempty"`
	OrgDetails  *OrgDetails        `json:"org_details,omitempty"`
	Attributes  *Attributes        `json:"attributes,omitempty"`
	ID          string             `json:"id,omitempty"`
	Provider    string             `json:"provider,omitempty"`
}

// IssuerItem is a list entry.
type IssuerItem struct {
	ID       string `json:"id"`
	Provider string `json:"provider,omitempty"`
}

// Contact is a certificate contact.
type Contact struct {
	Email string `json:"email,omitempty"`
	Name  string `json:"name,omitempty"`
	Phone string `json:"phone,omitempty"`
}

// Contacts is the vault's certificate contact list.
type Contacts struct {
	ID       string    `json:"id,omitempty"`
	Contacts []Contact `json:"contacts"`
}

func (i *Issuer) wire(base, name string) Issuer {
	out := *i
	out.ID = base + "/certificates/issuers/" + name

	if i.Credentials != nil {
		c := *i.Credentials
		c.Password = ""
		out.Credentials = &c
	}

	return out
}

// SetIssuer creates or replaces an issuer.
func (b *InMemoryBackend) SetIssuer(vaultName, base, name string, in *Issuer) (Issuer, error) {
	var out Issuer

	err := b.do(vaultName, func(v *vault, now int64) error {
		if in.Provider == "" {
			return errBadParameter("The issuer provider is required.")
		}

		stored := *in
		enabled := true
		stored.Attributes = &Attributes{Enabled: &enabled, Created: &now, Updated: &now}

		if in.Attributes != nil && in.Attributes.Enabled != nil {
			stored.Attributes.Enabled = in.Attributes.Enabled
		}

		v.Issuers[name] = &stored
		out = stored.wire(base, name)

		return nil
	})

	return out, err
}

// UpdateIssuer merges the supplied fields into an existing issuer.
func (b *InMemoryBackend) UpdateIssuer(vaultName, base, name string, in *Issuer) (Issuer, error) {
	var out Issuer

	err := b.do(vaultName, func(v *vault, now int64) error {
		cur, ok := v.Issuers[name]
		if !ok {
			return newErr(http.StatusNotFound, "CertificateIssuerNotFound", "Issuer not found: %s", name)
		}

		if in.Provider != "" {
			cur.Provider = in.Provider
		}

		if in.Credentials != nil {
			cur.Credentials = in.Credentials
		}

		if in.OrgDetails != nil {
			cur.OrgDetails = in.OrgDetails
		}

		cur.Attributes.merge(in.Attributes)
		cur.Attributes.Updated = &now
		out = cur.wire(base, name)

		return nil
	})

	return out, err
}

// GetIssuer returns an issuer.
func (b *InMemoryBackend) GetIssuer(vaultName, base, name string) (Issuer, error) {
	var out Issuer

	err := b.do(vaultName, func(v *vault, _ int64) error {
		cur, ok := v.Issuers[name]
		if !ok {
			return newErr(http.StatusNotFound, "CertificateIssuerNotFound", "Issuer not found: %s", name)
		}

		out = cur.wire(base, name)

		return nil
	})

	return out, err
}

// DeleteIssuer removes an issuer, returning it.
func (b *InMemoryBackend) DeleteIssuer(vaultName, base, name string) (Issuer, error) {
	var out Issuer

	err := b.do(vaultName, func(v *vault, _ int64) error {
		cur, ok := v.Issuers[name]
		if !ok {
			return newErr(http.StatusNotFound, "CertificateIssuerNotFound", "Issuer not found: %s", name)
		}

		out = cur.wire(base, name)

		delete(v.Issuers, name)

		return nil
	})

	return out, err
}

// ListIssuers lists issuers by name.
func (b *InMemoryBackend) ListIssuers(vaultName, base string) ([]IssuerItem, error) {
	var out []IssuerItem

	err := b.do(vaultName, func(v *vault, _ int64) error {
		names := make([]string, 0, len(v.Issuers))
		for n := range v.Issuers {
			names = append(names, n)
		}

		sort.Strings(names)

		for _, n := range names {
			out = append(out, IssuerItem{ID: base + "/certificates/issuers/" + n, Provider: v.Issuers[n].Provider})
		}

		return nil
	})

	return out, err
}

// SetContacts replaces the vault's contacts.
func (b *InMemoryBackend) SetContacts(vaultName, base string, in *Contacts) (Contacts, error) {
	var out Contacts

	err := b.do(vaultName, func(v *vault, _ int64) error {
		v.Contacts = &Contacts{Contacts: in.Contacts}
		out = Contacts{ID: base + "/certificates/contacts", Contacts: in.Contacts}

		return nil
	})

	return out, err
}

// GetContacts returns the vault's contacts.
func (b *InMemoryBackend) GetContacts(vaultName, base string) (Contacts, error) {
	var out Contacts

	err := b.do(vaultName, func(v *vault, _ int64) error {
		if v.Contacts == nil {
			return newErr(http.StatusNotFound, "ContactsNotFound", "Contacts not found for this vault.")
		}

		out = Contacts{ID: base + "/certificates/contacts", Contacts: v.Contacts.Contacts}

		return nil
	})

	return out, err
}

// DeleteContacts removes the vault's contacts, returning them.
func (b *InMemoryBackend) DeleteContacts(vaultName, base string) (Contacts, error) {
	var out Contacts

	err := b.do(vaultName, func(v *vault, _ int64) error {
		if v.Contacts == nil {
			return newErr(http.StatusNotFound, "ContactsNotFound", "Contacts not found for this vault.")
		}

		out = Contacts{ID: base + "/certificates/contacts", Contacts: v.Contacts.Contacts}
		v.Contacts = nil

		return nil
	})

	return out, err
}
