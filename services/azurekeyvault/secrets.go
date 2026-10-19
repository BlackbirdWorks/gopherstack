package azurekeyvault

import (
	"maps"
	"net/http"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const versionIDLen = 32

func newVersionID() string { return awsid.NewN("", versionIDLen) }

// SecretVersion is one stored version of a secret.
type SecretVersion struct {
	Tags        map[string]string `json:"tags,omitempty"`
	ID          string            `json:"id"`
	Value       string            `json:"value"`
	ContentType string            `json:"contentType,omitempty"`
	KID         string            `json:"kid,omitempty"`
	Attrs       Attributes        `json:"attrs"`
	Managed     bool              `json:"managed,omitempty"`
}

// VersionID implements Versioned.
func (s *SecretVersion) VersionID() string { return s.ID }

// Attr implements Versioned.
func (s *SecretVersion) Attr() *Attributes { return &s.Attrs }

// SecretWire is the secret bundle/item/deleted-bundle wire shape.
type SecretWire struct {
	Value              *string           `json:"value,omitempty"`
	Attributes         *Attributes       `json:"attributes,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	DeletedDate        *int64            `json:"deletedDate,omitempty"`
	ScheduledPurgeDate *int64            `json:"scheduledPurgeDate,omitempty"`
	ID                 string            `json:"id"`
	ContentType        string            `json:"contentType,omitempty"`
	KID                string            `json:"kid,omitempty"`
	RecoveryID         string            `json:"recoveryId,omitempty"`
	Managed            bool              `json:"managed,omitempty"`
}

// SetSecretRequest is the PUT /secrets/{name} body.
type SetSecretRequest struct {
	Value       *string           `json:"value"`
	Attributes  *Attributes       `json:"attributes"`
	Tags        map[string]string `json:"tags"`
	ContentType string            `json:"contentType"`
}

// UpdateSecretRequest is the PATCH /secrets/{name}/{version} body.
type UpdateSecretRequest struct {
	Attributes  *Attributes       `json:"attributes"`
	Tags        map[string]string `json:"tags"`
	ContentType *string           `json:"contentType"`
}

func cloneTags(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}

	out := make(map[string]string, len(in))
	maps.Copy(out, in)

	return out
}

func (s *SecretVersion) wire(
	base, name string, withValue bool, e *Entity[*SecretVersion], withVersion bool,
) SecretWire {
	attrs := s.Attrs
	w := SecretWire{
		ID:          base + "/secrets/" + name,
		ContentType: s.ContentType,
		Attributes:  &attrs,
		Tags:        s.Tags,
		KID:         s.KID,
		Managed:     s.Managed,
	}

	if withVersion {
		w.ID += "/" + s.ID
	}

	if withValue {
		val := s.Value
		w.Value = &val
	}

	if e != nil && e.Deleted != nil {
		d := *e.Deleted
		w.DeletedDate = &d.DeletedDate
		w.ScheduledPurgeDate = &d.ScheduledPurgeDate
		w.RecoveryID = base + "/deletedsecrets/" + name
	}

	return w
}

func protectedVault(v *vault) bool { return v.PurgeProtection }

// SetSecret creates a new version of the named secret.
func (b *InMemoryBackend) SetSecret(vaultName, base, name string, req *SetSecretRequest) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		if req.Value == nil {
			return errBadParameter("The request body must contain the value property.")
		}

		e, err := v.Secrets.create(kindSecret, name)
		if err != nil {
			return err
		}

		if l := e.latest(); l != nil && l.Managed {
			return newErr(http.StatusConflict, "Conflict", "Secret %s is managed by a certificate.", name)
		}

		ver := &SecretVersion{
			ID:          newVersionID(),
			Value:       *req.Value,
			ContentType: req.ContentType,
			Tags:        cloneTags(req.Tags),
			Attrs:       newAttributes(req.Attributes, now, v.RetentionDays, protectedVault(v)),
		}
		e.Versions = append(e.Versions, ver)
		out = ver.wire(base, name, true, nil, true)

		return nil
	})

	return out, err
}

func (v *vault) findSecret(name, version string) (*SecretVersion, error) {
	e, ok := v.Secrets.active(name)
	if !ok {
		return nil, errNotFound(kindSecret, name)
	}

	ver, ok := e.version(version)
	if !ok {
		return nil, errNotFound(kindSecret, name+"/"+version)
	}

	return ver, nil
}

// GetSecret returns a secret version (latest when version is empty).
func (b *InMemoryBackend) GetSecret(vaultName, base, name, version string) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		ver, err := v.findSecret(name, version)
		if err != nil {
			return err
		}

		if !ver.Attrs.enabled() {
			return errDisabled(kindSecret)
		}

		out = ver.wire(base, name, true, nil, true)

		return nil
	})

	return out, err
}

// UpdateSecret updates a secret version's mutable properties.
func (b *InMemoryBackend) UpdateSecret(
	vaultName, base, name, version string, req *UpdateSecretRequest,
) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		ver, err := v.findSecret(name, version)
		if err != nil {
			return err
		}

		ver.Attrs.merge(req.Attributes)
		ver.Attrs.Updated = &now

		if req.Tags != nil {
			ver.Tags = cloneTags(req.Tags)
		}

		if req.ContentType != nil {
			ver.ContentType = *req.ContentType
		}

		out = ver.wire(base, name, false, nil, true)

		return nil
	})

	return out, err
}

// DeleteSecret soft-deletes a secret.
func (b *InMemoryBackend) DeleteSecret(vaultName, base, name string) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		if e, ok := v.Secrets.active(name); ok && e.latest() != nil && e.latest().Managed {
			return newErr(
				http.StatusConflict,
				"Conflict",
				"Secret %s is managed by a certificate; delete the certificate.",
				name,
			)
		}

		e, err := v.Secrets.softDelete(kindSecret, name, now, v.RetentionDays)
		if err != nil {
			return err
		}

		out = e.latest().wire(base, name, true, e, true)

		return nil
	})

	return out, err
}

// GetDeletedSecret returns a soft-deleted secret.
func (b *InMemoryBackend) GetDeletedSecret(vaultName, base, name string) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Secrets.deleted(name)
		if !ok {
			return errNotFound(kindSecret, name)
		}

		out = e.latest().wire(base, name, true, e, true)

		return nil
	})

	return out, err
}

// RecoverSecret restores a soft-deleted secret.
func (b *InMemoryBackend) RecoverSecret(vaultName, base, name string) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, err := v.Secrets.recover(kindSecret, name)
		if err != nil {
			return err
		}

		out = e.latest().wire(base, name, false, nil, true)

		return nil
	})

	return out, err
}

// PurgeSecret permanently removes a soft-deleted secret.
func (b *InMemoryBackend) PurgeSecret(vaultName, name string) error {
	return b.do(vaultName, func(v *vault, _ int64) error {
		return v.Secrets.purge(kindSecret, name, v.PurgeProtection)
	})
}

// ListSecrets lists secrets (latest version identifiers), deleted or active.
func (b *InMemoryBackend) ListSecrets(vaultName, base string, deleted bool) ([]SecretWire, error) {
	var out []SecretWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		for _, n := range v.Secrets.names(deleted) {
			e := v.Secrets[n]
			if deleted && e.latest().Managed {
				continue
			}

			out = append(out, e.latest().wire(base, n, false, e, false))
		}

		return nil
	})

	return out, err
}

// ListSecretVersions lists every version of a secret.
func (b *InMemoryBackend) ListSecretVersions(vaultName, base, name string) ([]SecretWire, error) {
	var out []SecretWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Secrets.active(name)
		if !ok {
			return errNotFound(kindSecret, name)
		}

		versions := append([]*SecretVersion(nil), e.Versions...)
		sort.SliceStable(versions, func(i, j int) bool {
			return *versions[i].Attrs.Created < *versions[j].Attrs.Created
		})

		for _, ver := range versions {
			out = append(out, ver.wire(base, name, false, nil, true))
		}

		return nil
	})

	return out, err
}
