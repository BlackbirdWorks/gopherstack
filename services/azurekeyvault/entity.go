package azurekeyvault

import (
	"net/http"
	"sort"
)

const (
	kindSecret      = "Secret"
	kindKey         = "Key"
	kindCertificate = "Certificate"

	defaultRetentionDays = 90
	secondsPerDay        = 86400
	recoveryPurgeable    = "Recoverable+Purgeable"
	recoveryProtected    = "Recoverable+ProtectedSubscription"
)

// Attributes is the common attributes object (epoch-second timestamps).
type Attributes struct {
	Enabled         *bool  `json:"enabled,omitempty"`
	NBF             *int64 `json:"nbf,omitempty"`
	Exp             *int64 `json:"exp,omitempty"`
	Created         *int64 `json:"created,omitempty"`
	Updated         *int64 `json:"updated,omitempty"`
	RecoverableDays *int   `json:"recoverableDays,omitempty"`
	RecoveryLevel   string `json:"recoveryLevel,omitempty"`
}

func (a *Attributes) enabled() bool { return a == nil || a.Enabled == nil || *a.Enabled }

// merge applies the writable fields of in onto a.
func (a *Attributes) merge(in *Attributes) {
	if in == nil {
		return
	}

	if in.Enabled != nil {
		a.Enabled = in.Enabled
	}

	if in.NBF != nil {
		a.NBF = in.NBF
	}

	if in.Exp != nil {
		a.Exp = in.Exp
	}
}

// newAttributes builds attributes for a fresh version created at now.
func newAttributes(in *Attributes, now int64, retention int, protected bool) Attributes {
	enabled := true
	out := Attributes{Enabled: &enabled, Created: &now, Updated: &now}
	out.merge(in)
	out.stampRecovery(retention, protected)

	return out
}

func (a *Attributes) stampRecovery(retention int, protected bool) {
	a.RecoverableDays = &retention
	a.RecoveryLevel = recoveryPurgeable

	if protected {
		a.RecoveryLevel = recoveryProtected
	}
}

// Versioned is implemented by the per-kind version records.
type Versioned interface {
	comparable
	VersionID() string
	Attr() *Attributes
}

// DeletedInfo marks an entity as soft-deleted.
type DeletedInfo struct {
	DeletedDate        int64 `json:"deletedDate"`
	ScheduledPurgeDate int64 `json:"scheduledPurgeDate"`
}

// Entity is a named, versioned object (secret, key or certificate).
type Entity[P Versioned] struct {
	Deleted    *DeletedInfo    `json:"deleted,omitempty"`
	Rotation   *RotationPolicy `json:"rotation,omitempty"`
	CertPolicy *CertPolicy     `json:"certPolicy,omitempty"`
	CertOp     *CertOperation  `json:"certOp,omitempty"`
	Name       string          `json:"name"`
	Versions   []P             `json:"versions"`
}

func (e *Entity[P]) latest() P {
	var zero P
	if len(e.Versions) == 0 {
		return zero
	}

	return e.Versions[len(e.Versions)-1]
}

func (e *Entity[P]) version(id string) (P, bool) {
	var zero P

	if id == "" {
		l := e.latest()

		return l, l != zero
	}

	for _, v := range e.Versions {
		if v.VersionID() == id {
			return v, true
		}
	}

	return zero, false
}

// Collection is a vault's name-keyed set of entities of one kind.
type Collection[P Versioned] map[string]*Entity[P]

func (c Collection[P]) active(name string) (*Entity[P], bool) {
	e, ok := c[name]
	if !ok || e.Deleted != nil {
		return nil, false
	}

	return e, true
}

func (c Collection[P]) deleted(name string) (*Entity[P], bool) {
	e, ok := c[name]
	if !ok || e.Deleted == nil {
		return nil, false
	}

	return e, true
}

// sweep drops entities whose scheduled purge date has passed.
func (c Collection[P]) sweep(now int64) {
	for name, e := range c {
		if e.Deleted != nil && e.Deleted.ScheduledPurgeDate <= now {
			delete(c, name)
		}
	}
}

// create returns the entity to add a version to, creating it when absent.
func (c Collection[P]) create(kind, name string) (*Entity[P], error) {
	e, ok := c[name]
	if !ok {
		e = &Entity[P]{Name: name}
		c[name] = e

		return e, nil
	}

	if e.Deleted != nil {
		return nil, errDeletedConflict(kind, name)
	}

	return e, nil
}

func (c Collection[P]) softDelete(kind, name string, now int64, retention int) (*Entity[P], error) {
	e, ok := c.active(name)
	if !ok {
		return nil, errNotFound(kind, name)
	}

	e.Deleted = &DeletedInfo{DeletedDate: now, ScheduledPurgeDate: now + int64(retention)*secondsPerDay}

	return e, nil
}

func (c Collection[P]) recover(kind, name string) (*Entity[P], error) {
	e, ok := c.deleted(name)
	if !ok {
		return nil, errNotDeleted(kind, name)
	}

	e.Deleted = nil

	return e, nil
}

func (c Collection[P]) purge(kind, name string, protected bool) error {
	if _, ok := c.deleted(name); !ok {
		return errNotDeleted(kind, name)
	}

	if protected {
		return newErr(
			http.StatusForbidden,
			"Forbidden",
			"Operation purge is not allowed on a vault with purge protection enabled.",
		)
	}

	delete(c, name)

	return nil
}

// names returns sorted entity names, deleted or active.
func (c Collection[P]) names(deleted bool) []string {
	out := make([]string, 0, len(c))

	for n, e := range c {
		if (e.Deleted != nil) == deleted {
			out = append(out, n)
		}
	}

	sort.Strings(out)

	return out
}

// vault is one Key Vault's complete state.
type vault struct {
	Secrets         Collection[*SecretVersion] `json:"secrets"`
	Keys            Collection[*KeyVersion]    `json:"keys"`
	Certs           Collection[*CertVersion]   `json:"certs"`
	Issuers         map[string]*Issuer         `json:"issuers"`
	Contacts        *Contacts                  `json:"contacts,omitempty"`
	Name            string                     `json:"name"`
	RetentionDays   int                        `json:"retentionDays"`
	PurgeProtection bool                       `json:"purgeProtection"`
}

func newVault(name string) *vault {
	return &vault{
		Name:          name,
		RetentionDays: defaultRetentionDays,
		Secrets:       make(Collection[*SecretVersion]),
		Keys:          make(Collection[*KeyVersion]),
		Certs:         make(Collection[*CertVersion]),
		Issuers:       make(map[string]*Issuer),
	}
}

func (v *vault) sweep(now int64) {
	v.Secrets.sweep(now)
	v.Keys.sweep(now)
	v.Certs.sweep(now)
}
