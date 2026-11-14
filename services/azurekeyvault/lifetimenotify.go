package azurekeyvault

import (
	"slices"
	"strings"
	"time"
)

const (
	eventKeyNear  = "Microsoft.KeyVault.KeyNearExpiry"
	eventCertNear = "Microsoft.KeyVault.CertificateNearExpiry"
	actionEmail   = "EmailContacts"
)

// LifetimeNotification records a Notify or EmailContacts lifetime action that fired; nothing is sent.
type LifetimeNotification struct {
	Kind       string   `json:"kind"`
	Object     string   `json:"object"`
	Version    string   `json:"version"`
	Action     string   `json:"action"`
	Event      string   `json:"event"`
	Recipients []string `json:"recipients,omitempty"`
	At         int64    `json:"at"`
}

func (n LifetimeNotification) same(o LifetimeNotification) bool {
	return n.Kind == o.Kind && n.Object == o.Object && n.Version == o.Version && n.Action == o.Action
}

func (v *vault) contactEmails() []string {
	if v.Contacts == nil {
		return nil
	}

	var out []string

	for _, c := range v.Contacts.Contacts {
		if c.Email != "" {
			out = append(out, c.Email)
		}
	}

	return out
}

func (v *vault) record(n LifetimeNotification, out *[]LifetimeNotification) {
	if slices.ContainsFunc(v.Notifications, n.same) {
		return
	}

	v.Notifications = append(v.Notifications, n)
	*out = append(*out, n)
}

func keyNotifyDue(e *Entity[*KeyVersion], now time.Time) bool {
	cur := e.latest()
	if cur == nil || cur.Managed || e.Rotation == nil || !cur.Attrs.enabled() {
		return false
	}

	created := time.Unix(cur.Attrs.Created0(now.Unix()), 0)

	for _, la := range e.Rotation.LifetimeActions {
		if !strings.EqualFold(la.Action.Type, actionNotify) {
			continue
		}

		for _, at := range la.Trigger.firesAt(cur, created) {
			if at.After(created) && !now.Before(at) {
				return true
			}
		}
	}

	return false
}

func emailDue(e *Entity[*CertVersion], now time.Time) bool {
	cur := e.latest()
	if cur == nil || e.CertPolicy == nil || !cur.Attrs.enabled() || cur.Attrs.NBF == nil || cur.Attrs.Exp == nil {
		return false
	}

	nbf, exp := time.Unix(*cur.Attrs.NBF, 0), time.Unix(*cur.Attrs.Exp, 0)

	for _, la := range e.CertPolicy.LifetimeActions {
		if !strings.EqualFold(la.Action.ActionType, actionEmail) {
			continue
		}

		if at, ok := la.Trigger.fireTime(nbf, exp); ok && at.After(nbf) && !now.Before(at) {
			return true
		}
	}

	return false
}

// notifyDue records every Notify and EmailContacts action that fired and had not been recorded yet.
func (v *vault) notifyDue(now time.Time) []LifetimeNotification {
	var out []LifetimeNotification

	for _, name := range v.Keys.names(false) {
		if e := v.Keys[name]; keyNotifyDue(e, now) {
			v.record(LifetimeNotification{
				Kind: kindKey, Object: name, Version: e.latest().ID, Action: actionNotify,
				Event: eventKeyNear, At: now.Unix(),
			}, &out)
		}
	}

	for _, name := range v.Certs.names(false) {
		if e := v.Certs[name]; emailDue(e, now) {
			v.record(LifetimeNotification{
				Kind: kindCertificate, Object: name, Version: e.latest().ID, Action: actionEmail,
				Event: eventCertNear, Recipients: v.contactEmails(), At: now.Unix(),
			}, &out)
		}
	}

	return out
}

// Notifications returns the recorded lifetime notifications of a vault, oldest first.
func (b *InMemoryBackend) Notifications(vaultName string) ([]LifetimeNotification, error) {
	var out []LifetimeNotification

	err := b.do(vaultName, func(v *vault, _ int64) error {
		out = slices.Clone(v.Notifications)

		return nil
	})

	return out, err
}
