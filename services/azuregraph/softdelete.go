package azuregraph

import (
	"maps"
	"slices"
	"time"
)

// deletedRetention is how long Entra keeps soft-deleted objects before purging them.
const deletedRetention = 30 * 24 * time.Hour

type deletedItem struct {
	DeletedDateTime  time.Time             `json:"deletedDateTime"`
	Application      *Application          `json:"application,omitempty"`
	ServicePrincipal *ServicePrincipal     `json:"servicePrincipal,omitempty"`
	User             *User                 `json:"user,omitempty"`
	Group            *Group                `json:"group,omitempty"`
	Secrets          map[string]string     `json:"secrets,omitempty"`
	KeyMaterial      map[string]string     `json:"keyMaterial,omitempty"`
	Federated        []FederatedCredential `json:"federatedIdentityCredentials,omitempty"`
	Owners           []string              `json:"owners,omitempty"`
	Members          []string              `json:"members,omitempty"`
}

func (d *deletedItem) object() Object {
	switch {
	case d.Application != nil:
		return Object{Kind: kindApplication, Entity: *d.Application}
	case d.ServicePrincipal != nil:
		return Object{Kind: kindServicePrincipal, Entity: *d.ServicePrincipal}
	case d.User != nil:
		return Object{Kind: kindUser, Entity: *d.User}
	case d.Group != nil:
		return Object{Kind: kindGroup, Entity: *d.Group}
	}

	return Object{}
}

func (d *deletedItem) id() string {
	switch {
	case d.Application != nil:
		return d.Application.ID
	case d.ServicePrincipal != nil:
		return d.ServicePrincipal.ID
	case d.User != nil:
		return d.User.ID
	case d.Group != nil:
		return d.Group.ID
	}

	return ""
}

// DeletedObject is a soft-deleted directory object.
type DeletedObject struct {
	DeletedDateTime time.Time
	Object
}

// softDeletable reports whether Entra keeps the object in /directory/deletedItems: applications, service
// principals, users and Microsoft 365 (Unified) groups. Other groups are deleted permanently.
func softDeletable(o dirObject) bool {
	if g, ok := o.(*Group); ok {
		return slices.Contains(g.GroupTypes, "Unified")
	}

	return true
}

func (b *Backend) keyIDsOf(id string) []string {
	var out []string

	if a, ok := b.apps.get(id); ok {
		for _, k := range a.KeyCredentials {
			out = append(out, k.KeyID)
		}
	}

	if s, ok := b.sps.get(id); ok {
		for _, k := range s.KeyCredentials {
			out = append(out, k.KeyID)
		}
	}

	return out
}

func pick(m map[string]string, keys []string) map[string]string {
	out := map[string]string{}

	for _, k := range keys {
		if v, ok := m[k]; ok {
			out[k] = v
		}
	}

	return out
}

// deleteObject removes id, parking it in the deleted items container first when Entra would. Callers hold b.mu.
func (b *Backend) deleteObject(id string) {
	id = lc(id)

	if o, ok := b.lookup(id); ok && softDeletable(o) {
		item := &deletedItem{
			DeletedDateTime: b.stamp(), Owners: slices.Clone(b.owners[id]), Members: slices.Clone(b.members[id]),
			Secrets: pick(b.secrets, b.credentialsOf(id)), KeyMaterial: pick(b.keyMaterial, b.keyIDsOf(id)),
			Federated: slices.Clone(b.fics[id]),
		}

		switch e := o.(type) {
		case *Application:
			item.Application = e
		case *ServicePrincipal:
			item.ServicePrincipal = e
		case *User:
			item.User = e
		case *Group:
			item.Group = e
		}

		b.deleted.put(id, item)
	}

	b.removeObject(id)
}

// purgeExpired drops deleted items past the retention window. Callers hold the write lock.
func (b *Backend) purgeExpired() {
	cutoff := b.now().Add(-deletedRetention)

	for _, d := range b.deleted.all() {
		if !d.DeletedDateTime.After(cutoff) {
			b.deleted.del(lc(d.id()))
		}
	}
}

// ListDeleted returns the soft-deleted objects of a kind (all kinds when kind is empty).
func (b *Backend) ListDeleted(kind string) []DeletedObject {
	b.mu.Lock("ListDeleted")
	defer b.mu.Unlock()

	b.purgeExpired()

	var out []DeletedObject

	for _, d := range b.deleted.all() {
		if o := d.object(); kind == "" || o.Kind == kind {
			out = append(out, DeletedObject{Object: o, DeletedDateTime: d.DeletedDateTime})
		}
	}

	return out
}

func (b *Backend) findDeleted(id string) (*deletedItem, error) {
	if !isGUID(id) {
		return nil, badRequest("Invalid object identifier '%s'.", id)
	}

	b.purgeExpired()

	if d, ok := b.deleted.get(lc(id)); ok {
		return d, nil
	}

	return nil, notFound(id)
}

// GetDeleted returns one soft-deleted object.
func (b *Backend) GetDeleted(id string) (DeletedObject, error) {
	b.mu.Lock("GetDeleted")
	defer b.mu.Unlock()

	d, err := b.findDeleted(id)
	if err != nil {
		return DeletedObject{}, err
	}

	return DeletedObject{Object: d.object(), DeletedDateTime: d.DeletedDateTime}, nil
}

// PermanentDelete removes a soft-deleted object for good.
func (b *Backend) PermanentDelete(id string) error {
	b.mu.Lock("PermanentDelete")
	defer b.mu.Unlock()

	if _, err := b.findDeleted(id); err != nil {
		return err
	}

	b.deleted.del(lc(id))

	return nil
}

// RestoreDeleted moves a soft-deleted object back into the directory with its own owners, members and
// credentials. References from other objects to it are gone, as are its app role assignments and grants.
func (b *Backend) RestoreDeleted(id string) (Object, error) {
	b.mu.Lock("RestoreDeleted")
	defer b.mu.Unlock()

	d, err := b.findDeleted(id)
	if err != nil {
		return Object{}, err
	}

	id = lc(id)

	if err = b.reinstate(d, id); err != nil {
		return Object{}, err
	}

	b.deleted.del(id)
	b.owners[id] = b.stillPresent(d.Owners)
	b.members[id] = b.stillPresent(d.Members)
	maps.Copy(b.secrets, d.Secrets)
	maps.Copy(b.keyMaterial, d.KeyMaterial)

	if len(d.Federated) > 0 {
		b.fics[id] = slices.Clone(d.Federated)
	}

	return d.object(), nil
}

func (b *Backend) stillPresent(ids []string) []string {
	out := make([]string, 0, len(ids))

	for _, x := range ids {
		if _, ok := b.lookup(x); ok {
			out = append(out, x)
		}
	}

	return out
}

func (b *Backend) reinstate(d *deletedItem, id string) error {
	switch {
	case d.Application != nil:
		if err := b.validateApp(d.Application, id); err != nil {
			return err
		}

		b.apps.put(id, d.Application)
	case d.ServicePrincipal != nil:
		for _, o := range b.sps.all() {
			if lc(o.AppID) == lc(d.ServicePrincipal.AppID) {
				return badRequest(
					"Another object with the same value for property servicePrincipalNames already exists.",
				)
			}
		}

		b.sps.put(id, d.ServicePrincipal)
	case d.User != nil:
		if err := b.validateUser(d.User, id); err != nil {
			return err
		}

		b.users.put(id, d.User)
	case d.Group != nil:
		b.groups.put(id, d.Group)
	}

	return nil
}
