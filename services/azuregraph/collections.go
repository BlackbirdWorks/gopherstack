package azuregraph

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
)

// collection adapts one directory object type to the generic CRUD routes.
type collection struct {
	fields   map[string]bool
	list     func(*Backend) []map[string]any
	get      func(*Backend, Ref) (map[string]any, error)
	create   func(*Backend, map[string]json.RawMessage) (map[string]any, error)
	patch    func(*Backend, Ref, map[string]json.RawMessage) error
	del      func(*Backend, Ref) error
	name     string
	resource string
	kind     string
}

var keySegment = regexp.MustCompile(`^(\w+)\((\w+)='((?:[^']|'')*)'\)$`)

// splitKey separates `applications(appId='x')` into the collection name and an alternate key.
func splitKey(seg string) (string, Ref) {
	if m := keySegment.FindStringSubmatch(seg); m != nil {
		return m[1], Ref{Field: m[2], Value: strings.ReplaceAll(m[3], "''", "'")}
	}

	return seg, Ref{}
}

// toMap renders an entity as its generic JSON object.
func toMap(v any) map[string]any {
	raw, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}

	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)

	return out
}

func typed(kind string, v any) map[string]any {
	m := toMap(v)
	m["@odata.type"] = "#microsoft.graph." + kind

	return m
}

func mapAll[T any](in []T) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, v := range in {
		out = append(out, toMap(v))
	}

	return out
}

func one[T any](v T, err error) (map[string]any, error) {
	if err != nil {
		return nil, err
	}

	return toMap(v), nil
}

func fieldsOf[T any]() map[string]bool { return jsonFields(reflect.TypeFor[T]()) }

func buildCollections() map[string]*collection {
	return map[string]*collection{
		"applications": {
			name: "applications", resource: "Application", kind: kindApplication, fields: fieldsOf[Application](),
			list: func(b *Backend) []map[string]any { return mapAll(b.ListApplications()) },
			get:  func(b *Backend, r Ref) (map[string]any, error) { return one(b.GetApplication(r)) },
			create: func(b *Backend, m map[string]json.RawMessage) (map[string]any, error) {
				return one(b.CreateApplication(m))
			},
			patch: func(b *Backend, r Ref, m map[string]json.RawMessage) error { return b.UpdateApplication(r, m) },
			del:   func(b *Backend, r Ref) error { return b.DeleteApplication(r) },
		},
		"servicePrincipals": {
			name: "servicePrincipals", resource: "ServicePrincipal", kind: kindServicePrincipal,
			fields: fieldsOf[ServicePrincipal](),
			list:   func(b *Backend) []map[string]any { return mapAll(b.ListServicePrincipals()) },
			get:    func(b *Backend, r Ref) (map[string]any, error) { return one(b.GetServicePrincipal(r)) },
			create: func(b *Backend, m map[string]json.RawMessage) (map[string]any, error) {
				return one(b.CreateServicePrincipal(m))
			},
			patch: func(b *Backend, r Ref, m map[string]json.RawMessage) error { return b.UpdateServicePrincipal(r, m) },
			del:   func(b *Backend, r Ref) error { return b.DeleteServicePrincipal(r) },
		},
		"users": {
			name: "users", resource: "User", kind: kindUser, fields: fieldsOf[User](),
			list:   func(b *Backend) []map[string]any { return mapAll(b.ListUsers()) },
			get:    func(b *Backend, r Ref) (map[string]any, error) { return one(b.GetUser(r)) },
			create: func(b *Backend, m map[string]json.RawMessage) (map[string]any, error) { return one(b.CreateUser(m)) },
			patch:  func(b *Backend, r Ref, m map[string]json.RawMessage) error { return b.UpdateUser(r, m) },
			del:    func(b *Backend, r Ref) error { return b.DeleteUser(r) },
		},
		"groups": {
			name: "groups", resource: "Group", kind: kindGroup, fields: fieldsOf[Group](),
			list:   func(b *Backend) []map[string]any { return mapAll(b.ListGroups()) },
			get:    func(b *Backend, r Ref) (map[string]any, error) { return one(b.GetGroup(r)) },
			create: func(b *Backend, m map[string]json.RawMessage) (map[string]any, error) { return one(b.CreateGroup(m)) },
			patch:  func(b *Backend, r Ref, m map[string]json.RawMessage) error { return b.UpdateGroup(r, m) },
			del:    func(b *Backend, r Ref) error { return b.DeleteGroup(r) },
		},
	}
}
