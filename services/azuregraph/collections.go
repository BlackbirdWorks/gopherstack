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

	if t := reflect.TypeOf(v); t.Kind() == reflect.Struct {
		for name := range jsonFields(t) {
			if _, ok := out[name]; !ok {
				out[name] = nil
			}
		}
	}

	delete(out, "passwordProfile")

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

func newCollection[T any](
	name, resource, kind string,
	list func(*Backend) []T,
	get func(*Backend, Ref) (T, error),
	create func(*Backend, map[string]json.RawMessage) (T, error),
	patch func(*Backend, Ref, map[string]json.RawMessage) error,
	del func(*Backend, Ref) error,
) *collection {
	return &collection{
		name: name, resource: resource, kind: kind, fields: fieldsOf[T](), patch: patch, del: del,
		list:   func(b *Backend) []map[string]any { return mapAll(list(b)) },
		get:    func(b *Backend, r Ref) (map[string]any, error) { return one(get(b, r)) },
		create: func(b *Backend, m map[string]json.RawMessage) (map[string]any, error) { return one(create(b, m)) },
	}
}

func buildCollections() map[string]*collection {
	return map[string]*collection{
		collApplications: newCollection(collApplications, typeApplication, kindApplication,
			(*Backend).ListApplications, (*Backend).GetApplication, (*Backend).CreateApplication,
			(*Backend).UpdateApplication, (*Backend).DeleteApplication),
		collServicePrincipals: newCollection(collServicePrincipals, "ServicePrincipal", kindServicePrincipal,
			(*Backend).ListServicePrincipals, (*Backend).GetServicePrincipal, (*Backend).CreateServicePrincipal,
			(*Backend).UpdateServicePrincipal, (*Backend).DeleteServicePrincipal),
		collUsers: newCollection(collUsers, typeUser, kindUser,
			(*Backend).ListUsers, (*Backend).GetUser, (*Backend).CreateUser,
			(*Backend).UpdateUser, (*Backend).DeleteUser),
		collGroups: newCollection(collGroups, "Group", kindGroup,
			(*Backend).ListGroups, (*Backend).GetGroup, (*Backend).CreateGroup,
			(*Backend).UpdateGroup, (*Backend).DeleteGroup),
	}
}
