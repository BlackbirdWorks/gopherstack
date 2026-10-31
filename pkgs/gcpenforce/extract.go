package gcpenforce

import (
	"net/http"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	projectsSegment = "projects/"
	wildcardProject = "-"
	maxDepth        = 2
)

var projectRE = regexp.MustCompile(`(?:^|/)projects/([^/\s:?]+)`)

// priority lists the request field names that carry a resource name, most specific first. A field outside the
// list is tried last, in declaration order.
//
//nolint:gochecknoglobals // immutable lookup table
var priority = []string{
	"project", "project_id", "project_number", "parent", "name", "resource", "database", "instance", "session",
	"table_name", "topic", "subscription", "key_ring", "crypto_key", "secret", "location",
}

//nolint:gochecknoglobals // immutable lookup table
var bareProjectFields = map[string]bool{"project": true, "project_id": true, "project_number": true}

// FromMessage extracts the project a request targets. paths are per-method override field paths (dotted proto
// field names, such as "topic.name") tried first. It returns "" when the request names no project.
func FromMessage(msg proto.Message, paths ...string) string {
	if msg == nil {
		return ""
	}

	m := msg.ProtoReflect()

	for _, p := range paths {
		if v := stringAtPath(m, strings.Split(p, ".")); v != "" {
			if proj := fromValue(lastSegment(p), v); proj != "" {
				return proj
			}
		}
	}

	var cands []candidate

	collect(m, 0, &cands)

	best := -1
	bestRank := len(priority) + 1

	for i, c := range cands {
		if r := rank(c.field); r < bestRank {
			best, bestRank = i, r
		}
	}

	if best < 0 {
		return ""
	}

	return cands[best].project
}

type candidate struct {
	field   string
	project string
}

func rank(field string) int {
	for i, f := range priority {
		if f == field {
			return i
		}
	}

	return len(priority)
}

func collect(m protoreflect.Message, depth int, out *[]candidate) {
	fields := m.Descriptor().Fields()

	for i := range fields.Len() {
		fd := fields.Get(i)

		if !m.Has(fd) {
			continue
		}

		name := string(fd.Name())

		switch {
		case fd.Kind() == protoreflect.StringKind && !fd.IsMap():
			collectString(m, fd, name, out)
		case fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() && depth < maxDepth:
			collect(m.Get(fd).Message(), depth+1, out)
		}
	}
}

func collectString(m protoreflect.Message, fd protoreflect.FieldDescriptor, name string, out *[]candidate) {
	v := m.Get(fd)
	if fd.IsList() {
		if v.List().Len() == 0 {
			return
		}

		v = v.List().Get(0)
	}

	if proj := fromValue(name, v.String()); proj != "" {
		*out = append(*out, candidate{field: name, project: proj})
	}
}

func fromValue(field, v string) string {
	if v == "" {
		return ""
	}

	if mm := projectRE.FindStringSubmatch(v); mm != nil {
		return clean(mm[1])
	}

	if bareProjectFields[field] && !strings.Contains(v, "/") {
		return clean(v)
	}

	return ""
}

func clean(p string) string {
	if p == wildcardProject {
		return ""
	}

	return p
}

func lastSegment(path string) string {
	if i := strings.LastIndexByte(path, '.'); i >= 0 {
		return path[i+1:]
	}

	return path
}

func stringAtPath(m protoreflect.Message, path []string) string {
	for i, seg := range path {
		fd := m.Descriptor().Fields().ByName(protoreflect.Name(seg))
		if fd == nil || fd.IsList() || fd.IsMap() {
			return ""
		}

		if i == len(path)-1 {
			if fd.Kind() != protoreflect.StringKind {
				return ""
			}

			return m.Get(fd).String()
		}

		if fd.Kind() != protoreflect.MessageKind {
			return ""
		}

		m = m.Get(fd).Message()
	}

	return ""
}

// FromRequest extracts the project of a REST request: a projects/{id} path segment, else the project query
// parameter. With queryOnly the path is ignored (APIs whose object names may contain "projects/").
func FromRequest(r *http.Request, queryOnly bool) string {
	if !queryOnly {
		if mm := projectRE.FindStringSubmatch(r.URL.EscapedPath()); mm != nil {
			return clean(mm[1])
		}
	}

	q := r.URL.Query()
	for _, k := range []string{"project", "projectId", "project_id"} {
		if v := q.Get(k); v != "" {
			return clean(strings.TrimPrefix(v, projectsSegment))
		}
	}

	return ""
}
