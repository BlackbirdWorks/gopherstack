package gcpfirestore

import (
	"slices"
	"strings"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
)

const (
	docsSegment  = "documents"
	nameField    = "__name__"
	dbParts      = 4
	maxDocIDLen  = 1500
	autoIDLen    = 20
	autoIDAlphas = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)

// docPath is a parsed "projects/P/databases/D/documents/..." resource name.
type docPath struct {
	db   string
	segs []string
}

func (p docPath) root() string { return p.db + "/" + docsSegment }

func (p docPath) String() string {
	if len(p.segs) == 0 {
		return p.root()
	}

	return p.root() + "/" + strings.Join(p.segs, "/")
}

func (p docPath) isDoc() bool { return len(p.segs) > 0 && len(p.segs)%2 == 0 }

func parsePath(name string) (docPath, error) {
	parts := strings.Split(name, "/")
	if len(parts) < dbParts+1 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != docsSegment ||
		parts[1] == "" || parts[3] == "" {
		return docPath{}, errInvalid("Invalid resource name: %q", name)
	}

	segs := parts[dbParts+1:]
	if len(segs) == 1 && segs[0] == "" {
		segs = nil
	}

	for _, s := range segs {
		if s == "" || len(s) > maxDocIDLen {
			return docPath{}, errInvalid("Invalid resource name: %q", name)
		}
	}

	return docPath{db: strings.Join(parts[:dbParts], "/"), segs: segs}, nil
}

func validateDocName(name string) error {
	p, err := parsePath(name)
	if err != nil {
		return err
	}

	if !p.isDoc() {
		return errInvalid("Invalid document name: %q", name)
	}

	return nil
}

// parseParent parses a query/list parent: the documents root or a document.
func parseParent(name string) (docPath, error) {
	p, err := parsePath(name)
	if err != nil {
		return p, err
	}

	if len(p.segs) > 0 && !p.isDoc() {
		return p, errInvalid("Invalid parent: %q", name)
	}

	return p, nil
}

func validateDatabase(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != dbParts || parts[0] != "projects" || parts[2] != "databases" || parts[1] == "" || parts[3] == "" {
		return errInvalid("Invalid database name: %q", name)
	}

	return nil
}

// parseFieldPath splits a dotted, backtick-quoted field path.
func parseFieldPath(s string) ([]string, error) {
	if s == "" {
		return nil, errInvalid("Field path must not be empty.")
	}

	var (
		segs []string
		cur  strings.Builder
	)

	for i := 0; i < len(s); {
		switch s[i] {
		case '`':
			i++

			for ; i < len(s) && s[i] != '`'; i++ {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}

				cur.WriteByte(s[i])
			}

			if i >= len(s) {
				return nil, errInvalid("Unterminated quoted field path: %q", s)
			}

			i++
		case '.':
			segs = append(segs, cur.String())
			cur.Reset()

			i++
		default:
			cur.WriteByte(s[i])

			i++
		}
	}

	segs = append(segs, cur.String())

	if slices.Contains(segs, "") {
		return nil, errInvalid("Invalid field path: %q", s)
	}

	return segs, nil
}

func getPath(fields map[string]*firestorepb.Value, path []string) (*firestorepb.Value, bool) {
	cur := fields

	for i, seg := range path {
		v, ok := cur[seg]
		if !ok {
			return nil, false
		}

		if i == len(path)-1 {
			return v, true
		}

		m, isMap := v.GetValueType().(*firestorepb.Value_MapValue)
		if !isMap {
			return nil, false
		}

		cur = m.MapValue.GetFields()
	}

	return nil, false
}

func setPath(fields map[string]*firestorepb.Value, path []string, v *firestorepb.Value) {
	cur := fields

	for _, seg := range path[:len(path)-1] {
		next, ok := cur[seg].GetValueType().(*firestorepb.Value_MapValue)
		if !ok || next.MapValue.GetFields() == nil {
			nm := map[string]*firestorepb.Value{}
			cur[seg] = mapVal(nm)
			cur = nm

			continue
		}

		cur = next.MapValue.GetFields()
	}

	cur[path[len(path)-1]] = v
}

func deletePath(fields map[string]*firestorepb.Value, path []string) {
	cur := fields

	for _, seg := range path[:len(path)-1] {
		next, ok := cur[seg].GetValueType().(*firestorepb.Value_MapValue)
		if !ok {
			return
		}

		cur = next.MapValue.GetFields()
	}

	delete(cur, path[len(path)-1])
}
