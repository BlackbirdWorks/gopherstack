package gremlin

import (
	"regexp"
)

// Access is the extra permission a query needs beyond read; every query reads.
type Access struct {
	Write  bool
	Delete bool
}

var writeStepRE = regexp.MustCompile(`\b(addV|addE|mergeV|mergeE|property|drop)\s*\(`)

// ScriptAccess scans a script for steps that can write or delete; it over-approximates, never under.
func ScriptAccess(script string) Access {
	var a Access

	for _, m := range writeStepRE.FindAllStringSubmatch(script, -1) {
		a.merge(stepNameAccess(m[1]))
	}

	return a
}

// Access reports what the traversal's steps, modulators and nested traversals can do to the graph.
func (t *Traversal) Access() Access {
	var a Access

	for i := range t.Steps {
		a.merge(stepAccess(&t.Steps[i]))
	}

	return a
}

func (a *Access) merge(o Access) {
	a.Write = a.Write || o.Write
	a.Delete = a.Delete || o.Delete
}

func stepNameAccess(name string) Access {
	switch {
	case isCreateStep(name):
		return Access{Write: true}
	case name == "drop":
		return Access{Delete: true}
	case mutatingStep(name):
		return Access{Write: true, Delete: true} // property
	}

	return Access{}
}

func stepAccess(s *Step) Access {
	a := stepNameAccess(s.Name)

	for _, arg := range s.Args {
		a.merge(argAccess(arg))
	}

	for i := range s.Mods {
		a.merge(stepAccess(&s.Mods[i]))
	}

	return a
}

func argAccess(v any) Access {
	switch t := v.(type) {
	case *Traversal:
		return t.Access()
	case []any:
		var a Access

		for _, e := range t {
			a.merge(argAccess(e))
		}

		return a
	}

	return Access{}
}
