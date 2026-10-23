package gremlin

import "slices"

// Traversal is a parsed Gremlin traversal: the steps after g (or inside an anonymous __).
type Traversal struct {
	Steps []Step
}

// Step is one step with its arguments and the modulators ("by", "from", modTimes, ...) that follow it.
type Step struct {
	Name string
	Args []any
	Mods []Step
	// Pre marks a modulator written before its step (until/emit before repeat).
	Pre bool
}

// VertexRef is a vertex or edge argument that arrived by id.
type VertexRef struct {
	ID string
}

func isModulatorName(name string) bool {
	switch name {
	case "by", "from", "to", modTimes, modUntil, modEmit, "option", "with", "read", "write":
		return true
	}

	return false
}

func isCreateStep(name string) bool {
	switch name {
	case "addV", "addE", "mergeV", "mergeE":
		return true
	}

	return false
}

// add appends a step, attaching modulators to the step they modify.
func (t *Traversal) add(s Step) {
	if n := len(t.Steps); n > 0 {
		prev := &t.Steps[n-1]
		if isModulatorName(s.Name) && s.Name != modEmit && s.Name != modUntil && s.Name != modTimes {
			prev.Mods = append(prev.Mods, s)

			return
		}

		if s.Name == "property" && isCreateStep(prev.Name) {
			prev.Mods = append(prev.Mods, s)

			return
		}

		if loopModulator(s.Name) && prev.Name == "repeat" {
			prev.Mods = append(prev.Mods, s)

			return
		}
	}

	if loopModulator(s.Name) {
		s.Pre = true
		t.Steps = append(t.Steps, s)

		return
	}

	if s.Name == "repeat" {
		s = t.absorbPre(s)
	}

	t.Steps = append(t.Steps, s)
}

func loopModulator(name string) bool {
	return name == modEmit || name == modUntil || name == modTimes
}

// absorbPre moves until/emit steps written before repeat onto it.
func (t *Traversal) absorbPre(rep Step) Step {
	for n := len(t.Steps); n > 0 && t.Steps[n-1].Pre; n = len(t.Steps) {
		rep.Mods = append([]Step{t.Steps[n-1]}, rep.Mods...)
		t.Steps = t.Steps[:n-1]
	}

	return rep
}

func mutatingStep(name string) bool {
	switch name {
	case "addV", "addE", "property", "drop", "mergeV", "mergeE":
		return true
	}

	return false
}

// Mutates reports whether running the traversal can change the graph.
func (t *Traversal) Mutates() bool {
	for i := range t.Steps {
		if stepMutates(&t.Steps[i]) {
			return true
		}
	}

	return false
}

func stepMutates(s *Step) bool {
	if mutatingStep(s.Name) {
		return true
	}

	if slices.ContainsFunc(s.Args, argMutates) {
		return true
	}

	for i := range s.Mods {
		if stepMutates(&s.Mods[i]) {
			return true
		}
	}

	return false
}

func argMutates(a any) bool {
	switch t := a.(type) {
	case *Traversal:
		return t.Mutates()
	case []any:
		if slices.ContainsFunc(t, argMutates) {
			return true
		}
	}

	return false
}
