package codecommit

// resolver applies the caller's explicit resolutions and the strategy to conflicting files.
type resolver struct {
	deletes   map[string]bool
	replaces  map[string]ReplaceContentEntry
	modes     map[string]string
	strategy  string
	lineLevel bool
}

func newResolver(s MergeSettings) *resolver {
	r := &resolver{
		deletes:   map[string]bool{},
		replaces:  map[string]ReplaceContentEntry{},
		modes:     map[string]string{},
		strategy:  s.Strategy,
		lineLevel: s.DetailLevel == detailLineLevel || s.Strategy == strategyAutomerge,
	}

	if s.Resolution == nil {
		return r
	}

	for _, p := range s.Resolution.DeleteFiles {
		r.deletes[p] = true
	}

	for _, e := range s.Resolution.ReplaceContents {
		r.replaces[e.FilePath] = e
	}

	for _, e := range s.Resolution.SetFileModes {
		r.modes[e.FilePath] = e.FileMode
	}

	return r
}

// pickFor names the side an ACCEPT strategy takes for conflicting hunks.
func (r *resolver) pickFor() string {
	switch r.strategy {
	case strategyAcceptSource:
		return pickSource
	case strategyAcceptDest:
		return pickDest
	default:
		return ""
	}
}

func clearConflict(fm *fileMerge) {
	fm.conflict = false
	fm.meta.ContentConflict = false
	fm.meta.FileModeConflict = false
	fm.meta.NumberOfConflicts = 0
}

// resolve settles fm when explicit entries or the strategy cover it.
func (r *resolver) resolve(fm *fileMerge, base, src, dst side) {
	if r.applyExplicit(fm, base, src, dst) {
		return
	}

	chosen := src
	switch r.pickFor() {
	case "":
		return
	case pickDest:
		chosen = dst
	}

	switch {
	case fm.meta.ContentConflict:
		fm.result = chosen
	case fm.meta.FileModeConflict:
		fm.result.mode = chosen.mode
	}

	clearConflict(fm)
}

func (r *resolver) applyExplicit(fm *fileMerge, base, src, dst side) bool {
	path := fm.path

	if r.deletes[path] {
		fm.result = side{absent: true}
		clearConflict(fm)

		return true
	}

	if rep, ok := r.replaces[path]; ok {
		fm.result = replacement(rep, fm.result, base, src, dst)
		clearConflict(fm)

		return true
	}

	if mode, ok := r.modes[path]; ok && !fm.result.absent {
		fm.result.mode = mode

		if fm.meta.FileModeConflict {
			fm.meta.FileModeConflict = false
			fm.meta.NumberOfConflicts--
		}

		if !fm.meta.ContentConflict {
			clearConflict(fm)

			return true
		}
	}

	return false
}

func replacement(rep ReplaceContentEntry, cur, base, src, dst side) side {
	switch rep.ReplacementType {
	case "KEEP_BASE":
		return base
	case "KEEP_SOURCE":
		return src
	case "KEEP_DESTINATION":
		return dst
	default:
		mode := rep.FileMode
		if mode == "" {
			mode = modeOrDefault(cur.mode)
		}

		return side{content: rep.Content, mode: mode}
	}
}
