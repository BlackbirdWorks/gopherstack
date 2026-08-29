package main

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// finding is one emitted error code this tool could not verify against its
// service's pinned SDK. Confident findings are sound: a direct literal
// (never more than one hop of same-package identifier resolution) reached
// through an unambiguous single resolved SDK module, absent from both that
// module's legitimate code set and the generic protocol-level allowlist.
// Needs-review findings come from a weaker signal -- see scan() for the
// three ways a finding is demoted rather than dropped, since dropping
// silently hides a real bug exactly as easily as a false one (the
// enumcheck/xmlitemwrap lesson this tool's brief calls out explicitly).
type finding struct {
	File      string    `json:"file"`
	Code      string    `json:"code"`
	Mechanism mechanism `json:"mechanism"`
	Reason    string    `json:"reason"`
	Line      int       `json:"line"`
	Confident bool      `json:"confident"`
}

func scan(repoRoot, cache string, goModVersions map[string]string) ([]finding, error) {
	svcDirs, err := serviceDirs(filepath.Join(repoRoot, "services"))
	if err != nil {
		return nil, err
	}

	var all []finding

	for _, dir := range svcDirs {
		found, scanErr := scanServiceDir(dir, repoRoot, cache, goModVersions)
		if scanErr != nil {
			return nil, scanErr
		}

		all = append(all, found...)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].File != all[j].File {
			return all[i].File < all[j].File
		}

		return all[i].Line < all[j].Line
	})

	return all, nil
}

func serviceDirs(svcRoot string) ([]string, error) {
	entries, err := os.ReadDir(svcRoot)
	if err != nil {
		return nil, err
	}

	var dirs []string

	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(svcRoot, e.Name()))
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}

// scanServiceDir resolves dir's pinned SDK module(s), builds their
// legitimate code set, extracts every candidate emitted code, and reports
// each one absent from both that set and the generic allowlist. A service
// with no resolvable SDK module, or whose resolved module(s) model NO
// error codes at all (ec2's documented case: 785 operations, zero typed
// exceptions in this SDK version -- there is no ground truth to check
// against, so every emission would false-positive as "absent") contributes
// nothing, never an error.
func scanServiceDir(dir, repoRoot, cache string, goModVersions map[string]string) ([]finding, error) {
	mods, err := resolveServiceModules(dir)
	if err != nil {
		return nil, err
	}

	if len(mods) == 0 {
		return nil, nil
	}

	gt, err := buildServiceGroundTruth(cache, mods, goModVersions)
	if err != nil {
		return nil, err
	}

	if gt.codeModules == 0 {
		return nil, nil
	}

	candidates, err := extractCandidates(dir, repoRoot)
	if err != nil {
		return nil, err
	}

	return classify(candidates, gt), nil
}

func classify(candidates []candidate, gt *serviceGroundTruth) []finding {
	seen := map[[3]string]bool{}

	var out []finding

	for _, c := range candidates {
		if gt.codes[c.Code] || genericProtocolCodes[c.Code] {
			continue
		}

		key := [3]string{c.File, strconv.Itoa(c.Line), c.Code}
		if seen[key] {
			continue
		}

		seen[key] = true

		out = append(out, buildFinding(c, gt))
	}

	return out
}

func buildFinding(c candidate, gt *serviceGroundTruth) finding {
	f := finding{File: c.File, Line: c.Line, Code: c.Code, Mechanism: c.Mechanism}

	switch {
	case c.MapperReason != "":
		f.Confident = false
		f.Reason = c.MapperReason
	case gt.resolvedModules > 1:
		f.Confident = false
		f.Reason = "service resolves 2+ SDK modules; which one's exception set applies here is unknown"
	case gt.sparse:
		f.Confident = false
		f.Reason = "resolved SDK module models errors on under half its operations (s3-class); " +
			"absence here is weak evidence, verify against AWS docs directly"
	case c.Indirect:
		f.Confident = false
		f.Reason = "reached through a weaker signal (resolved identifier or function-name heuristic), not a direct literal"
	default:
		f.Confident = true
		f.Reason = "direct literal, single resolved SDK module, absent from its " +
			"ErrorCode()/deserializer set and the generic allowlist"
	}

	return f
}
