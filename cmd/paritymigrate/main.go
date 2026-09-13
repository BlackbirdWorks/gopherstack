// Command paritymigrate is gopherstack-anjf's one-off migration: it
// consolidates every services/<svc>/PARITY.md's scattered open-item fields
// (gaps:/residual_gaps:) into the single canonical items_still_open: list.
// See .claude/memories/parity-principles.md rule 6 for the schema this
// produces.
//
// -apply RELOCATES every item verbatim -- it never deletes one, even an
// item classify() judged "resolved elsewhere" (a later dated section of the
// same file already marks it fixed, the gopherstack-anjf shape). Hand-
// checking that bucket across the corpus found real false positives (a
// "fixed" citation for an incidental comparison token, not the item's
// actual subject -- see classify.go's verdict.drop doc comment), too high a
// rate to trust unattended against this campaign's most valuable signal.
// Pass -drop-resolved-elsewhere to additionally delete that bucket instead
// of relocating it -- NOT used for this migration's own applied run; the
// dry-run report lists every such candidate, with its fix citation, for a
// human to confirm and remove by hand (or file a bd issue against).
//
// deferred: is deliberately never touched -- it records audit scope, not
// fix status (see manifest.go's sourceFields doc comment). Body text is
// never touched. A pre-existing items_still_open: block is never rewritten
// by this tool -- only reported on if one of its own entries looks resolved
// elsewhere (see migrate.go's existingOpenFindings).
//
// Usage:
//
//	go run ./cmd/paritymigrate                      # dry run, report to stdout
//	go run ./cmd/paritymigrate -service ecs,iam     # limit to these services
//	go run ./cmd/paritymigrate -apply               # relocate in clean services, in place
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	dir := "services"
	apply := false
	dropResolved := false
	ensurePresentMode := false

	var only map[string]bool

	args := os.Args[1:]

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-apply":
			apply = true
		case "-drop-resolved-elsewhere":
			dropResolved = true
		case "-ensure-present":
			ensurePresentMode = true
		case "-dir":
			i++
			dir = args[i]
		case "-service":
			i++
			only = toSet(strings.Split(args[i], ","))
		default:
			fmt.Fprintln(os.Stderr, "paritymigrate: unknown flag", args[i])
			os.Exit(1)
		}
	}

	if ensurePresentMode {
		if err := runEnsurePresent(dir, apply, only); err != nil {
			fmt.Fprintln(os.Stderr, "paritymigrate:", err)
			os.Exit(1)
		}

		return
	}

	if err := run(dir, apply, dropResolved, only); err != nil {
		fmt.Fprintln(os.Stderr, "paritymigrate:", err)
		os.Exit(1)
	}
}

// runEnsurePresent implements gopherstack-anjf option 3(c) mechanically: add
// "items_still_open: []" to every manifest whose source blocks are
// genuinely empty and which has no items_still_open: of its own yet. See
// ensure.go.
func runEnsurePresent(dir string, apply bool, only map[string]bool) error {
	manifests, err := discoverManifests(dir)
	if err != nil {
		return err
	}

	dirtyServices, err := gitDirtyServices()
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}

	var touched, skipped []string

	for _, m := range manifests {
		if only != nil && !only[m.service] {
			continue
		}

		content, ok, reason := ensurePresent(m)
		if reason != "" {
			skipped = append(skipped, fmt.Sprintf("%s: %s", m.service, reason))
		}

		if !ok {
			continue
		}

		if apply {
			if dirtyServices[m.service] {
				skipped = append(skipped, m.service+": dirty git status")

				continue
			}

			if writeErr := os.WriteFile(m.path, []byte(content), 0o600); writeErr != nil {
				return fmt.Errorf("write %s: %w", m.path, writeErr)
			}
		}

		touched = append(touched, m.service)
	}

	fmt.Fprintf(os.Stdout, "items_still_open: [] added to %d services: %s\n", len(touched), strings.Join(touched, ", "))

	if len(skipped) > 0 {
		fmt.Fprintf(os.Stdout, "skipped: %s\n", strings.Join(skipped, "; "))
	}

	return nil
}

func toSet(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, it := range items {
		m[strings.TrimSpace(it)] = true
	}

	return m
}

func run(dir string, apply, dropResolved bool, only map[string]bool) error {
	manifests, err := discoverManifests(dir)
	if err != nil {
		return err
	}

	dirtyServices, err := gitDirtyServices()
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}

	var (
		results []serviceResult
		skipped []string
	)

	for _, m := range manifests {
		if only != nil && !only[m.service] {
			continue
		}

		res := planMigration(m)

		if apply && (res.totalKept()+res.totalDropped()) > 0 {
			if dirtyServices[m.service] {
				skipped = append(skipped, m.service)

				continue
			}

			newContent := rewrite(m, res, dropResolved)
			if writeErr := os.WriteFile(m.path, []byte(newContent), 0o600); writeErr != nil {
				return fmt.Errorf("write %s: %w", m.path, writeErr)
			}
		}

		results = append(results, res)
	}

	printReport(os.Stdout, results, apply)

	if len(skipped) > 0 {
		fmt.Fprintf(os.Stdout, "\nskipped (dirty git status): %s\n", strings.Join(skipped, ", "))
	}

	return nil
}

// gitDirtyServices returns the set of services/<svc> directories `git
// status --short` reports any change under, so -apply never touches a
// manifest sitting next to work another pass has in flight.
func gitDirtyServices() (map[string]bool, error) {
	out, err := exec.CommandContext(context.Background(), "git", "status", "--short").Output()
	if err != nil {
		return nil, err
	}

	dirty := map[string]bool{}

	for line := range strings.SplitSeq(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		p := fields[len(fields)-1]

		const prefix = "services/"
		if !strings.HasPrefix(p, prefix) {
			continue
		}

		rest := strings.TrimPrefix(p, prefix)
		if svc, _, found := strings.Cut(rest, "/"); found {
			dirty[svc] = true
		}
	}

	return dirty, nil
}
