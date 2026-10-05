// Command covledger reads and queries the bug-class coverage ledger at
// cmd/covledger/coverage.yaml -- gopherstack-7q13's answer to two
// consecutive targeting failures on this campaign, both traceable to the
// same missing thing: which service has been checked for which class of
// bug lived only in prose, scattered across bd comments, commit subjects
// and per-service PARITY.md sections under labels chosen ad hoc per pass.
// A pass was dispatched at three services already swept days earlier
// under different commit subjects ("dropped filters", "wrapper keys");
// it returned zero bugs and documentation-only changes. A mechanical
// detector built from four confirmed sightings of one bug shape produced
// nine candidates and zero true positives, because nothing recorded which
// of those nine had already been checked.
//
// THIS TOOL DOES NOT DETECT BUGS. It is a ledger reader, not a scanner --
// gopherstack-7q13 is explicit that the classes below are judgement
// calls about work performed, not properties of source code a static
// pass could discover. Every row in coverage.yaml was written by a human
// (or an agent under human review) after reading a commit, a bd comment,
// or a PARITY.md section, not by running this tool over services/. If you
// are looking for a wire-shape scanner, see cmd/reqfieldscan,
// cmd/enumcheck, cmd/errcodeaudit or cmd/structfielddiff instead -- and
// read gopherstack-uox6 first, which explains why none of those tools can
// see this campaign's harder bugs either.
//
// gopherstack-ri57: A "CLEAN" VERDICT PRODUCES NO CODE DIFF, SO IT OFTEN
// PRODUCES NO ROW EITHER. A pass that finds a service clean touches at
// most a PARITY.md line, usually inside a commit named for whichever
// sibling service DID have a bug -- so reading commit subjects alone
// systematically under-records clean verdicts (confirmed four times:
// transcribe, docdb, swf, and the fsx/codebuild pair, the last two filed
// under a different class label entirely). Every Row now carries an
// optional Source field recording what evidence backs it -- "commit"
// (the commit subject/body names the service), "parity" (a PARITY.md
// entry), "bd_comment" (a tracking-issue comment), or a '+'-joined
// combination. Run with -parity-only to list every row resting on
// PARITY.md alone: PARITY has been wrong eighteen distinct ways across
// this campaign, so a row with no commit-subject or bd-comment
// corroboration deserves less trust than one that has it, not the same
// trust as a hand-verified fix.
//
// THE CLASSES, and how they differ from their nearest neighbour:
//
//   - request_field_never_read: a field is declared on the wire and
//     decoded, and no handler code reads it at all. cmd/reqfieldscan's
//     ground truth.
//   - wrong_wire_key: the code reads (or writes) a field under a key,
//     nesting, or cardinality that does not match the real wire shape --
//     a singular key where the wire sends a plural list, a response
//     member dropped or fabricated, a scalar read where the wire is an
//     indexed list. The field IS "read", just never populated correctly
//     regardless of intent. gopherstack-6flj's wrapper-key sweep.
//   - filter_default_semantics: the field IS read and applied, but the
//     ALGORITHM is wrong -- an operator ignored, a boundary off by one,
//     a default that widens where its documentation narrows, a negation
//     mark compared as literal text. This is the one no shape-comparison
//     tool can see: gopherstack-uox6's whole point is that a field-diff
//     can report a service "wire-complete" while its filter logic does
//     the wrong thing with the right field.
//   - error_envelope_shape: the wire shape of an ERROR response --
//     bare vs. wrapped, alias vs. shape name, a failure silently reported
//     as success.
//   - fabricated_error_code: an error code that names no type the real
//     SDK defines, so a typed client's errors.As can never match it.
//   - wrong_enum_value: a value written into a real, correctly-keyed
//     enum-typed field that is not a member of that enum's declared set.
//   - pagination_ordering: an unstable sort feeding a paginated cursor,
//     a cursor or page size accepted and not honoured, an ordering two
//     calls can disagree on.
//
// These are stable because the campaign that produced them (gopherstack
// -6flj, -uox6, and roughly 300 commits of fix()/docs()/test() passes on
// this branch) never distinguished an eighth. A future pass that finds a
// genuinely new shape should add a Class constant in ledger.go, not
// force it into the nearest existing one.
//
// WHAT THE LEDGER CANNOT TELL YOU, stated here because a coverage table
// invites more confidence than it earns:
//
//   - A "clean" verdict records that a service was CHECKED, not that it
//     is bug-free. gopherstack-7q13 itself: a pass recorded as clean may
//     have been shallow, and one recorded as fixed may have missed other
//     instances of the same class in the same service.
//   - Rows were derived mainly from commit SUBJECT LINES and their named
//     scope (the services named in "fix(a,b,c): ..."), not from a diff
//     of every file the commit touched. A commit whose subject names
//     three services but whose body describes a bug found in only one of
//     them may over-attribute a "fixed" verdict to the other two --
//     usually defensible, since these commits' own bodies describe all
//     three as swept with the same discipline, but not the same as a
//     per-service diff review. Treat a row as "this service was part of
//     a pass that used this discipline and reached this verdict for the
//     batch", not as a promise that this exact service's own diff
//     contains a hunk for this exact class.
//   - Coverage of the classes across the campaign's history is
//     uneven by construction: the campaign audited pagination and
//     wire-key bugs far more exhaustively than error-envelope or
//     enum-value bugs, so a class with few rows may be under-audited
//     rather than clean, and a service with zero rows anywhere may
//     simply never have been named in a commit subject even if it was
//     touched incidentally by one.
//   - An ABSENT row means "unknown", never "clean". A service with no
//     row for a class has not been ruled out; it has never been looked
//     at under this ledger's evidence standard. Do not read a service's
//     absence from every class as evidence the service is fine.
//   - Only commits reachable from this branch (main..HEAD at the time
//     this ledger was built) were read. Work recorded solely in bd
//     comments with no corresponding commit, or merged to main through a
//     different branch, is not reflected here unless it was also cross-
//     checked into a row by hand.
//   - This ledger was populated in one pass, over roughly 150 of the
//     ~300 commits on this branch (the fix()/docs()/test() ones; pure
//     chore(beads) bookkeeping commits carry no code evidence and were
//     skipped, as were internal tool-only fixes to cmd/reqfieldscan,
//     cmd/enumcheck and cmd/errcodeaudit that named no service). It is a
//     snapshot, not a live index -- nothing here updates coverage.yaml
//     automatically as new passes land. The next pass that establishes a
//     new row is expected to append it by hand, the same way this one
//     was built.
//   - A row sourced from "parity" alone (see -parity-only) rests entirely
//     on a PARITY.md prose entry that no commit subject and no bd comment
//     corroborates. PARITY.md has been wrong eighteen distinct ways over
//     this campaign, including a front-matter state field that was simply
//     false and a note falsified by the very commit that wrote it -- so a
//     parity-only row inherits that error rate. It is stronger evidence
//     than no row at all, but weaker than a row with a second source.
//     PARITY.md is also read for what it says explicitly, not inferred: a
//     service's overall A/B grade is a WIRE-SHAPE verdict, a different
//     axis from any of the classes here, and was never treated as
//     coverage for any of them. A PARITY section was only turned into a
//     row when it named a class (or a class's issue ID) explicitly; a
//     dated entry that just says "audited, still correct" with no class
//     named was left out rather than guessed at (example: the earlier
//     "browser parity pass" and "wrapper-key sweep" notes throughout
//     services/*/PARITY.md predate this class taxonomy and name no class
//     of the classes, so they were not mined for rows even where they read
//     as a clean verdict).
//   - VerdictInapplicable records a refusal. A row with a Subject (the
//     operation.parameter it is about) is one structural refusal inside a
//     pass and does not mark the service as covered for the class; a row
//     without one means the whole class has no surface. List them with
//     -inapplicable; the Reasoning keeps the wording that stopped the pass.
//   - conflicts: (top-level, alongside rows in coverage.yaml) records a
//     (service, class) pair where two evidence sources disagree, rather
//     than one being picked silently -- see ValidateConflicts. None exist
//     in the current file: every row added this pass had its sources
//     cross-checked and they agreed. The mechanism exists so the next
//     pass that finds a real disagreement has somewhere honest to put it
//     instead of guessing.
//
// Usage:
//
//	go run ./cmd/covledger                          # validate, print the per-class summary
//	go run ./cmd/covledger -class wrong_wire_key      # validate, then list services with no row for this class
//	go run ./cmd/covledger -service opensearch        # validate, then list every row for this service
//	go run ./cmd/covledger -parity-only               # validate, then list rows resting on PARITY.md alone
//	go run ./cmd/covledger -data path/to/other.yaml   # use a different ledger file
//
// Every invocation validates the ledger first (see Validate), regardless
// of which query flag is given: a query answer is only as good as the
// file it came from.
//
// Exit codes: 0 success, 1 a run error (bad flag, unreadable file,
// unparseable YAML), 2 the ledger failed validation.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	exitOK       = 0
	exitRunError = 1
	exitInvalid  = 2
)

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	os.Exit(run(opts, os.Stdout, os.Stderr))
}

type options struct {
	row        Row
	data       string
	service    string
	class      string
	parityOnly bool
	inapplic   bool
	add        bool
}

func parseFlags(args []string) (options, error) {
	fs := flag.NewFlagSet("covledger", flag.ContinueOnError)

	data := fs.String(
		"data",
		"",
		"path to the ledger YAML file (default: cmd/covledger/coverage.yaml in this checkout)",
	)
	service := fs.String("service", "", "list every row for this service")
	class := fs.String("class", "", "list services with no row for this class")
	parityOnly := fs.Bool("parity-only", false, "list rows whose only evidence is PARITY.md")

	inapplic := fs.Bool("inapplicable", false, "list every recorded refusal (inapplicable rows) with its reasoning")
	add := fs.Bool(
		"add",
		false,
		"append a row: needs -service -class -verdict -commit; optional -date -subject -source -reasoning",
	)
	verdict := fs.String("verdict", "", "with -add: fixed, clean or inapplicable")
	commit := fs.String("commit", "", "with -add: commit that establishes the verdict")
	date := fs.String("date", time.Now().Format(time.DateOnly), "with -add: verdict date")
	subject := fs.String(
		"subject",
		"",
		"with -add: operation/parameter this row is about (does not count as class coverage)",
	)
	source := fs.String("source", "", "with -add: commit, parity, bd_comment, '+'-joined")
	reasoning := fs.String("reasoning", "", "with -add: required for inapplicable")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}

	return options{
		data: *data, service: *service, class: *class, parityOnly: *parityOnly, inapplic: *inapplic, add: *add,
		row: Row{
			Service: *service, Class: *class, Verdict: *verdict, Date: *date, Commit: *commit,
			Source: *source, Subject: *subject, Reasoning: *reasoning,
		},
	}, nil
}

func run(opts options, stdout, stderr io.Writer) int {
	dataPath := opts.data
	if dataPath == "" {
		repoRoot, rerr := repoRootDir()
		if rerr != nil {
			fmt.Fprintln(stderr, "error:", rerr)

			return exitRunError
		}

		dataPath = filepath.Join(repoRoot, "cmd", "covledger", "coverage.yaml")
	}

	rows, err := LoadLedger(dataPath)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)

		return exitRunError
	}

	conflicts, err := LoadConflicts(dataPath)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)

		return exitRunError
	}

	servicesDir, err := servicesRootDir()
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)

		return exitRunError
	}

	knownServices, err := listServiceDirs(servicesDir)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)

		return exitRunError
	}

	errs := Validate(rows, knownServices)
	errs = append(errs, ValidateConflicts(conflicts, rows, knownServices)...)

	if len(errs) > 0 {
		fmt.Fprintln(stderr, "ledger validation FAILED:")

		for _, e := range errs {
			fmt.Fprintln(stderr, "  -", e)
		}

		return exitInvalid
	}

	return dispatch(opts, rows, len(conflicts), knownServices, dataPath, stdout, stderr)
}

func dispatch(
	opts options,
	rows []Row,
	conflicts int,
	knownServices map[string]bool,
	dataPath string,
	stdout, stderr io.Writer,
) int {
	var err error

	switch {
	case opts.add:
		if err = AppendRow(dataPath, rows, opts.row, knownServices); err != nil {
			fmt.Fprintln(stderr, "error:", err)

			return exitRunError
		}

		fmt.Fprintln(stdout, "appended:", opts.row.Service, opts.row.Class, opts.row.Verdict)
	case opts.inapplic:
		printInapplicable(stdout, rows)
	case opts.service != "":
		printServiceRows(stdout, rows, opts.service)
	case opts.class != "":
		if !isKnownClass(opts.class) {
			fmt.Fprintf(stderr, "error: %q is not a known class; see the package doc for the list\n", opts.class)

			return exitRunError
		}

		printMissingForClass(stdout, rows, opts.class, sortedKeys(knownServices))
	case opts.parityOnly:
		printParityOnly(stdout, rows)
	default:
		fmt.Fprintln(stdout, "ledger valid:", len(rows), "rows,", conflicts, "open evidence conflicts")
		printSummary(stdout, rows, sortedKeys(knownServices))
	}

	return exitOK
}

// repoRootDir mirrors cmd/reqfieldscan's own repo-root discovery.
func repoRootDir() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

// servicesRootDir returns this checkout's services/ directory. It is
// always the real tree, even under -data: a ledger row's service name is
// only meaningful relative to the services this checkout actually has.
func servicesRootDir() (string, error) {
	repoRoot, err := repoRootDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(repoRoot, "services"), nil
}

func listServiceDirs(root string) (map[string]bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", root, err)
	}

	out := make(map[string]bool, len(entries))

	for _, e := range entries {
		if e.IsDir() {
			out[e.Name()] = true
		}
	}

	return out, nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

func printServiceRows(w io.Writer, rows []Row, service string) {
	svcRows := RowsForService(rows, service)

	if len(svcRows) == 0 {
		fmt.Fprintf(w, "%s: no rows -- unknown coverage for every class\n", service)

		return
	}

	fmt.Fprintf(w, "%s: %d row(s)\n", service, len(svcRows))

	for _, r := range svcRows {
		fmt.Fprintf(w, "  %-30s %-14s %s  %s  %s\n", r.Class, r.Verdict, r.Date, r.Commit, r.Subject)
	}
}

func printInapplicable(w io.Writer, rows []Row) {
	out := InapplicableRows(rows)

	fmt.Fprintf(w, "%d recorded refusal(s) -- never re-dispatch:\n", len(out))

	for _, r := range out {
		fmt.Fprintf(w, "  %s / %s / %s\n      %s\n", r.Service, r.Class, r.Subject, r.Reasoning)
	}
}

func printParityOnly(w io.Writer, rows []Row) {
	only := RowsSourcedOnly(rows, "parity")

	fmt.Fprintf(w, "%d row(s) sourced only from PARITY.md, no commit-subject or bd-comment corroboration:\n", len(only))

	for _, r := range only {
		fmt.Fprintf(w, "  %-20s %-30s %-14s %s\n", r.Service, r.Class, r.Verdict, r.Commit)
	}
}

func printMissingForClass(w io.Writer, rows []Row, class string, allServices []string) {
	missing := MissingForClass(rows, class, allServices)

	fmt.Fprintf(w, "%s: %d of %d services have no row\n", class, len(missing), len(allServices))

	for _, s := range missing {
		fmt.Fprintln(w, " ", s)
	}
}

func printSummary(w io.Writer, rows []Row, allServices []string) {
	for _, c := range KnownClasses {
		missing := MissingForClass(rows, string(c), allServices)

		fixed, clean, inapplicable := 0, 0, 0

		for _, r := range rows {
			if r.Class != string(c) {
				continue
			}

			switch Verdict(r.Verdict) {
			case VerdictFixed:
				fixed++
			case VerdictClean:
				clean++
			case VerdictInapplicable:
				inapplicable++
			}
		}

		fmt.Fprintf(w, "%-28s fixed=%-3d clean=%-3d inapplicable=%-3d  no-row=%d of %d\n",
			c, fixed, clean, inapplicable, len(missing), len(allServices))
	}
}
