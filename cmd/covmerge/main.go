// Command covmerge merges Go coverage profiles (as produced by
// `go test -covermode=atomic -coverprofile=...`) into a single
// deduplicated profile.
//
// Every unit/integration/e2e CI job in this repo runs with a wide
// -coverpkg (all instrumented packages), so each profile it emits already
// contains a coverage block for every package under test -- including ones
// that specific job never exercised (recorded with a zero count). Naively
// concatenating profiles (`tail -n +2 "$f" >> coverage.out` for each file,
// as a plain `cat`-style merge would) therefore duplicates every block once
// per job. With ~6 jobs and ~200 covered packages that inflates the merged
// file enough to blow past Node's ~512MB string length limit when a
// downstream step reads it back with fs.readFileSync.
//
// covmerge instead parses each line's block key -- everything before the
// trailing execution count -- and sums counts for duplicate keys, emitting
// exactly one line per block. This is the same merge semantics as the
// well-known gocovmerge tool, specialized to atomic/count-style profiles
// (the only mode this repo's CI produces) and implemented with the
// standard library only so it needs no extra go.mod dependency.
//
// Profiles are read and merged line-by-line via bufio.Scanner, so memory
// use is proportional to the number of distinct coverage blocks in the
// codebase, not to the combined size of the input files.
//
// Usage:
//
//	go run ./cmd/covmerge -o coverage.out unit-0-coverage.out unit-1-coverage.out ...
//
// With no -o flag, the merged profile is written to stdout.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	errNoInputs       = errors.New("no input coverage profiles given")
	errNoCoverageData = errors.New("no coverage data found in input files")
	errModeMismatch   = errors.New("profile mode does not match earlier mode")
	errMalformedLine  = errors.New("malformed coverage line")
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "covmerge:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("covmerge", flag.ContinueOnError)
	output := fs.String("o", "", "output file (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	inputs := fs.Args()
	if len(inputs) == 0 {
		return errNoInputs
	}

	merged, mode, err := mergeProfiles(inputs)
	if err != nil {
		return err
	}

	w := stdout
	if *output != "" {
		outFile, createErr := os.Create(*output)
		if createErr != nil {
			return fmt.Errorf("creating output file: %w", createErr)
		}

		defer outFile.Close()
		w = outFile
	}

	return writeProfile(w, mode, merged)
}

// blockCounts accumulates per-block execution counts, keyed on everything in
// a coverage profile line except the trailing count. keys preserves
// first-seen order during scanning; it is sorted before the profile is
// written so output is deterministic regardless of input file order.
type blockCounts struct {
	counts map[string]int64
	keys   []string
}

// mergeProfiles reads every profile in paths and sums counts for duplicate
// block keys. All profiles must declare the same `mode:` header.
func mergeProfiles(paths []string) (*blockCounts, string, error) {
	bc := &blockCounts{counts: make(map[string]int64)}

	var mode string

	for _, path := range paths {
		if err := mergeOneProfile(path, &mode, bc); err != nil {
			return nil, "", err
		}
	}

	if mode == "" {
		return nil, "", fmt.Errorf("%w (%d file(s) provided)", errNoCoverageData, len(paths))
	}

	sort.Strings(bc.keys)

	return bc, mode, nil
}

func mergeOneProfile(path string, mode *string, bc *blockCounts) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	lineNo := 0

	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if line == "" {
			continue
		}

		if rest, ok := strings.CutPrefix(line, "mode:"); ok {
			m := strings.TrimSpace(rest)
			switch {
			case *mode == "":
				*mode = m
			case *mode != m:
				return fmt.Errorf("%s: %q vs %q: %w", path, m, *mode, errModeMismatch)
			}

			continue
		}

		key, count, parseErr := parseCoverageLine(line)
		if parseErr != nil {
			return fmt.Errorf("%s:%d: %w", path, lineNo, parseErr)
		}

		if _, exists := bc.counts[key]; !exists {
			bc.keys = append(bc.keys, key)
		}

		bc.counts[key] += count
	}

	if scanErr := sc.Err(); scanErr != nil {
		return fmt.Errorf("reading %s: %w", path, scanErr)
	}

	return nil
}

// parseCoverageLine splits a profile line of the form
// "name.go:startLine.startCol,endLine.endCol numStmts count" into its block
// key (everything before the final field) and the trailing count.
func parseCoverageLine(line string) (string, int64, error) {
	idx := strings.LastIndexByte(line, ' ')
	if idx < 0 {
		return "", 0, fmt.Errorf("%w: %q", errMalformedLine, line)
	}

	key := line[:idx]

	count, err := strconv.ParseInt(line[idx+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid count in line %q: %w", line, err)
	}

	return key, count, nil
}

func writeProfile(w io.Writer, mode string, bc *blockCounts) error {
	bw := bufio.NewWriter(w)
	if _, err := fmt.Fprintf(bw, "mode: %s\n", mode); err != nil {
		return err
	}

	for _, key := range bc.keys {
		if _, err := fmt.Fprintf(bw, "%s %d\n", key, bc.counts[key]); err != nil {
			return err
		}
	}

	return bw.Flush()
}
