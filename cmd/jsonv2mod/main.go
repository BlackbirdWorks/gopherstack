// Command jsonv2mod rewrites encoding/json (v1) call sites to
// encoding/json/v2 plus jsontext, passing jsonx.V1() so behavior is unchanged.
//
// Usage:
//
//	go run ./cmd/jsonv2mod [-n] [-skip frag,...] path...
//
// Each path is a Go file or a directory walked recursively. Files are
// rewritten in place (-n lists them instead). Constructs that cannot be
// rewritten safely are left untouched and printed as file:line: reason; the
// exit status is 1 when any are reported. See pkgs/jsonx for the mapping.
package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	exitLeftovers = 1
	exitFailure   = 2
	fileMode      = 0o600
)

type options struct {
	out, errOut io.Writer
	skips       []string
	dry         bool
}

func main() {
	dry := flag.Bool("n", false, "list files that would change without writing")
	skip := flag.String("skip", "", "comma-separated path fragments to skip")
	flag.Parse()

	opts := options{out: os.Stdout, errOut: os.Stderr, skips: strings.Split(*skip, ","), dry: *dry}

	sum, err := opts.run(flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "jsonv2mod:", err)
		os.Exit(exitFailure)
	}

	fmt.Fprintf(os.Stderr, "jsonv2mod: %d files changed, %d leftovers\n", sum.changed, sum.leftovers)

	if sum.leftovers > 0 {
		os.Exit(exitLeftovers)
	}
}

type summary struct{ changed, leftovers int }

func (o options) run(roots []string) (summary, error) {
	var (
		files []string
		sum   summary
	)

	for _, root := range roots {
		walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(path, ".go") && !skipped(path, o.skips) {
				files = append(files, path)
			}

			return err
		})
		if walkErr != nil {
			return sum, walkErr
		}
	}

	for _, path := range files {
		one, err := o.process(path)
		if err != nil {
			return sum, err
		}

		sum.changed += one.changed
		sum.leftovers += one.leftovers
	}

	return sum, nil
}

func (o options) process(path string) (summary, error) {
	var sum summary

	res, err := processFile(path)
	if err != nil {
		return sum, err
	}

	for _, f := range res.findings {
		fmt.Fprintf(o.errOut, "%s:%d: %s\n", path, f.line, f.msg)

		sum.leftovers++
	}

	if res.out == nil {
		return sum, nil
	}

	sum.changed++

	fmt.Fprintln(o.out, path)

	if o.dry {
		return sum, nil
	}

	return sum, os.WriteFile(path, res.out, fileMode)
}

func skipped(path string, skips []string) bool {
	slash := filepath.ToSlash(path)
	if strings.Contains(slash, "pkgs/jsonx/") || strings.Contains(slash, "cmd/jsonv2mod/") {
		return true
	}

	for _, s := range skips {
		if s != "" && strings.Contains(slash, s) {
			return true
		}
	}

	return false
}

func processFile(path string) (*result, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return rewrite(path, src)
}
