// Command jsonv2mod rewrites encoding/json (v1) call sites to
// encoding/json/v2 plus jsontext, passing jsonx.V1() so behavior is unchanged.
//
// Usage:
//
//	go run ./cmd/jsonv2mod [-n] [-skip dir,...] path...
//
// Each path is a Go file or a directory walked recursively. Files are
// rewritten in place (-n lists them instead). Constructs that cannot be
// rewritten safely are left untouched and printed as file:line: reason; the
// exit status is 1 when any are reported. See pkgs/jsonx for the mapping.
package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	dry := flag.Bool("n", false, "list files that would change without writing")
	skip := flag.String("skip", "", "comma-separated path fragments to skip")
	flag.Parse()

	skips := strings.Split(*skip, ",")
	changed, leftovers := 0, 0

	for _, root := range flag.Args() {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() || !strings.HasSuffix(path, ".go") || skipped(path, skips) {
				return nil
			}

			res, err := processFile(path)
			if err != nil {
				return err
			}

			for _, f := range res.findings {
				fmt.Fprintf(os.Stderr, "%s:%d: %s\n", path, f.line, f.msg)
				leftovers++
			}

			if res.out == nil {
				return nil
			}

			changed++
			fmt.Println(path)

			if *dry {
				return nil
			}

			return os.WriteFile(path, res.out, 0o644)
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "jsonv2mod:", err)
			os.Exit(2)
		}
	}

	fmt.Fprintf(os.Stderr, "jsonv2mod: %d files changed, %d leftovers\n", changed, leftovers)

	if leftovers > 0 {
		os.Exit(1)
	}
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
