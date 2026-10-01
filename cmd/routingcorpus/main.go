// Command routingcorpus regenerates testdata/routing/corpus.tsv request columns
// from the pinned aws-sdk-go-v2 sources, keeping recorded expectations per request.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const (
	corpusPath = "testdata/routing/corpus.tsv"
	sdkPrefix  = "github.com/aws/aws-sdk-go-v2/service/"
	pending    = "-"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "routingcorpus:", err)
		os.Exit(1)
	}
}

func run() error {
	sdkRows, err := collectSDKRows()
	if err != nil {
		return err
	}

	known, order := loadExpectations(corpusPath)
	rows := pinLegacyOrder(expandRows(sdkRows), order)

	var sb strings.Builder

	for _, r := range rows {
		key := r.key()

		exp := pending + "\t" + pending

		if q := known[key]; len(q) > 0 {
			exp, known[key] = q[0], q[1:]
		}

		sb.WriteString(key + "\t" + exp + "\n")
	}

	return os.WriteFile(corpusPath, []byte(sb.String()), 0o600)
}

func loadExpectations(path string) (map[string][]string, map[string]int) {
	known := map[string][]string{}
	order := map[string]int{}

	data, err := os.ReadFile(path)
	if err != nil {
		return known, order
	}

	for i, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		f := strings.Split(line, "\t")
		if len(f) == corpusFields {
			k := strings.Join(f[:requestFields], "\t")
			known[k] = append(known[k], f[requestFields]+"\t"+f[requestFields+1])

			if _, ok := order[k]; !ok {
				order[k] = i
			}
		}
	}

	return known, order
}

// pinLegacyOrder keeps AppStream legacy-JSON rows in their recorded order, which came from a map.
func pinLegacyOrder(rows []row, order map[string]int) []row {
	var slots []int

	var legacy []row

	for i, r := range rows {
		if strings.HasPrefix(r.target, "PhotonAdminProxyService.") {
			slots = append(slots, i)
			legacy = append(legacy, r)
		}
	}

	sort.SliceStable(legacy, func(a, b int) bool {
		ia, oka := order[legacy[a].key()]
		ib, okb := order[legacy[b].key()]

		return oka && (!okb || ia < ib)
	})

	for n, i := range slots {
		rows[i] = legacy[n]
	}

	return rows
}

func sdkDirs() ([][2]string, error) {
	out, err := exec.CommandContext(
		context.Background(), "go", "list", "-m", "-f", "{{.Path}} {{.Dir}}", "all",
	).Output()
	if err != nil {
		return nil, fmt.Errorf("go list -m all: %w", err)
	}

	var dirs [][2]string

	for line := range strings.SplitSeq(string(out), "\n") {
		path, dir, ok := strings.Cut(line, " ")
		if ok && strings.Contains(path, sdkPrefix) && dir != "" {
			dirs = append(dirs, [2]string{path[strings.LastIndex(path, "/")+1:], dir})
		}
	}

	return dirs, nil
}
