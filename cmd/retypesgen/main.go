// Command retypesgen regenerates services/resourceexplorer2/supported_types.json from the AWS Resource
// Explorer "supported resource types" documentation page.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"
)

const source = "https://docs.aws.amazon.com/resource-explorer/latest/userguide/supported-resource-types.html"

var (
	typeRe    = regexp.MustCompile(`<code class="code">([a-z0-9-]+:[A-Za-z0-9_./-]+)</code>`)
)

type catalog struct {
	Source string   `json:"source"`
	Types  []string `json:"types"`
}

func parse(page string) []string {
	set := map[string]bool{}

	for _, sec := range strings.Split(page, `<h3 id="services-`)[1:] {
		sec, _, _ = strings.Cut(sec, "<h2")

		for _, m := range typeRe.FindAllStringSubmatch(sec, -1) {
			set[m[1]] = true
		}
	}

	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}

	slices.Sort(out)

	return out
}

func main() {
	out := flag.String("out", "services/resourceexplorer2/supported_types.json", "output file")
	flag.Parse()

	c := &http.Client{Timeout: 60 * time.Second, Transport: http.DefaultTransport.(*http.Transport).Clone()}

	resp, err := c.Get(source) //nolint:noctx // one-shot generator
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "fetch failed", resp.Status, err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(catalog{Source: source, Types: parse(string(b))}, "", " ")
	if err != nil || os.WriteFile(*out, append(data, '\n'), 0o644) != nil {
		fmt.Fprintln(os.Stderr, "write failed", err)
		os.Exit(1)
	}

	fmt.Println(len(parse(string(b))), "types", strings.Count(string(data), "\n"))
}
