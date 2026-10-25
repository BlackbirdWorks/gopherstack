// Command retypesgen regenerates services/resourceexplorer2/supported_types.json from the AWS Resource
// Explorer "supported resource types" documentation page.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const fetchTimeout = time.Minute

var errFetch = errors.New("fetch failed")

const source = "https://docs.aws.amazon.com/resource-explorer/latest/userguide/supported-resource-types.html"

var (
	typeRe = regexp.MustCompile(`<code class="code">([a-z0-9-]+:[A-Za-z0-9_./-]+)</code>`)
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

	n, err := run(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "%d types\n", n)
}

func run(out string) (int, error) {
	c := httpclient.New(fetchTimeout)

	resp, err := c.Get(source) //nolint:noctx // one-shot generator
	if err != nil {
		return 0, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%w: %s", errFetch, resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	types := parse(string(b))

	data, err := json.MarshalIndent(catalog{Source: source, Types: types}, "", " ")
	if err != nil {
		return 0, err
	}

	return len(types), os.WriteFile(out, append(data, '\n'), 0o600)
}
