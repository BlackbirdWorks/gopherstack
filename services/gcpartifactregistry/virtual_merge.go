package gcpartifactregistry

import (
	"bytes"
	"cmp"
	"encoding/json/v2"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	npmDistTags = "dist-tags"
	npmTimeKey  = "time"
)

var (
	pyIndexAnchorRe = regexp.MustCompile(`(?m)^<a href="[^"]*"[^>]*>([^<]*)</a><br>\n?`)
	mavenVerRe      = regexp.MustCompile(`<version>([^<]*)</version>`)
	mavenUpdateRe   = regexp.MustCompile(`<lastUpdated>(\d+)</lastUpdated>`)
)

// mergeableIndex reports whether a read of path on a repository of format is a package index whose entries a
// virtual repository combines across its upstreams.
func mergeableIndex(format artifactregistrypb.Repository_Format, path string) bool {
	switch format {
	case artifactregistrypb.Repository_NPM:
		pkg, tail := splitNPMPath(path)

		return pkg != "" && tail == "" && !strings.HasPrefix(path, "-/")
	case artifactregistrypb.Repository_PYTHON:
		rest, ok := strings.CutPrefix(path, "simple/")

		return ok && rest != "" && !strings.Contains(rest, "/")
	case artifactregistrypb.Repository_MAVEN:
		return strings.HasSuffix(path, "/"+mavenMetadata)
	default:
		return false
	}
}

// mergeIndex combines the 200 responses of several upstreams for one package index, the highest priority first.
func mergeIndex(format artifactregistrypb.Repository_Format, hits []*bufResponse) ([]byte, bool) {
	switch format {
	case artifactregistrypb.Repository_NPM:
		return mergeNPM(hits)
	case artifactregistrypb.Repository_PYTHON:
		return mergePython(hits), true
	case artifactregistrypb.Repository_MAVEN:
		return mergeMaven(hits), true
	default:
		return nil, false
	}
}

func mergeNPM(hits []*bufResponse) ([]byte, bool) {
	var out map[string]any

	for _, h := range hits {
		var doc map[string]any
		if err := json.Unmarshal(h.body.Bytes(), &doc, jsonx.V1()); err != nil {
			return nil, false
		}

		if out == nil {
			out = doc

			continue
		}

		for _, key := range []string{"versions", npmDistTags, npmTimeKey} {
			into, _ := out[key].(map[string]any)
			from, _ := doc[key].(map[string]any)

			if into == nil {
				into = map[string]any{}
			}

			for k, v := range from {
				if _, have := into[k]; !have {
					into[k] = v
				}
			}

			out[key] = into
		}
	}

	raw, err := json.Marshal(out, jsonx.V1())

	return raw, err == nil
}

func mergePython(hits []*bufResponse) []byte {
	first := hits[0].body.String()
	seen := map[string]bool{}

	var anchors strings.Builder

	for _, h := range hits {
		for _, m := range pyIndexAnchorRe.FindAllStringSubmatch(h.body.String(), -1) {
			if !seen[m[1]] {
				seen[m[1]] = true

				anchors.WriteString(strings.TrimSuffix(m[0], "\n") + "\n")
			}
		}
	}

	loc := pyIndexAnchorRe.FindStringIndex(first)
	if loc == nil {
		return []byte(first)
	}

	tail := strings.LastIndex(first, "</body>")

	return []byte(first[:loc[0]] + anchors.String() + first[tail:])
}

func mergeMaven(hits []*bufResponse) []byte {
	var versions []string

	updated := ""

	for _, h := range hits {
		for _, m := range mavenVerRe.FindAllStringSubmatch(h.body.String(), -1) {
			if !slices.Contains(versions, m[1]) {
				versions = append(versions, m[1])
			}
		}

		if m := mavenUpdateRe.FindStringSubmatch(h.body.String()); m != nil && m[1] > updated {
			updated = m[1]
		}
	}

	slices.SortStableFunc(versions, compareVersions)

	body := hits[0].body.String()

	head, _, found := strings.Cut(body, "<versioning>")
	if !found {
		return []byte(body)
	}

	latest, release := versions[len(versions)-1], ""

	for _, v := range slices.Backward(versions) {
		if !strings.HasSuffix(v, snapshotSfx) {
			release = v

			break
		}
	}

	var vs bytes.Buffer
	for _, v := range versions {
		vs.WriteString("      <version>" + v + "</version>\n")
	}

	out := head + "<versioning>\n    <latest>" + latest + "</latest>\n"
	if release != "" {
		out += "    <release>" + release + "</release>\n"
	}

	out += "    <versions>\n" + vs.String() + "    </versions>\n    <lastUpdated>" + updated +
		"</lastUpdated>\n  </versioning>\n</metadata>\n"

	return []byte(out)
}

func writeMerged(w http.ResponseWriter, first *bufResponse, body []byte) {
	h := w.Header()
	if ct := first.header.Get("Content-Type"); ct != "" {
		h.Set("Content-Type", ct)
	}

	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// compareVersions orders dotted versions segment by segment, numerically where both segments are numbers.
func compareVersions(a, b string) int {
	as, bs := strings.FieldsFunc(a, isVersionSep), strings.FieldsFunc(b, isVersionSep)

	for i := range min(len(as), len(bs)) {
		x, xErr := strconv.Atoi(as[i])
		y, yErr := strconv.Atoi(bs[i])

		var c int

		if xErr == nil && yErr == nil {
			c = cmp.Compare(x, y)
		} else {
			c = strings.Compare(as[i], bs[i])
		}

		if c != 0 {
			return c
		}
	}

	return cmp.Compare(len(as), len(bs))
}

func isVersionSep(r rune) bool { return r == '.' || r == '-' }
