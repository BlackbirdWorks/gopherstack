package cloudformation_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// The fixtures under testdata/sam_translator are copied from the AWS SAM translator test suite
// (see testdata/sam_translator/NOTICE.md); each output is the translator's own result for its input.

const samGoldenDir = "testdata/sam_translator"

const samGoldenTransform = "Transform: AWS::Serverless-2016-10-31\n"

// samGoldenRegion is the region the translator's own test suite patches into boto3.
const samGoldenRegion = "ap-southeast-1"

var (
	samHashSuffixRe = regexp.MustCompile(`[0-9a-f]{10}$`)
	samDigestRe     = regexp.MustCompile(`^RestApi deployment id: [0-9a-f]{40}$`)
)

// samGoldenIDOnly lists fixtures whose resource ids and types must match the translator but whose
// properties differ in a documented way.
func samGoldenIDOnly() map[string]string {
	return map[string]string{}
}

func jsonString(t *testing.T, v any) string {
	t.Helper()

	raw, err := json.Marshal(v)
	require.NoError(t, err)

	return string(raw)
}

// normalizeSAMOutput maps a translated Resources section to a comparable form: hash suffixes of
// generated ids collapse (the Deployment hash input is sorted JSON here, Python's dict repr in SAM),
// "arn:${AWS::Partition}:" reads as the "aws" partition SAM bakes in, constant Fn::Sub wrappers
// collapse, and lists sort like SAM's own test comparison.
func normalizeSAMOutput(t *testing.T, res map[string]any) map[string]any {
	t.Helper()

	idmap := hashedIDMap(t, res)

	var walk func(v any) any
	walk = func(v any) any {
		switch val := v.(type) {
		case string:
			for from, to := range idmap {
				val = strings.ReplaceAll(val, from, to)
			}
			val = strings.ReplaceAll(val, "arn:${AWS::Partition}:", "arn:aws:")
			if samDigestRe.MatchString(val) {
				val = "RestApi deployment id: #"
			}

			return val
		case []any:
			out := make([]any, len(val))
			for i, e := range val {
				out[i] = walk(e)
			}
			slices.SortFunc(out, func(a, b any) int { return strings.Compare(jsonString(t, a), jsonString(t, b)) })

			return out
		case map[string]any:
			if sub, ok := val["Fn::Sub"].(string); ok && len(val) == 1 {
				if w, _ := walk(sub).(string); !strings.Contains(w, "${") {
					return w
				}
			}
			out := make(map[string]any, len(val))
			for k, e := range val {
				key, _ := walk(k).(string)
				out[key] = walk(e)
			}

			return out
		}

		return v
	}

	out, _ := walk(res).(map[string]any)

	return out
}

func samGoldenFixtures(t *testing.T, errors bool) []string {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(samGoldenDir, "input", "*.yaml"))
	require.NoError(t, err)

	var names []string
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".yaml")
		if strings.HasPrefix(name, "error_") == errors {
			names = append(names, name)
		}
	}
	require.NotEmpty(t, names)

	return names
}

func transformGolden(t *testing.T, name string) (string, error) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(samGoldenDir, "input", name+".yaml"))
	require.NoError(t, err)
	body := string(raw)
	if !strings.HasSuffix(body, "\n") {
		body += "\n"
	}
	if !strings.Contains(body, "\nTransform:") && !strings.HasPrefix(body, "Transform:") {
		// A "%YAML 1.1" directive has to stay ahead of the document start marker.
		if head, rest, found := strings.Cut(body, "---\n"); found && strings.HasPrefix(body, "%") {
			body = head + "---\n" + samGoldenTransform + rest
		} else {
			body = samGoldenTransform + body
		}
	}

	return cloudformation.ProcessedTemplateBody(body, samGoldenRegion)
}

func resourceTypes(res map[string]any) map[string]string {
	out := make(map[string]string, len(res))
	for id, r := range res {
		out[id], _ = r.(map[string]any)["Type"].(string)
	}

	return out
}

func TestSAMGolden_Translator(t *testing.T) {
	t.Parallel()

	idOnly := samGoldenIDOnly()
	for _, name := range samGoldenFixtures(t, false) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wantRaw, err := os.ReadFile(filepath.Join(samGoldenDir, "output", name+".json"))
			require.NoError(t, err)
			var want map[string]any
			require.NoError(t, json.Unmarshal(wantRaw, &want))

			processed, err := transformGolden(t, name)
			require.NoError(t, err)
			var doc map[string]any
			require.NoError(t, json.Unmarshal([]byte(processed), &doc))

			got := normalizeSAMOutput(t, doc["Resources"].(map[string]any))
			exp := normalizeSAMOutput(t, want["Resources"].(map[string]any))
			require.Equal(t, resourceTypes(exp), resourceTypes(got), "logical ids and types")

			if reason, skip := idOnly[name]; skip {
				t.Log("properties not compared:", reason)

				return
			}
			for id, r := range exp {
				assert.JSONEq(t, jsonString(t, r), jsonString(t, got[id]), id)
			}
		})
	}
}

func TestSAMGolden_Errors(t *testing.T) {
	t.Parallel()

	for _, name := range samGoldenFixtures(t, true) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := transformGolden(t, name)
			require.ErrorIs(t, err, cloudformation.ErrSAMTransform)
		})
	}
}

// hashedIDMap maps ids ending in a SAM hash to a stable name: the prefix plus "#", with an index when several
// ids share a prefix (ordered by their content, so equal templates number alike).
func hashedIDMap(t *testing.T, res map[string]any) map[string]string {
	t.Helper()

	groups := map[string][]string{}
	for id, r := range res {
		typ, _ := r.(map[string]any)["Type"].(string)
		if samHashSuffixRe.MatchString(id) && strings.HasPrefix(typ, "AWS::") {
			key := samHashSuffixRe.ReplaceAllString(id, "#")
			groups[key] = append(groups[key], id)
		}
	}
	plain := map[string]string{}
	for key, ids := range groups {
		for _, id := range ids {
			plain[id] = key
		}
	}
	strip := func(r any) string {
		raw := jsonString(t, r)
		for from, to := range plain {
			raw = strings.ReplaceAll(raw, from, to)
		}

		return raw
	}
	out := map[string]string{}
	for key, ids := range groups {
		slices.SortFunc(ids, func(a, b string) int { return strings.Compare(strip(res[a]), strip(res[b])) })
		for i, id := range ids {
			out[id] = key
			if len(ids) > 1 {
				out[id] = key + strconv.Itoa(i)
			}
		}
	}

	return out
}
