package app

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/routegates"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	holdChunks      = 8
	holdShortStride = 4
)

// holdGate is one gate under proof: covered reports whether the gate admits the request;
// entry's matcher must return false for every request it does not admit.
type holdGate struct {
	entry   *service.Entry
	covered func(rc routingCase, req *http.Request) bool
	name    string
}

// sweepHolds builds each variant request once and checks every gate against it, so the
// request set is shared across gates instead of rebuilt per gate.
func sweepHolds(t *testing.T, gates []holdGate, cases []routingCase, variants func(routingCase) []routingCase) {
	t.Helper()

	chunk := (len(cases) + holdChunks - 1) / holdChunks

	var seen sync.Map

	for i := range holdChunks {
		part := cases[min(i*chunk, len(cases)):min((i+1)*chunk, len(cases))]

		t.Run("chunk", func(t *testing.T) {
			t.Parallel()

			for _, rc := range part {
				for _, v := range variants(rc) {
					if _, dup := seen.LoadOrStore(v, struct{}{}); dup {
						continue
					}

					req := v.request(t)

					for _, g := range gates {
						if g.covered(v, req) {
							continue
						}

						require.False(t, g.entry.Matcher(echo.NewContext(req, httptest.NewRecorder())),
							"%s matched %s %s%s outside its gate (target=%q auth=%q headers=%q)",
							g.name, v.method, v.host, v.uri, v.target, v.authSvc, v.headers)
					}
				}
			}
		})
	}
}

func holdEntries(t *testing.T) map[string]*service.Entry {
	t.Helper()

	_, entries := routingFixture(t)
	byName := map[string]*service.Entry{}

	for _, e := range entries {
		byName[e.Registerable.Name()] = e
	}

	return byName
}

func TestRoutePathGatesHold(t *testing.T) {
	t.Parallel()

	byName := holdEntries(t)

	pathGates := routegates.PathGates()
	gates := make([]holdGate, 0, len(pathGates))

	for name, prefixes := range pathGates {
		require.NotNil(t, byName[name], "gate names an unregistered service")

		gates = append(
			gates,
			holdGate{name: name, entry: byName[name], covered: func(_ routingCase, req *http.Request) bool {
				path := "/" + strings.TrimLeft(req.URL.Path, "/")

				return slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(path, p) })
			}},
		)
	}

	cases := loadRoutingCorpus(t)
	arnRows := make(map[routingCase]bool, len(cases))

	for _, rc := range cases {
		arnRows[rc] = !testing.Short()
	}

	if testing.Short() {
		for _, rc := range strideSample(cases, holdShortStride) {
			arnRows[rc] = true
		}
	}

	sweepHolds(t, gates, cases, func(rc routingCase) []routingCase {
		if !arnRows[rc] {
			return slices.Concat([]routingCase{rc}, pathVariants(rc))
		}

		return slices.Concat([]routingCase{rc}, pathVariants(rc), arnVariants(rc))
	})
}

func TestRouteTargetGatesHold(t *testing.T) {
	t.Parallel()

	byName := holdEntries(t)

	targetGates := routegates.TargetGates()
	gates := make([]holdGate, 0, len(targetGates))

	var all []string

	for name, prefixes := range targetGates {
		require.NotNil(t, byName[name], "gate names an unregistered service")

		all = append(all, prefixes...)
		gates = append(
			gates,
			holdGate{name: name, entry: byName[name], covered: func(rc routingCase, _ *http.Request) bool {
				return hasTargetPrefix(rc.target, prefixes) || cborCovered(name, rc)
			}},
		)
	}

	targets := []string{"", "Zz.Op", "Zz" + strings.Join(all, "")}

	sweepHolds(t, gates, loadRoutingCorpus(t), func(rc routingCase) []routingCase {
		out := make([]routingCase, 0, len(targets))

		for _, target := range targets {
			rc.target = target
			out = append(out, rc)
		}

		return out
	})
}

func TestRouteAltGatesHold(t *testing.T) {
	t.Parallel()

	byName := holdEntries(t)
	altGates := routegates.AltGates()

	gates := make([]holdGate, 0, len(altGates))

	for name, gate := range altGates {
		require.NotNil(t, byName[name], "gate names an unregistered service")

		gates = append(
			gates,
			holdGate{name: name, entry: byName[name], covered: func(rc routingCase, req *http.Request) bool {
				return altAdmits(gate, req, rc)
			}},
		)
	}

	cases := loadRoutingCorpus(t)
	sampled := make(map[routingCase]bool)

	for _, rc := range strideSample(cases, altRowStride) {
		sampled[rc] = true
	}

	sweepHolds(t, gates, cases, func(rc routingCase) []routingCase {
		out := slices.Concat([]routingCase{rc}, pathVariants(rc))
		if !sampled[rc] {
			return out
		}

		seen := make(map[routingCase]bool)

		for _, g := range altGates {
			for _, v := range altVariants(rc, g) {
				if !seen[v] {
					seen[v] = true
					out = append(out, v)
				}
			}
		}

		return out
	})
}
