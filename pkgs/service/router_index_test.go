package service

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

type diffService struct {
	match    Matcher
	name     string
	priority int
}

func (d *diffService) Name() string                          { return d.name }
func (d *diffService) Handler() echo.HandlerFunc             { return func(*echo.Context) error { return nil } }
func (d *diffService) RouteMatcher() Matcher                 { return d.match }
func (d *diffService) GetSupportedOperations() []string      { return nil }
func (d *diffService) ExtractOperation(*echo.Context) string { return "" }
func (d *diffService) ExtractResource(*echo.Context) string  { return "" }
func (d *diffService) MatchPriority() int                    { return d.priority }

func diffTargets() []string { return []string{"Alpha.", "Alpine.", "Beta_1.", "B", "Gamma", "Delta."} }

func diffPaths() []string { return []string{"/alpha", "/al", "/beta/x", "/b", "/gamma", "/delta"} }

func hasTarget(c *echo.Context, p string) bool {
	return strings.HasPrefix(c.Request().Header.Get(amzTargetHeader), p)
}

func hasPath(c *echo.Context, p string) bool { return strings.HasPrefix(c.Request().URL.Path, p) }

func pick(rng *rand.Rand, xs []string) string { return xs[rng.IntN(len(xs))] }

func diffRequest(rng *rand.Rand) *echo.Context {
	path := pick(rng, diffPaths())

	switch rng.IntN(5) {
	case 0:
		path = "/"
	case 1:
		path += "/tail"
	case 2:
		path = path[:1+rng.IntN(len(path))]
	}

	req := httptest.NewRequest(http.MethodPost, "http://localhost"+path, strings.NewReader(""))

	if rng.IntN(3) > 0 {
		req.Header.Set(amzTargetHeader, pick(rng, diffTargets())+"Op")
	}

	if rng.IntN(4) == 0 {
		req.Header.Set("Host-Hint", "yes")
	}

	return echo.NewContext(req, httptest.NewRecorder())
}

// diffFixture builds services of every gate shape; each declared gate is sound for its matcher.
func diffFixture(rng *rand.Rand, n int) *Router {
	reg := NewRegistry()
	targetGates := map[string][]string{}
	pathGates := map[string][]string{}

	for i := range n {
		name := fmt.Sprintf("svc%d", i)
		tp := pick(rng, diffTargets())
		pp := pick(rng, diffPaths())
		svc := &diffService{name: name, priority: []int{100, 95, 90, 50, 0}[rng.IntN(5)]}

		switch rng.IntN(5) {
		case 0:
			svc.match = func(c *echo.Context) bool { return hasTarget(c, tp) }
			targetGates[name] = []string{tp}
		case 1:
			svc.match = func(c *echo.Context) bool { return hasPath(c, pp) }
			pathGates[name] = []string{pp}
		case 2:
			svc.match = func(c *echo.Context) bool {
				return hasPath(c, pp) && hasTarget(c, tp)
			}
			targetGates[name] = []string{tp}
			pathGates[name] = []string{pp}
		case 3:
			svc.match = func(c *echo.Context) bool {
				return c.Request().Header.Get("Host-Hint") != "" && c.Request().URL.Path == pp
			}
		default:
			svc.match = func(c *echo.Context) bool {
				return hasPath(c, pp) || hasTarget(c, tp)
			}
		}

		if err := reg.Register(svc); err != nil {
			panic(err)
		}
	}

	return NewServiceRouter(reg).WithTargetGates(targetGates).WithPathGates(pathGates)
}

func TestRouterIndexMatchesLinear(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		seed     uint64
		services int
	}{
		{"few", 1, 4},
		{"some", 2, 12},
		{"many", 3, 40},
		{"dense", 4, 120},
		{"other_seed", 5, 40},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(tt.seed, tt.seed))
			router := diffFixture(rng, tt.services)
			mismatches := 0

			for range 4000 {
				c := diffRequest(rng)
				if router.Lookup(c) != router.lookupLinear(c) {
					mismatches++
				}
			}

			assert.Zero(t, mismatches, "indexed Lookup diverged from linear scan")
		})
	}
}

func TestValidGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefixes []string
		minLen   int
		wantNil  bool
	}{
		{"path_ok", []string{"/ab", "/cd"}, 2, false},
		{"path_no_slash", []string{"ab"}, 2, true},
		{"path_root_only", []string{"/"}, 2, true},
		{"target_ok", []string{"A", "Bc"}, 1, false},
		{"target_empty_prefix", []string{""}, 1, true},
		{"no_prefixes", nil, 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := validGate(tt.prefixes, tt.minLen)
			assert.Equal(t, tt.wantNil, got == nil)
		})
	}
}
