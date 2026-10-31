package app

import (
	"github.com/blackbirdworks/gopherstack/internal/routegates"

	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Regenerate: go run ./cmd/routingcorpus, then UPDATE_ROUTING_GOLDEN=1 go test -run TestRoutingEquivalence .
const (
	routingCorpusPath   = "testdata/routing/corpus.tsv"
	routingUpdateEnv    = "UPDATE_ROUTING_GOLDEN"
	routingNoMatch      = "-"
	routingCorpusFields = 10
	routingMaxDiffs     = 20
)

type routingCase struct {
	method, host, uri, authSvc, ctype, target, body, headers, wantScan, wantLookup string
}

//nolint:gochecknoglobals // full production service composition built once and shared by the routing tests
var (
	routingOnce    sync.Once
	routingEntries []*service.Entry
	routingReg     *service.Registry
	errRoutingInit error
)

func routingFixture(t *testing.T) (*service.Registry, []*service.Entry) {
	t.Helper()

	routingOnce.Do(func() {
		log := buildLogger("")
		ctx := context.Background()

		cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
		cli.portAlloc = setupPortAllocatorWithReservations(ctx, log, cli)
		cli.faultStore = chaos.NewFaultStore()

		services, err := initializeServices(&service.AppContext{
			Logger: log, Config: &cli, JanitorCtx: ctx, PortAlloc: cli.portAlloc,
		})
		if err != nil {
			errRoutingInit = err

			return
		}

		routingReg = service.NewRegistry()

		for _, svc := range services {
			if err = routingReg.Register(svc); err != nil {
				errRoutingInit = err

				return
			}
		}

		routingEntries = routingReg.GetAll()
		_ = service.NewServiceRouter(routingReg)
	})

	require.NoError(t, errRoutingInit)

	return routingReg, routingEntries
}

func loadRoutingCorpus(t *testing.T) []routingCase {
	t.Helper()

	f, err := os.Open(routingCorpusPath)
	require.NoError(t, err)

	defer f.Close()

	var cases []routingCase

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)

	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		require.Len(t, fields, routingCorpusFields, "line %d", len(cases)+1)

		cases = append(cases, routingCase{
			method: fields[0], host: fields[1], uri: fields[2], authSvc: fields[3], ctype: fields[4],
			target: fields[5], body: fields[6], headers: fields[7], wantScan: fields[8], wantLookup: fields[9],
		})
	}

	require.NoError(t, sc.Err())

	return cases
}

func (rc routingCase) request(t *testing.T) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(), rc.method, "http://"+rc.host+rc.uri, strings.NewReader(rc.body),
	)
	require.NoError(t, err)

	if rc.authSvc != "" {
		req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", rc.authSvc, 1))
		req.Header.Set("X-Amz-Date", "20260101T000000Z")
	}

	if rc.ctype != "" {
		req.Header.Set("Content-Type", rc.ctype)
	}

	if rc.target != "" {
		req.Header.Set("X-Amz-Target", rc.target)
	}

	for kv := range strings.SplitSeq(rc.headers, ";") {
		if k, v, ok := strings.Cut(kv, ":"); ok {
			req.Header.Set(k, v)
		}
	}

	return req
}

func entryName(e *service.Entry) string {
	if e == nil {
		return routingNoMatch
	}

	return e.Registerable.Name()
}

func scanSelect(entries []*service.Entry, c *echo.Context) string {
	for _, e := range entries {
		if e.Matcher(c) {
			return e.Registerable.Name()
		}
	}

	return routingNoMatch
}

func writeRoutingGolden(t *testing.T, cases []routingCase, scan, lookup []string) {
	t.Helper()

	var sb strings.Builder

	for i, rc := range cases {
		fmt.Fprintf(&sb, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			rc.method, rc.host, rc.uri, rc.authSvc, rc.ctype, rc.target, rc.body, rc.headers, scan[i], lookup[i])
	}

	require.NoError(t, os.WriteFile(routingCorpusPath, []byte(sb.String()), 0o600))
}

func selectAll(t *testing.T, cases []routingCase, pick func(c *echo.Context) string) []string {
	t.Helper()

	got := make([]string, len(cases))
	for i, rc := range cases {
		got[i] = pick(echo.NewContext(rc.request(t), httptest.NewRecorder()))
	}

	return got
}

func TestRoutingEquivalence(t *testing.T) {
	t.Parallel()

	reg, entries := routingFixture(t)
	cases := loadRoutingCorpus(t)

	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())
	scan := selectAll(t, cases, func(c *echo.Context) string { return scanSelect(entries, c) })
	lookup := selectAll(t, cases, func(c *echo.Context) string { return entryName(router.Lookup(c)) })

	if os.Getenv(routingUpdateEnv) != "" {
		writeRoutingGolden(t, cases, scan, lookup)

		return
	}

	tests := []struct {
		want func(rc routingCase) string
		name string
		got  []string
	}{
		{name: "priority_scan", got: scan, want: func(rc routingCase) string { return rc.wantScan }},
		{name: "router_lookup", got: lookup, want: func(rc routingCase) string { return rc.wantLookup }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var diffs []string

			for i, rc := range cases {
				if tt.got[i] != tt.want(rc) && len(diffs) < routingMaxDiffs {
					diffs = append(diffs, fmt.Sprintf("line %d %s %s%s auth=%q target=%q body=%q: got %s want %s",
						i+1, rc.method, rc.host, rc.uri, rc.authSvc, rc.target, rc.body, tt.got[i], tt.want(rc)))
				}
			}

			assert.Empty(t, diffs, "routing decisions changed vs golden")
		})
	}
}

func TestRoutingCorpusCoversServices(t *testing.T) {
	t.Parallel()

	_, entries := routingFixture(t)
	cases := loadRoutingCorpus(t)

	selected := map[string]int{}
	for _, rc := range cases {
		selected[rc.wantScan]++
	}

	var missing []string

	neverMatch := []string{
		"AzureAppConfig", "AzureARM", "AzureBlob", "AzureEventGrid", "AzureEventHubs", "AzureKeyVault", "AzureQueue",
		"AzureFile", "AzureGraph", "AzureMonitor", "AzureServiceBus", "AzureStorageVHost", "AzureTable", "CosmosDB",
		"GCPStorage", "GCPPubSub", "GCPSecretManager", "GCPKMS", "GCPCloudTasks", "GCPScheduler",
		"GCPFirestore", "GCPIAM", "GCPRun", "GCPFunctions", "GCPBigQuery", "GCPSQL", "GCPLogging",
		"GCPMonitoring",
		"GCPRedis", "AzureRedis", "AzureSQL", "GCPSpanner", "GCPBigtable", "GCPCloudBuild", "GCPEventarc",
		"GCPWorkflows", "GCPDNS", "GCPCompute", "GCPDatastore", "GCPContainer",
		"GCPIdentityToolkit", "GCPAPIGateway", "GCPBatch",
		"AzureContainerRegistry", "AzureFunctions", "GCPArtifactRegistry", "OCIObjectStorage",
		"OCIVault", "OCIStreaming", "OCIIdentity", "OCIFunctions", "OCICore", "OCIQueue", "OCINotifications",
		"OCIContainerEngine", "OCIMySQL", "OCILogging", "OCIMonitoring", "OCINoSQL", "OCIAPIGateway", "OCIEvents",
		"OCILoadBalancer", "OCIArtifacts",
	}

	for _, e := range entries {
		if selected[e.Registerable.Name()] == 0 && !slices.Contains(neverMatch, e.Registerable.Name()) {
			missing = append(missing, e.Registerable.Name())
		}
	}

	slices.Sort(missing)
	assert.Empty(t, missing, "services never selected by the corpus")
}

func cborCovered(name string, rc routingCase) bool {
	shape := routegates.CBORGates()[name]
	path, _ := splitURI(rc.uri)

	return shape != "" && strings.HasPrefix(path, "/service/"+shape+"/operation/")
}

func hasTargetPrefix(target string, prefixes []string) bool {
	return slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(target, p) })
}
