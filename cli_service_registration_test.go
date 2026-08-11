package main

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetServiceProviders_MatchesServiceDirectories guards against gopherstack-91e0:
// opsworks had real code, tests, and an "A" PARITY.md grade, but no Provider{} entry
// in cli.go's getServiceProviders chain at all -- silently unreachable at runtime,
// which made its parity grade meaningless. This diffs services/*/ against every
// "&aliasbackend.Provider{}"-shaped entry in cli.go's source, so a new or
// accidentally-dropped registration fails the build instead of surfacing years later
// in an audit of a service that was never actually running.
func TestGetServiceProviders_MatchesServiceDirectories(t *testing.T) {
	t.Parallel()

	// servicesWithNoProviderEntry lists services/ directories deliberately excluded
	// from cli.go's getServiceProviders chain, with why. Anything else unwired is the
	// gopherstack-91e0 bug class.
	servicesWithNoProviderEntry := map[string]string{
		"qldb":        "AWS deprecated QLDB (EOS 2025-07-31); removed, see services/qldb/README.md",
		"qldbsession": "AWS deprecated QLDB (EOS 2025-07-31); removed, see services/qldbsession/README.md",
	}

	serviceImportRE := regexp.MustCompile(
		`(?m)^\s*(\w+)\s+"github\.com/blackbirdworks/gopherstack/services/([^"]+)"\s*$`,
	)
	providerEntryRE := regexp.MustCompile(`&(\w+)\.\w*Provider\{\}`)

	dirEntries, err := os.ReadDir("services")
	require.NoError(t, err)

	allDirs := make(map[string]bool, len(dirEntries))

	for _, e := range dirEntries {
		if e.IsDir() {
			allDirs[e.Name()] = true
		}
	}

	src, err := os.ReadFile("cli.go")
	require.NoError(t, err)

	aliasToDir := make(map[string]string)
	for _, m := range serviceImportRE.FindAllStringSubmatch(string(src), -1) {
		aliasToDir[m[1]] = m[2]
	}

	registeredDirs := make(map[string]bool)
	for _, m := range providerEntryRE.FindAllStringSubmatch(string(src), -1) {
		if dir, ok := aliasToDir[m[1]]; ok {
			registeredDirs[dir] = true
		}
	}

	wantDirs := make(map[string]bool, len(allDirs))
	for dir := range allDirs {
		if _, excluded := servicesWithNoProviderEntry[dir]; !excluded {
			wantDirs[dir] = true
		}
	}

	require.Equal(t, wantDirs, registeredDirs,
		"services/*/ directories must exactly match cli.go's registered providers, "+
			"minus servicesWithNoProviderEntry's explicit exclusions")
}
