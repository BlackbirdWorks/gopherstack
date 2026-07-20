// Package version holds the build-time version information injected via ldflags.
package version

import "runtime/debug"

const devVersion = "dev"

// Build is set at build time via:
//
//	go build -ldflags "-X github.com/blackbirdworks/gopherstack/pkgs/version.Build=<tag>"
//
// When built without ldflags (e.g. go run or local dev build) it defaults to "dev".
var Build = devVersion //nolint:gochecknoglobals // Build information is standard as a global

var readBuildInfo = debug.ReadBuildInfo //nolint:gochecknoglobals // Mocked in tests

// Get returns the build version. It uses the ldflags injected version if available,
// falling back to [debug.BuildInfo] for standard go installations, and finally "dev".
func Get() string {
	if Build != devVersion {
		return Build
	}
	if info, ok := readBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
	}

	return devVersion
}
