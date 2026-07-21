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

// ReadBuildInfo is the function used to get build info. Exported for tests.
var ReadBuildInfo = debug.ReadBuildInfo //nolint:gochecknoglobals // Mutable for testing

// Get returns the build version. It uses the ldflags injected version if available,
// falling back to [debug.BuildInfo] for standard go installations, and finally "dev".
func Get() string {
	if Build != devVersion {
		return Build
	}
	if info, ok := ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
	}

	return devVersion
}
