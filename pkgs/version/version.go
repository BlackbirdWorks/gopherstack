// Package version holds the build-time version information injected via ldflags.
package version

// Build is set at build time via:
//
//	go build -ldflags "-X github.com/blackbirdworks/gopherstack/pkgs/version.Build=<tag>"
//
// When built without ldflags (e.g. go run or local dev build) it defaults to "dev".
var Build = "dev" //nolint:gochecknoglobals // Build information is standard as a global
