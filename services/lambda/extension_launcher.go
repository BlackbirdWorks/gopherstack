package lambda

import (
	"os"
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	extensionsDir          = "extensions"
	extensionLauncherName  = ".gopherstack-launch-extensions.sh"
	extensionLauncherPath  = "/opt/" + extensionLauncherName
	extensionLauncherMode  = 0o755
	baseImageEntrypoint    = "/lambda-entrypoint.sh"
	executableBits         = 0o111
	extensionLauncherShell = `#!/bin/sh
` + extensionStartLoop + `exec "$@"
`
	extensionStartLoop = `for f in /opt/extensions/*; do
  if [ -f "$f" ] && [ -x "$f" ]; then
    ( "$f" 2>&1 | while IFS= read -r l || [ -n "$l" ]; do printf '\036gopherstack-extension\036%s\n' "$l"; done ) &
  fi
done
`
)

// listLayerExtensions returns the executable files in <layerDir>/extensions.
func listLayerExtensions(layerDir string) []string {
	entries, err := os.ReadDir(filepath.Join(layerDir, extensionsDir))
	if err != nil {
		return nil
	}

	var out []string

	for _, e := range entries {
		info, infoErr := e.Info()
		if infoErr == nil && info.Mode().IsRegular() && info.Mode().Perm()&executableBits != 0 {
			out = append(out, e.Name())
		}
	}

	return out
}

// wrapWithExtensionLauncher starts the layer's /opt/extensions/* executables
// before the runtime. The AWS base images only do this under the runtime
// interface emulator, which is bypassed once AWS_LAMBDA_RUNTIME_API is set.
func wrapWithExtensionLauncher(spec *container.Spec, layerMount string) {
	if layerMount == "" {
		return
	}

	dir := layerHostDir(layerMount)
	if len(listLayerExtensions(dir)) == 0 {
		return
	}

	script := filepath.Join(dir, extensionLauncherName)
	if err := os.WriteFile(script, []byte(extensionLauncherShell), extensionLauncherMode); err != nil { // #nosec G306
		return
	}

	_ = os.Chmod(script, extensionLauncherMode) // #nosec G302

	inner := spec.Entrypoint
	if len(inner) == 0 {
		inner = []string{baseImageEntrypoint}
	}

	spec.Entrypoint = append([]string{"/bin/sh", extensionLauncherPath}, inner...)
}
