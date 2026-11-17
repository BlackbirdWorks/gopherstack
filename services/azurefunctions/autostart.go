package azurefunctions

import (
	"archive/zip"
	"bytes"
	"encoding/json/v2"
	"io"
	"path"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// hasBackgroundTrigger reports whether a package declares an enabled non-HTTP trigger, in a function.json file or
// in application code.
func hasBackgroundTrigger(zipData []byte) bool {
	return hasJSONTrigger(zipData) || hasCodeFirstTrigger(zipData)
}

// triggersUnknown reports whether the package's triggers cannot be listed: no package (a container image), an
// unreadable archive, a function.json that does not parse, or a scanned file past the size cap. Such an app is
// never reaped for idleness.
func triggersUnknown(zipData []byte) bool {
	if len(zipData) == 0 {
		return true
	}

	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return true
	}

	for _, f := range zr.File {
		dir, base := path.Split(f.Name)
		switch {
		case base == "function.json" && strings.Count(dir, "/") == 1 && validFunctionName(strings.TrimSuffix(dir, "/")):
			if !functionJSONParses(f) {
				return true
			}
		case !isDependencyPath(f.Name) && f.UncompressedSize64 > maxScanBytes && isScannedExt(f.Name):
			return true
		}
	}

	return false
}

func isScannedExt(name string) bool {
	switch strings.ToLower(path.Ext(name)) {
	case ".js", ".mjs", ".cjs", ".py", extDLL:
		return true
	}

	return false
}

func functionJSONParses(f *zip.File) bool {
	rc, err := f.Open()
	if err != nil {
		return false
	}
	defer rc.Close()

	raw, err := io.ReadAll(io.LimitReader(rc, maxUnpacked))

	var cfg map[string]any

	return err == nil && json.Unmarshal(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")), &cfg, jsonx.V1()) == nil
}

func hasJSONTrigger(zipData []byte) bool {
	for _, def := range scanFunctions(zipData) {
		if disabled, _ := def.Config["disabled"].(bool); disabled {
			continue
		}

		bindings, _ := def.Config["bindings"].([]any)
		for _, raw := range bindings {
			bd, _ := raw.(map[string]any)
			t, _ := bd["type"].(string)

			if strings.HasSuffix(strings.ToLower(t), "trigger") && !strings.EqualFold(t, "httpTrigger") {
				return true
			}
		}
	}

	return false
}

// autoStart starts the host of an app whose triggers fire without a request (timer, queue, blob, ...), the way
// Azure runs it as soon as it is deployed. Apps with only HTTP triggers still start on their first request.
func (b *InMemoryBackend) autoStart(name string) {
	b.mu.Lock("autoStart")
	defer b.mu.Unlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok || a.Stopped || a.containerID != "" || a.starting != nil || b.runtime == nil || a.isWeb() ||
		!hasBackgroundTrigger(a.Zip) {
		return
	}

	ctx := b.runCtx

	b.wg.Go(func() {
		if _, err := b.ensureStarted(ctx, name); err != nil {
			logger.Load(ctx).DebugContext(ctx, "azurefunctions: background trigger host did not start",
				"app", name, "error", err)
		}
	})
}
