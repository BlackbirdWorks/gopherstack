package azurefunctions

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// hasBackgroundTrigger reports whether a package declares an enabled non-HTTP trigger in a function.json file.
func hasBackgroundTrigger(zipData []byte) bool {
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

	b.wg.Add(1)

	go func() {
		defer b.wg.Done()

		if _, err := b.ensureStarted(ctx, name); err != nil {
			logger.Load(ctx).DebugContext(ctx, "azurefunctions: background trigger host did not start",
				"app", name, "error", err)
		}
	}()
}
