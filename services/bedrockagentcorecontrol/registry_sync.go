package bedrockagentcorecontrol

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	keySyncConfig       = "synchronizationConfiguration"
	keySyncType         = "synchronizationType"
	keySyncRecord       = "synchronization"
	keyTriggerSync      = "triggerSynchronization"
	syncTypeURL         = "URL"
	reasonConfiguration = "configuration"
	maxSyncCreds        = 1
)

func syncFromURL(doc map[string]any) map[string]any {
	return objOf(objOf(doc, keySyncConfig), "fromUrl")
}

func hasSynchronization(doc, patch map[string]any) bool {
	if v, ok := patch[keySyncConfig]; ok {
		return mapOf(v) != nil && syncFromURL(patch) != nil
	}

	return syncFromURL(doc) != nil
}

// validateSynchronization checks the synchronization members of a registry record input; the https-only URL
// and the single credential provider come from the FromUrlSynchronizationConfiguration member docs in the SDK.
func validateSynchronization(in map[string]any) error {
	if t := strOf(in, keySyncType); t != "" && t != syncTypeURL {
		return invalid("synchronizationType %q must be %s", t, syncTypeURL)
	}

	cfg := mapOf(in[keySyncConfig])
	if cfg == nil {
		return nil
	}

	from := objOf(cfg, "fromUrl")
	if from == nil {
		return invalid("synchronizationConfiguration must set fromUrl")
	}

	u, err := url.Parse(strOf(from, "url"))
	if err != nil || u.Scheme != schemeHTTPS || u.Host == "" {
		return invalid("synchronizationConfiguration.fromUrl.url must be an https URL")
	}

	if len(listOf(from, "credentialProviderConfigurations")) > maxSyncCreds {
		return invalid("at most %d credential provider configuration can be specified", maxSyncCreds)
	}

	return nil
}

// recordSynchronization notes a requested synchronization of a registry record from its URL source. The
// emulator performs no outbound fetch of record metadata, so the request is persisted and logged only.
func (b *InMemoryBackend) recordSynchronization(ctx context.Context, r *Resource, patch map[string]any) {
	from := syncFromURL(r.Doc)
	if from == nil {
		delete(r.Aux2(), keySyncRecord)

		return
	}

	_, changed := patch[keySyncConfig]
	trigger, _ := patch[keyTriggerSync].(bool)

	if !changed && !trigger {
		return
	}

	reason := reasonConfiguration
	if trigger {
		reason = "trigger"
	}

	r.Aux2()[keySyncRecord] = map[string]any{
		"url":         strOf(from, "url"),
		"reason":      reason,
		"requestedAt": b.now().UTC().Format(time.RFC3339Nano),
	}

	logger.Load(ctx).InfoContext(ctx, "bedrockagentcorecontrol: would synchronize registry record from URL",
		"record", r.ID, "host", hostOf(strOf(from, "url")), "reason", reason)
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}

	return strings.ToLower(u.Host)
}
