package bedrockagentcorecontrol

import (
	"encoding/json"

	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

// DataPlane returns the seam the AgentCore data plane resolves control-plane resources through.
func (b *InMemoryBackend) DataPlane() dataplane.Control { return dataSeam{b: b} }

type dataSeam struct{ b *InMemoryBackend }

func dsc(sc dataplane.Scope) scope { return scope{account: sc.Account, region: sc.Region} }

func (d dataSeam) Runtime(sc dataplane.Scope, ref, qualifier string) (dataplane.RuntimeInfo, error) {
	b := d.b
	b.mu.RLock("DataPlane.Runtime")
	defer b.mu.RUnlock()

	rt, ok := b.find(dsc(sc), b.kinds[kindRuntime], "", ref)
	if !ok {
		return dataplane.RuntimeInfo{}, dataplane.ErrNotFound
	}

	ep, ok := b.find(dsc(sc), b.kinds[kindRuntimeEndpoint], rt.ID, qualifier)
	if !ok {
		return dataplane.RuntimeInfo{}, dataplane.ErrNotFound
	}

	if b.visible(rt) != stateReady || b.visible(ep) != stateReady {
		return dataplane.RuntimeInfo{}, dataplane.ErrNotReady
	}

	live := strOf(ep.Doc, "liveVersion")

	for _, v := range rt.Versions {
		if v.ID == live {
			return runtimeInfo(rt.ID, v.Doc), nil
		}
	}

	return dataplane.RuntimeInfo{}, dataplane.ErrNotFound
}

func runtimeInfo(id string, d map[string]any) dataplane.RuntimeInfo {
	art := objOf(d, "agentRuntimeArtifact")
	life := objOf(d, "lifecycleConfiguration")

	info := dataplane.RuntimeInfo{
		ID: id, ImageURI: strOf(objOf(art, "containerConfiguration"), "containerUri"),
		Protocol:     strOf(objOf(d, "protocolConfiguration"), "serverProtocol"),
		IdleSeconds:  intOf(life, "idleRuntimeSessionTimeout"),
		MaxLifetimeS: intOf(life, "maxLifetime"),
		Env:          map[string]string{},
	}

	for k, v := range objOf(d, "environmentVariables") {
		if s, ok := v.(string); ok {
			info.Env[k] = s
		}
	}

	return info
}

func intOf(m map[string]any, k string) int {
	f, _ := m[k].(float64)

	return int(f)
}

func (d dataSeam) Memory(sc dataplane.Scope, id string) (dataplane.MemoryInfo, bool) {
	b := d.b
	b.mu.RLock("DataPlane.Memory")
	defer b.mu.RUnlock()

	r, ok := b.find(dsc(sc), b.kinds[kindMemory], "", id)
	if !ok {
		return dataplane.MemoryInfo{}, false
	}

	info := dataplane.MemoryInfo{ID: r.ID, ExpiryDays: intOf(r.Doc, "eventExpiryDuration")}

	for _, s := range listOf(r.Doc, "strategies") {
		m := mapOf(s)
		info.Strategies = append(info.Strategies, dataplane.StrategyInfo{
			ID: strOf(m, "strategyId"), Name: strOf(m, keyName), Type: strOf(m, "type"),
		})
	}

	return info, true
}

func (d dataSeam) Tool(sc dataplane.Scope, kind dataplane.ToolKind, id string) (dataplane.ToolInfo, bool) {
	b := d.b
	b.mu.RLock("DataPlane.Tool")
	defer b.mu.RUnlock()

	name := kindBrowser
	if kind == dataplane.ToolCodeInterpreter {
		name = kindCodeInterpreter
	}

	r, ok := b.find(dsc(sc), b.kinds[name], "", id)
	if !ok || b.visible(r) != stateReady {
		return dataplane.ToolInfo{}, false
	}

	return dataplane.ToolInfo{ID: r.ID, Mode: strOf(objOf(r.Doc, "networkConfiguration"), "networkMode")}, true
}

func (d dataSeam) WorkloadIdentity(sc dataplane.Scope, name string) bool {
	b := d.b
	b.mu.RLock("DataPlane.WorkloadIdentity")
	defer b.mu.RUnlock()

	_, ok := b.find(dsc(sc), b.kinds[kindWorkloadIdentity], "", name)

	return ok
}

func (d dataSeam) Provider(
	sc dataplane.Scope, kind dataplane.ProviderKind, name string,
) (dataplane.ProviderInfo, bool) {
	b := d.b
	b.mu.RLock("DataPlane.Provider")
	defer b.mu.RUnlock()

	if kind == dataplane.ProviderAPIKey {
		r, ok := b.find(dsc(sc), b.kinds[kindAPIKeyProvider], "", name)
		if !ok {
			return dataplane.ProviderInfo{}, false
		}

		return dataplane.ProviderInfo{
			Name: r.ID, SecretARN: strOf(objOf(r.Doc, "apiKeySecretArn"), "secretArn"),
			SecretJSONID: strOf(r.Doc, "apiKeySecretJsonKey"),
		}, true
	}

	r, ok := b.find(dsc(sc), b.kinds[kindOAuthProvider], "", name)
	if !ok {
		return dataplane.ProviderInfo{}, false
	}

	info := dataplane.ProviderInfo{
		Name:   r.ID,
		Vendor: strOf(r.Doc, "credentialProviderVendor"),
		SecretARN: strOf(
			objOf(r.Doc, "clientSecretArn"),
			"secretArn",
		),
		SecretJSONID: strOf(r.Doc, "clientSecretJsonKey"),
	}

	for _, cfg := range objOf(r.Doc, "oauth2ProviderConfigOutput") {
		info.Config = mapOf(cfg)
	}

	return info, true
}

func (d dataSeam) SecretValue(arn, jsonKey string) (string, bool) {
	b := d.b
	b.mu.RLock("DataPlane.SecretValue")
	refs := b.refs
	b.mu.RUnlock()

	st, _ := refs.(SecretStore)
	if st == nil || arn == "" {
		return "", false
	}

	raw, ok := st.GetSecret(arn)
	if !ok || jsonKey == "" {
		return raw, ok
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return raw, true
	}

	switch v := fields[jsonKey].(type) {
	case string:
		return v, true
	case nil:
		return "", false
	default:
		return string(mustMarshal(v)), true
	}
}

func mustMarshal(v any) []byte {
	out, _ := json.Marshal(v)

	return out
}
