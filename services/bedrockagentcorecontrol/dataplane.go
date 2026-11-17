package bedrockagentcorecontrol

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

var (
	errNoCodeStore  = errors.New("S3 is not available")
	errNoCodeObject = errors.New("code package not found")
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
			info := runtimeInfo(rt.ID, v.Doc)
			info.Code = b.codeInfo(v.Doc)
			info.Authorize = b.runtimeAuthorizer(v.Doc)
			info.ProtectedResource = runtimeProtectedResource(v.Doc)

			return info, nil
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
	if err := json.Unmarshal([]byte(raw), &fields, jsonx.V1()); err != nil {
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
	out, _ := json.Marshal(v, jsonx.V1())

	return out
}

// runtimeAuthorizer enforces the runtime's customJWTAuthorizer on inbound calls; nil when none is configured.
func (b *InMemoryBackend) runtimeAuthorizer(
	doc map[string]any,
) func(context.Context, *http.Request) *dataplane.AuthFailure {
	cfg := deepCopy(objOf(objOf(doc, "authorizerConfiguration"), "customJWTAuthorizer"))
	if cfg == nil {
		return nil
	}

	return func(ctx context.Context, r *http.Request) *dataplane.AuthFailure {
		f := b.authorizeJWT(ctx, r, cfg)
		if f == nil {
			return nil
		}

		return &dataplane.AuthFailure{Status: f.status, Challenge: f.challenge, Message: f.message}
	}
}

func runtimeProtectedResource(doc map[string]any) func(*http.Request) map[string]any {
	cfg := deepCopy(objOf(objOf(doc, "authorizerConfiguration"), "customJWTAuthorizer"))
	if cfg == nil {
		return nil
	}

	return func(r *http.Request) map[string]any { return protectedResourceMetadata(r, cfg) }
}

// codeInfo describes a direct code deployment; nil for container runtimes.
func (b *InMemoryBackend) codeInfo(doc map[string]any) *dataplane.CodeInfo {
	cc := objOf(objOf(doc, "agentRuntimeArtifact"), "codeConfiguration")
	if cc == nil {
		return nil
	}

	s3 := objOf(objOf(cc, "code"), "s3")
	bucket, key := strOf(s3, "bucket"), strOf(s3, "prefix")
	refs := b.refs

	return &dataplane.CodeInfo{
		Runtime: strOf(cc, "runtime"), EntryPoint: stringsOf(cc["entryPoint"]),
		Load: func() ([]byte, error) {
			if refs == nil {
				return nil, errNoCodeStore
			}

			data, ok := refs.ReadObject(bucket, key)
			if !ok {
				return nil, fmt.Errorf("%w: s3://%s/%s", errNoCodeObject, bucket, key)
			}

			return data, nil
		},
	}
}

// VerifyWorkloadJWT verifies a user token for a workload identity. A runtime or gateway-managed identity accepts
// only tokens its inbound customJWTAuthorizer accepts (issuer, audience, clients, scopes, custom claims).
func (d dataSeam) VerifyWorkloadJWT(ctx context.Context, sc dataplane.Scope, workload, token string) (string, error) {
	b := d.b

	cfg := b.managedAuthorizer(dsc(sc), workload)
	if cfg == nil {
		return d.VerifyUserJWT(ctx, token)
	}

	claims, f := b.verifyBearerToken(ctx, strings.TrimSpace(token), cfg)
	if f != nil {
		return "", fmt.Errorf("%w: %s", dataplane.ErrInvalidFederation, f.message)
	}

	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", fmt.Errorf("%w: no sub claim", dataplane.ErrInvalidFederation)
	}

	return sub, nil
}

func (b *InMemoryBackend) managedAuthorizer(sc scope, workload string) map[string]any {
	b.mu.RLock("managedAuthorizer")
	defer b.mu.RUnlock()

	for _, r := range b.resources.All() {
		if r.Account != sc.account || r.Region != sc.region || strOf(r.Aux, "managedIdentity") != workload {
			continue
		}

		if cfg := objOf(objOf(r.Doc, "authorizerConfiguration"), "customJWTAuthorizer"); cfg != nil {
			return deepCopy(cfg)
		}
	}

	return nil
}
