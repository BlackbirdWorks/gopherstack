package azurearm

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

const (
	webFieldFlexConfig = "functionAppConfig"

	flexMaxInstancesDefault = 100
	flexMaxInstancesMax     = 1000
	flexMemoryDefault       = 2048
)

var (
	errFlexPlan      = errors.New("functionAppConfig requires a Flex Consumption (FC1) plan")
	errFlexNoConfig  = errors.New("a function app on a Flex Consumption plan requires properties.functionAppConfig")
	errFlexKind      = errors.New("functionAppConfig is only valid for Linux function apps")
	errFlexStorage   = errors.New("functionAppConfig.deployment.storage")
	errFlexAuth      = errors.New("functionAppConfig.deployment.storage.authentication")
	errFlexRuntime   = errors.New("functionAppConfig.runtime")
	errFlexScale     = errors.New("functionAppConfig.scaleAndConcurrency")
	errFlexUpdateStr = errors.New("functionAppConfig.siteUpdateStrategy")
)

// flexRuntimeVersions are the stack versions Learn lists for Flex Consumption (Supported language stack versions).
func flexRuntimeVersions() map[string][]string {
	return map[string][]string{
		"dotnet-isolated": {"8.0", "9.0", "10.0"},
		"java":            {"8", "11", "17", "21", "25"},
		"node":            {"22", "24"},
		"powershell":      {"7.4", "7.6"},
		"python":          {"3.10", "3.11", "3.12", "3.13", "3.14"},
		"custom":          nil,
	}
}

var flexMemorySizes = []int{512, 2048, 4096}

func flexErr(base error, format string, args ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrWebInvalidSiteConfig, base, fmt.Sprintf(format, args...))
}

func fnAppInt(m map[string]any, key string) (int, bool) {
	f, ok := m[key].(float64)
	if !ok {
		if i, isInt := m[key].(int); isInt {
			return i, true
		}

		return 0, false
	}

	return int(f), f == float64(int(f))
}

func normalizeFlexVersion(name, v string) string {
	v = strings.TrimSpace(v)
	if name == "dotnet-isolated" && v != "" && !strings.Contains(v, ".") {
		return v + ".0"
	}

	return v
}

func validateFlexRuntime(cfg map[string]any) (map[string]any, error) {
	rt := mapOf(cfg["runtime"])
	name, _ := rt[fieldName].(string)
	versions, known := flexRuntimeVersions()[name]

	if !known {
		return nil, flexErr(errFlexRuntime, "name must be one of dotnet-isolated, node, java, powershell, python, custom")
	}

	version, _ := rt["version"].(string)
	version = normalizeFlexVersion(name, version)

	if name != "custom" && !slices.Contains(versions, version) {
		return nil, flexErr(errFlexRuntime, "%s version %q is not supported; supported: %s",
			name, version, strings.Join(versions, ", "))
	}

	return map[string]any{fieldName: name, "version": version}, nil
}

func validateFlexStorage(cfg map[string]any) (map[string]any, error) {
	storage := mapOf(mapOf(cfg["deployment"])["storage"])

	if t, _ := storage[fieldType].(string); t != "blobContainer" {
		return nil, flexErr(errFlexStorage, "type must be blobContainer")
	}

	raw, _ := storage[fieldValue].(string)
	if u, err := url.Parse(raw); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		strings.Trim(u.Path, "/") == "" {
		return nil, flexErr(errFlexStorage, "value must be the blob container URL")
	}

	auth := mapOf(storage["authentication"])
	authType, _ := auth[fieldType].(string)
	connName, _ := auth["storageAccountConnectionStringName"].(string)
	identity, _ := auth["userAssignedIdentityResourceId"].(string)

	switch authType {
	case "StorageAccountConnectionString":
		if connName == "" || identity != "" {
			return nil, flexErr(errFlexAuth, "StorageAccountConnectionString needs storageAccountConnectionStringName only")
		}
	case "UserAssignedIdentity":
		if identity == "" || connName != "" {
			return nil, flexErr(errFlexAuth, "UserAssignedIdentity needs userAssignedIdentityResourceId only")
		}
	case "SystemAssignedIdentity":
		if identity != "" || connName != "" {
			return nil, flexErr(errFlexAuth, "SystemAssignedIdentity takes no other authentication property")
		}
	default:
		return nil, flexErr(errFlexAuth,
			"type must be StorageAccountConnectionString, SystemAssignedIdentity or UserAssignedIdentity")
	}

	return map[string]any{"storage": map[string]any{fieldType: "blobContainer", fieldValue: raw, "authentication": auth}}, nil
}

func validateFlexScale(cfg map[string]any) (map[string]any, error) {
	sc := mapOf(cfg["scaleAndConcurrency"])
	memory, maxCount := flexMemoryDefault, flexMaxInstancesDefault

	if _, set := sc["instanceMemoryMB"]; set {
		v, ok := fnAppInt(sc, "instanceMemoryMB")
		if !ok || !slices.Contains(flexMemorySizes, v) {
			return nil, flexErr(errFlexScale, "instanceMemoryMB must be 512, 2048 or 4096")
		}

		memory = v
	}

	if _, set := sc["maximumInstanceCount"]; set {
		v, ok := fnAppInt(sc, "maximumInstanceCount")
		if !ok || v < 1 || v > flexMaxInstancesMax {
			return nil, flexErr(errFlexScale, "maximumInstanceCount must be between 1 and %d", flexMaxInstancesMax)
		}

		maxCount = v
	}

	ready := []any{}

	for _, raw := range flexSlice(sc["alwaysReady"]) {
		e := mapOf(raw)
		n, _ := e[fieldName].(string)
		c, ok := fnAppInt(e, "instanceCount")

		if n == "" || !ok || c < 0 {
			return nil, flexErr(errFlexScale, "alwaysReady entries need a name and a non-negative instanceCount")
		}

		ready = append(ready, map[string]any{fieldName: n, "instanceCount": c})
	}

	out := map[string]any{"instanceMemoryMB": memory, "maximumInstanceCount": maxCount, "alwaysReady": ready}

	if http := mapOf(mapOf(sc["triggers"])["http"]); len(http) > 0 {
		c, ok := fnAppInt(http, "perInstanceConcurrency")
		if !ok || c < 1 {
			return nil, flexErr(errFlexScale, "triggers.http.perInstanceConcurrency must be at least 1")
		}

		out["triggers"] = map[string]any{"http": map[string]any{"perInstanceConcurrency": c}}
	}

	return out, nil
}

// validateFlexConfig checks properties.functionAppConfig and returns it with Flex defaults filled in.
func validateFlexConfig(raw map[string]any) (map[string]any, error) {
	storage, err := validateFlexStorage(raw)
	if err != nil {
		return nil, err
	}

	rt, err := validateFlexRuntime(raw)
	if err != nil {
		return nil, err
	}

	scale, err := validateFlexScale(raw)
	if err != nil {
		return nil, err
	}

	out := map[string]any{"deployment": storage, "runtime": rt, "scaleAndConcurrency": scale}

	if s, set := mapOf(raw["siteUpdateStrategy"])[fieldType]; set {
		if st, _ := s.(string); st != "Recreate" && st != "RollingUpdate" {
			return nil, flexErr(errFlexUpdateStr, "type must be Recreate or RollingUpdate")
		}

		out["siteUpdateStrategy"] = map[string]any{fieldType: s}
	}

	return out, nil
}

// flexLinuxFx is the data plane's stack selector for a Flex runtime.
func flexLinuxFx(cfg map[string]any) string {
	rt := mapOf(cfg["runtime"])
	name, _ := rt[fieldName].(string)
	version, _ := rt["version"].(string)

	if name == "custom" || version == "" {
		return name
	}

	return strings.ToUpper(name) + "|" + version
}

func (p *WebProvider) planIsFlex(serverFarmID string) bool {
	pid, err := ParseGenericResourcePath(serverFarmID)
	if err != nil {
		return false
	}

	pl, ok := p.plans[planKey(pid.ResourceGroup, pid.LeafName())]
	if !ok {
		return false
	}

	tier, _ := pl.SKU[cosmosFieldTier].(string)
	name, _ := pl.SKU[fieldName].(string)

	return strings.EqualFold(tier, "FlexConsumption") || strings.EqualFold(name, "FC1")
}

// checkFlexSite validates properties.functionAppConfig against the site's plan and returns the normalized value
// (nil when the site is not a Flex app).
func (p *WebProvider) checkFlexSite(kind, farm string, props map[string]any) (map[string]any, error) {
	raw, present := props[webFieldFlexConfig].(map[string]any)
	flexPlan := farm != "" && p.planIsFlex(farm)

	switch {
	case !present && flexPlan:
		return nil, badWebRequest(fmt.Errorf("%w: %w", ErrWebInvalidSiteConfig, errFlexNoConfig))
	case !present:
		return nil, nil
	case !isFunctionAppKind(kind) || !isLinuxKind(kind):
		return nil, badWebRequest(fmt.Errorf("%w: %w", ErrWebInvalidSiteConfig, errFlexKind))
	case !flexPlan:
		return nil, badWebRequest(fmt.Errorf("%w: %w", ErrWebInvalidSiteConfig, errFlexPlan))
	}

	cfg, err := validateFlexConfig(raw)
	if err != nil {
		return nil, badWebRequest(err)
	}

	return cfg, nil
}


func flexSlice(v any) []any {
	s, _ := v.([]any)

	return s
}
