package azurearm

import (
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	acaCodeInvalid     = "InvalidRequestParameterWithDetails"
	acaMinNameLen      = 2
	acaMaxAppNameLen   = 32
	acaMaxEnvNameLen   = 60
	acaMaxSuffixLen    = 64
	acaMaxLabelLen     = 64
	acaMaxReplicas     = 1000
	acaDefaultMax      = 10
	acaDefaultCPU      = 0.5
	acaDefaultMemGi    = 1.0
	acaCPUStep         = 0.25
	acaConsumptionCPU  = 4.0
	acaConsumptionOnly = 2.0
	acaMemPerCPU       = 2.0
	acaMiPerGi         = 1024.0
	acaMaxPort         = 65535
	acaFloatEps        = 1e-9
	acaModeSingle      = "Single"
	acaModeMultiple    = "Multiple"
	acaModeLabels      = "Labels"
	acaConsumption     = "Consumption"
	acaIdentitySystem  = "system"
	fieldTemplate      = "template"
	fieldContainers    = "containers"
	fieldConfiguration = "configuration"
	fieldSecrets       = "secrets"
	fieldIngress       = "ingress"
	fieldRegistries    = "registries"
	fieldVolumes       = "volumes"
	fieldScale         = "scale"
	fieldTraffic       = "traffic"
	fieldRevisionName  = "revisionName"
	fieldLatestRev     = "latestRevision"
	fieldWeight        = "weight"
	fieldLabel         = "label"
	fieldResources     = "resources"
	fieldRevSuffix     = "revisionSuffix"
	fieldIdentitySet   = "identity"
	fieldKeyVaultURL   = "keyVaultUrl"
	fieldStorageName   = "storageName"
	fieldMinReplicas   = "minReplicas"
	fieldMaxReplicas   = "maxReplicas"
	fieldInitContainer = "initContainers"
	fieldEnvironmentID = "environmentId"
	fieldManagedEnvID  = "managedEnvironmentId"
	fieldWorkloadProf  = "workloadProfileName"
)

const (
	acaFieldOutboundIPs = "outboundIpAddresses"
	acaFieldSystemData  = "systemData"
	acaFieldKind        = "kind"
	acaFieldImage       = "image"
	acaFieldStatus      = "status"
	acaFieldRunning     = "runningState"
	acaFieldPEC         = "privateEndpointConnections"
	acaScaleHTTP        = "http"
	acaScaleTCP         = "tcp"
	acaSchemeHTTPS      = "https"
	acaRuleAllow        = "allow"
	acaRuleDeny         = "deny"
	acaActActivate      = "activate"
	acaActDeactivate    = "deactivate"
	acaActRestart       = "restart"
	acaHealthNone       = "None"
)

var (
	acaNameRe   = regexp.MustCompile(`^[a-z]([a-z0-9-]*[a-z0-9])?$`)
	acaSecretRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
	acaVaultRe  = regexp.MustCompile(`^/secrets/([0-9A-Za-z-]+)(?:/([0-9a-fA-F]+))?/?$`)
)

func acaBad(format string, args ...any) error {
	return netErr(http.StatusBadRequest, acaCodeInvalid, format, args...)
}

func acaList(v any) []any {
	l, _ := v.([]any)

	return l
}

func acaNum(m map[string]any, k string) (float64, bool) {
	f, ok := m[k].(float64)

	return f, ok
}

func acaBool(m map[string]any, k string) bool {
	b, _ := m[k].(bool)

	return b
}

func acaClone(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	buf, err := json.Marshal(m)
	if err != nil {
		return nil
	}

	var out map[string]any
	if err = json.Unmarshal(buf, &out); err != nil {
		return nil
	}

	return out
}

func acaNameValid(name string, minLen, maxLen int) bool {
	return len(name) >= minLen && len(name) <= maxLen && acaNameRe.MatchString(name) && !strings.Contains(name, "--")
}

func acaWeightOf(m map[string]any) int {
	f, _ := acaNum(m, fieldWeight)

	return int(f)
}

func parseMemoryGi(s string) (float64, bool) {
	for _, u := range []struct {
		suffix string
		div    float64
	}{{"Gi", 1}, {"Mi", acaMiPerGi}, {"G", 1}, {"M", acaMiPerGi}} {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			f, err := strconv.ParseFloat(num, 64)

			return f / u.div, err == nil && f > 0
		}
	}

	return 0, false
}

type acaSizing struct{ cpu, memGi float64 }

func acaContainerSizing(c map[string]any) (acaSizing, error) {
	res := mapOf(c[fieldResources])
	if res == nil {
		return acaSizing{acaDefaultCPU, acaDefaultMemGi}, nil
	}

	cpu, hasCPU := acaNum(res, "cpu")
	memStr := strOf(res, "memory")

	if hasCPU != (memStr != "") {
		return acaSizing{}, acaBad("container '%s': resources.cpu and resources.memory must be set together.",
			strOf(c, fieldName))
	}

	if !hasCPU {
		return acaSizing{acaDefaultCPU, acaDefaultMemGi}, nil
	}

	mem, ok := parseMemoryGi(memStr)
	if !ok || cpu <= 0 {
		return acaSizing{}, acaBad(
			"container '%s': resources.memory '%s' must be like '1Gi' and resources.cpu positive.",
			strOf(c, fieldName),
			memStr,
		)
	}

	return acaSizing{cpu, mem}, nil
}

// acaCheckSizing enforces the documented Consumption vCPU/memory combinations over all main containers.
func acaCheckSizing(containers []any, dedicated, consumptionOnly bool) error {
	var total acaSizing

	for _, e := range containers {
		s, err := acaContainerSizing(mapOf(e))
		if err != nil {
			return err
		}

		total.cpu += s.cpu
		total.memGi += s.memGi
	}

	if dedicated {
		return nil
	}

	maxCPU := acaConsumptionCPU
	if consumptionOnly {
		maxCPU = acaConsumptionOnly
	}

	steps := total.cpu / acaCPUStep
	if total.cpu > maxCPU+acaFloatEps || math.Abs(steps-math.Round(steps)) > acaFloatEps ||
		math.Abs(total.memGi-total.cpu*acaMemPerCPU) > acaFloatEps {
		return acaBad("The total CPU (%g) and memory (%gGi) allocated to all containers must add up to one of the "+
			"supported vCPU/memory combinations (0.25/0.5Gi, 0.5/1.0Gi ... %g/%gGi).",
			total.cpu, total.memGi, maxCPU, maxCPU*acaMemPerCPU)
	}

	return nil
}

func acaCheckContainers(t map[string]any, secrets map[string]bool, mounts map[string]bool) error {
	names := map[string]bool{}

	for _, key := range []string{fieldContainers, fieldInitContainer} {
		for _, e := range acaList(t[key]) {
			c := mapOf(e)
			if err := acaCheckContainer(c, secrets, mounts, names); err != nil {
				return err
			}
		}
	}

	return nil
}

func acaCheckContainer(c map[string]any, secrets, volumes, names map[string]bool) error {
	name := strOf(c, fieldName)
	if name == "" || strOf(c, acaFieldImage) == "" {
		return acaBad("Each container requires a name and an image.")
	}

	if names[name] {
		return acaBad("Container name '%s' is used more than once.", name)
	}

	names[name] = true

	for _, ev := range acaList(c["env"]) {
		e := mapOf(ev)
		ref := strOf(e, "secretRef")

		switch {
		case strOf(e, fieldName) == "":
			return acaBad("Container '%s' has an environment variable without a name.", name)
		case ref != "" && strOf(e, fieldValue) != "":
			return acaBad("Environment variable '%s' sets both value and secretRef.", strOf(e, fieldName))
		case ref != "" && !secrets[ref]:
			return acaBad(
				"Secret '%s' referenced by environment variable '%s' was not found.",
				ref,
				strOf(e, fieldName),
			)
		}
	}

	for _, vm := range acaList(c["volumeMounts"]) {
		m := mapOf(vm)
		if !volumes[strOf(m, "volumeName")] || strOf(m, "mountPath") == "" {
			return acaBad("Container '%s' mounts volume '%s' which is not defined.", name, strOf(m, "volumeName"))
		}
	}

	return acaCheckProbes(name, acaList(c["probes"]))
}

func acaCheckProbes(container string, probes []any) error {
	seen := map[string]bool{}

	for _, e := range probes {
		typ := strings.ToLower(strOf(mapOf(e), fieldType))

		switch typ {
		case "liveness", "readiness", "startup":
		default:
			return acaBad("Container '%s' has a probe of unknown type '%s'.", container, typ)
		}

		if seen[typ] {
			return acaBad("Container '%s' defines the %s probe more than once.", container, typ)
		}

		seen[typ] = true
	}

	return nil
}

func acaCheckScale(s map[string]any) error {
	minR, hasMin := acaNum(s, fieldMinReplicas)
	maxR, hasMax := acaNum(s, fieldMaxReplicas)

	if !hasMax {
		maxR = acaDefaultMax
	}

	switch {
	case hasMin && (minR < 0 || minR > acaMaxReplicas):
		return acaBad("scale.minReplicas must be between 0 and %d.", acaMaxReplicas)
	case maxR < 1 || maxR > acaMaxReplicas:
		return acaBad("scale.maxReplicas must be between 1 and %d.", acaMaxReplicas)
	case hasMin && minR > maxR:
		return acaBad("scale.minReplicas (%g) cannot exceed scale.maxReplicas (%g).", minR, maxR)
	}

	return acaCheckScaleRules(acaList(s["rules"]))
}

func acaCheckScaleRules(rules []any) error {
	names := map[string]bool{}

	for _, e := range rules {
		r := mapOf(e)
		name := strOf(r, fieldName)

		kinds := 0

		for _, k := range []string{acaScaleHTTP, acaScaleTCP, "azureQueue", "custom"} {
			if r[k] != nil {
				kinds++
			}
		}

		switch {
		case name == "" || names[name]:
			return acaBad("Scale rule names must be non-empty and unique.")
		case kinds != 1:
			return acaBad("Scale rule '%s' must define exactly one of http, tcp, azureQueue or custom.", name)
		case r["custom"] != nil && strOf(mapOf(r["custom"]), fieldType) == "":
			return acaBad("Custom scale rule '%s' requires a type.", name)
		}

		names[name] = true
	}

	return nil
}

func acaCheckVolumes(vols []any, storages map[string]bool) (map[string]bool, error) {
	names := map[string]bool{}

	for _, e := range vols {
		v := mapOf(e)
		name := strOf(v, fieldName)

		if name == "" || names[name] {
			return nil, acaBad("Volume names must be non-empty and unique.")
		}

		names[name] = true

		switch strings.ToLower(strOf(v, "storageType")) {
		case "", "emptydir", "secret", "nfsazurefile":
		case "azurefile":
			if !storages[strings.ToLower(strOf(v, fieldStorageName))] {
				return nil, acaBad("Volume '%s' references storage '%s' which does not exist in the environment.",
					name, strOf(v, fieldStorageName))
			}
		default:
			return nil, acaBad("Volume '%s' has unsupported storageType '%s'.", name, strOf(v, "storageType"))
		}
	}

	return names, nil
}

type acaVaultRef struct{ vault, secret, version string }

func parseVaultURL(raw string) (acaVaultRef, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != acaSchemeHTTPS || u.Hostname() == "" {
		return acaVaultRef{}, false
	}

	m := acaVaultRe.FindStringSubmatch(u.Path)
	host, _, _ := strings.Cut(u.Hostname(), ".")

	if m == nil || host == "" {
		return acaVaultRef{}, false
	}

	return acaVaultRef{vault: host, secret: m[1], version: m[2]}, true
}

// acaCheckSecrets validates the secrets array and returns the set of names.
func acaCheckSecrets(list []any, ident resourceIdentity) (map[string]bool, error) {
	names := map[string]bool{}

	for _, e := range list {
		s := mapOf(e)
		name := strOf(s, fieldName)

		if len(name) > 253 || !acaSecretRe.MatchString(name) || names[name] {
			return nil, acaBad("Secret name '%s' is invalid or duplicated.", name)
		}

		names[name] = true

		val, kv := strOf(s, fieldValue), strOf(s, fieldKeyVaultURL)

		switch {
		case (val == "") == (kv == ""):
			return nil, acaBad("Secret '%s' must define exactly one of value or keyVaultUrl.", name)
		case kv != "":
			if err := acaCheckVaultSecret(name, kv, strOf(s, fieldIdentitySet), ident); err != nil {
				return nil, err
			}
		}
	}

	return names, nil
}

func acaCheckVaultSecret(name, rawURL, identity string, ident resourceIdentity) error {
	if _, ok := parseVaultURL(rawURL); !ok {
		return acaBad(
			"Secret '%s': keyVaultUrl '%s' must look like https://<vault>.<domain>/secrets/<name>[/<version>].",
			name,
			rawURL,
		)
	}

	if !acaIdentityAssigned(identity, ident) {
		return acaBad("Secret '%s': identity '%s' is not assigned to the container app.", name, identity)
	}

	return nil
}

func acaIdentityAssigned(id string, ident resourceIdentity) bool {
	switch {
	case id == "":
		return false
	case strings.EqualFold(id, acaIdentitySystem):
		return ident.hasSystem()
	}

	for _, ua := range ident.UserAssigned {
		if strings.EqualFold(ua, id) {
			return true
		}
	}

	return false
}

func acaCheckRegistries(list []any, secrets map[string]bool, ident resourceIdentity) error {
	servers := map[string]bool{}

	for _, e := range list {
		r := mapOf(e)
		server := strings.ToLower(strOf(r, "server"))

		if server == "" || servers[server] {
			return acaBad("Registry servers must be non-empty and unique.")
		}

		servers[server] = true

		ref, identity := strOf(r, "passwordSecretRef"), strOf(r, fieldIdentitySet)

		switch {
		case identity != "" && (ref != "" || strOf(r, "username") != ""):
			return acaBad("Registry '%s' cannot combine identity with username/password.", server)
		case identity != "" && !acaIdentityAssigned(identity, ident):
			return acaBad("Registry '%s': identity '%s' is not assigned to the container app.", server, identity)
		case identity == "" && ref != "" && !secrets[ref]:
			return acaBad("Registry '%s': secret '%s' was not found.", server, ref)
		}
	}

	return nil
}

func acaCheckDapr(d map[string]any) error {
	if d != nil && acaBool(d, "enabled") && strOf(d, "appId") == "" {
		return acaBad("dapr.appId is required when Dapr is enabled.")
	}

	return nil
}

func acaCheckIngress(in map[string]any) error {
	if in == nil {
		return nil
	}

	transport := strings.ToLower(strOf(in, "transport"))

	switch transport {
	case "", "auto", acaScaleHTTP, "http2", acaScaleTCP:
	default:
		return acaBad("ingress.transport '%s' must be auto, http, http2 or tcp.", transport)
	}

	port, hasPort := acaNum(in, "targetPort")
	if !hasPort || port < 1 || port > acaMaxPort {
		return acaBad("ingress.targetPort is required and must be between 1 and %d.", acaMaxPort)
	}

	if ex, has := acaNum(in, "exposedPort"); has && (transport != acaScaleTCP || ex < 1 || ex > acaMaxPort) {
		return acaBad("ingress.exposedPort is only valid for tcp transport and must be a port number.")
	}

	return acaCheckIPRules(acaList(in["ipSecurityRestrictions"]))
}

func acaCheckIPRules(rules []any) error {
	action := ""

	for _, e := range rules {
		r := mapOf(e)
		a := strings.ToLower(strOf(r, "action"))

		if (a != acaRuleAllow && a != acaRuleDeny) || strOf(r, "ipAddressRange") == "" || strOf(r, fieldName) == "" {
			return acaBad(
				"ipSecurityRestrictions entries need a name, an ipAddressRange and an action of Allow or Deny.",
			)
		}

		if action != "" && action != a {
			return acaBad("ipSecurityRestrictions cannot mix Allow and Deny rules.")
		}

		action = a
	}

	return nil
}

func acaCheckLabel(label string) error {
	if len(label) > acaMaxLabelLen || !acaNameRe.MatchString(label) || strings.Contains(label, "--") {
		return acaBad("Label '%s' must be lowercase alphanumerics and dashes, start with a letter, end alphanumeric, "+
			"contain no '--' and be at most %d characters.", label, acaMaxLabelLen)
	}

	return nil
}

func acaModeOf(cfg map[string]any) (string, error) {
	switch strings.ToLower(strOf(cfg, "activeRevisionsMode")) {
	case "", "single":
		return acaModeSingle, nil
	case "multiple":
		return acaModeMultiple, nil
	case "labels":
		return acaModeLabels, nil
	}

	return "", acaBad("configuration.activeRevisionsMode '%s' must be Single, Multiple or Labels.",
		strOf(cfg, "activeRevisionsMode"))
}
