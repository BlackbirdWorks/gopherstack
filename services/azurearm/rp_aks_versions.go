package azurearm

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

const (
	aksCodeVersionUnsupported = "K8sVersionNotSupported"
	aksCodeInvalidUpgrade     = "InvalidUpgradeParameter"
	aksMaxPoolSkew            = 3
	aksVersionParts           = 3
	aksMinorParts             = 2
)

type aksMinor struct {
	minor     string
	patches   []string
	isPreview bool
}

// aksMinors is a representative set of the minors AKS documents as supported (Learn: supported-kubernetes-versions,
// 2026-10); the patch numbers are illustrative, not the live service's.
//
//nolint:gochecknoglobals // static read-only catalog
var aksMinors = []aksMinor{
	{minor: "1.34", patches: []string{"1.34.4", "1.34.6"}},
	{minor: "1.35", patches: []string{"1.35.2", "1.35.4"}},
	{minor: "1.36", patches: []string{"1.36.1", "1.36.3"}},
	{minor: "1.37", patches: []string{"1.37.0", "1.37.1"}},
	{minor: "1.38", patches: []string{"1.38.0"}, isPreview: true},
}

const aksDefaultMinor = "1.36"

type aksVersion struct {
	minor, patch int
	hasPatch     bool
}

func parseAKSVersion(s string) (aksVersion, bool) {
	parts := strings.Split(s, ".")
	if len(parts) < aksMinorParts || len(parts) > aksVersionParts || parts[0] != "1" {
		return aksVersion{}, false
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < 0 {
		return aksVersion{}, false
	}

	v := aksVersion{minor: minor}

	if len(parts) == aksVersionParts {
		patch, perr := strconv.Atoi(parts[2])
		if perr != nil || patch < 0 {
			return aksVersion{}, false
		}

		v.patch, v.hasPatch = patch, true
	}

	return v, true
}

func (v aksVersion) minorKey() string { return fmt.Sprintf("1.%d", v.minor) }

func findAKSMinor(key string) (aksMinor, bool) {
	i := slices.IndexFunc(aksMinors, func(m aksMinor) bool { return m.minor == key })
	if i < 0 {
		return aksMinor{}, false
	}

	return aksMinors[i], true
}

// resolveAKSVersion maps a requested "1.N" or "1.N.P" to a full catalog patch version. An empty request resolves to
// the default minor's newest patch.
func resolveAKSVersion(req string) (string, error) {
	if req == "" {
		req = aksDefaultMinor
	}

	v, ok := parseAKSVersion(req)
	if !ok {
		return "", aksVersionErr(req)
	}

	m, ok := findAKSMinor(v.minorKey())
	if !ok {
		return "", aksVersionErr(req)
	}

	if !v.hasPatch {
		return m.patches[len(m.patches)-1], nil
	}

	if !slices.Contains(m.patches, req) {
		return "", aksVersionErr(req)
	}

	return req, nil
}

func aksVersionErr(req string) error {
	return netErr(http.StatusBadRequest, aksCodeVersionUnsupported,
		"Version %s is not supported in this region. Please use `az aks get-versions` command to get the supported "+
			"version list in this region.", req)
}

// aksUpgradeAllowed enforces the control plane rules: patch moves stay in the minor and never go back, minor moves are
// exactly one ahead, and no downgrade (Learn: upgrade-aks-control-plane).
func aksUpgradeAllowed(from, to string) error {
	if from == to {
		return nil
	}

	f, _ := parseAKSVersion(from)
	t, _ := parseAKSVersion(to)

	switch {
	case t.minor < f.minor, t.minor == f.minor && t.patch < f.patch:
		return netErr(http.StatusBadRequest, aksCodeInvalidUpgrade,
			"Downgrading Kubernetes version from %s to %s is not allowed.", from, to)
	case t.minor > f.minor+1:
		return netErr(http.StatusBadRequest, aksCodeInvalidUpgrade,
			"Upgrading Kubernetes version from %s to %s is not allowed: minor versions cannot be skipped, upgrade "+
				"one minor version at a time.", from, to)
	}

	return nil
}

// aksUpgradesFrom lists the versions a cluster at current can move to.
func aksUpgradesFrom(current string) []map[string]any {
	cur, _ := parseAKSVersion(current)
	out := []map[string]any{}

	for _, m := range aksMinors {
		mv, _ := parseAKSVersion(m.minor)

		if mv.minor != cur.minor && mv.minor != cur.minor+1 {
			continue
		}

		for _, p := range m.patches {
			pv, _ := parseAKSVersion(p)
			if mv.minor == cur.minor && pv.patch <= cur.patch {
				continue
			}

			out = append(out, map[string]any{aksFieldK8sVersion: p, "isPreview": m.isPreview})
		}
	}

	return out
}

// aksPoolSkewOK checks a node pool version against the control plane: never newer, at most three minors behind.
func aksPoolSkewOK(pool, controlPlane string) error {
	p, _ := parseAKSVersion(pool)
	c, _ := parseAKSVersion(controlPlane)

	if p.minor > c.minor || (p.minor == c.minor && p.patch > c.patch) {
		return netErr(http.StatusBadRequest, aksCodeInvalidUpgrade,
			"Node pool version %s cannot be newer than the control plane version %s.", pool, controlPlane)
	}

	if c.minor-p.minor > aksMaxPoolSkew {
		return netErr(http.StatusBadRequest, aksCodeInvalidUpgrade,
			"Node pool version %s is more than %d minor versions behind the control plane version %s.",
			pool, aksMaxPoolSkew, controlPlane)
	}

	return nil
}

func aksKubernetesVersionsBody() map[string]any {
	values := make([]any, 0, len(aksMinors))

	for _, m := range aksMinors {
		patches := map[string]any{}
		for _, p := range m.patches {
			patches[p] = map[string]any{"upgrades": upgradeNames(aksUpgradesFrom(p))}
		}

		entry := map[string]any{
			"version": m.minor, "patchVersions": patches,
			"capabilities": map[string]any{"supportPlan": []any{"KubernetesOfficial"}},
		}

		if m.minor == aksDefaultMinor {
			entry["isDefault"] = true
		}

		if m.isPreview {
			entry["isPreview"] = true
		}

		values = append(values, entry)
	}

	return map[string]any{"values": values}
}

func upgradeNames(ups []map[string]any) []any {
	out := make([]any, 0, len(ups))
	for _, u := range ups {
		out = append(out, u[aksFieldK8sVersion])
	}

	return out
}

const aksCodeInvalidParameter = "InvalidParameter"

func aksBad(format string, args ...any) error {
	return netErr(http.StatusBadRequest, aksCodeInvalidParameter, format, args...)
}
