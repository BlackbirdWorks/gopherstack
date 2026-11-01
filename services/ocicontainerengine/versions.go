package ocicontainerengine

import (
	"cmp"
	"regexp"
	"slices"
	"strconv"
)

const maxMinorSkew = 3

var versionRe = regexp.MustCompile(`^v1\.(\d+)\.(\d+)$`)

// supportedVersions are the Kubernetes versions OKE lists as supported for new clusters, oldest first.
//
//nolint:gochecknoglobals // immutable version catalogue
var supportedVersions = []string{"v1.34.1", "v1.34.2", "v1.34.10", "v1.35.2", "v1.36.1", "v1.36.4"}

type kver struct{ minor, patch int }

func parseVersion(s string) (kver, bool) {
	m := versionRe.FindStringSubmatch(s)
	if m == nil {
		return kver{}, false
	}

	minor, _ := strconv.Atoi(m[1])
	patch, _ := strconv.Atoi(m[2])

	return kver{minor, patch}, true
}

func (v kver) cmp(o kver) int {
	return cmp.Or(cmp.Compare(v.minor, o.minor), cmp.Compare(v.patch, o.patch))
}

func isSupported(v string) bool { return slices.Contains(supportedVersions, v) }

// listVersions returns every supported version, or only the newest patch of each minor.
func listVersions(all bool) []string {
	if all {
		return slices.Clone(supportedVersions)
	}

	var out []string

	for i, v := range supportedVersions {
		pv, _ := parseVersion(v)

		if i+1 < len(supportedVersions) {
			if nv, _ := parseVersion(supportedVersions[i+1]); nv.minor == pv.minor {
				continue
			}
		}

		out = append(out, v)
	}

	return out
}

// upgradesFrom lists the supported versions newer than v that are at most one minor ahead.
func upgradesFrom(v string) []string {
	cur, ok := parseVersion(v)
	if !ok {
		return []string{}
	}

	out := []string{}

	for _, s := range supportedVersions {
		if sv, _ := parseVersion(s); sv.cmp(cur) > 0 && sv.minor <= cur.minor+1 {
			out = append(out, s)
		}
	}

	return out
}

// poolVersionOK reports whether a node pool may run pool under a control plane at cluster.
func poolVersionOK(pool, cluster string) *APIError {
	pv, ok := parseVersion(pool)
	if !ok {
		return errParam(
			"kubernetesVersion " + pool + " is not a valid Kubernetes version (expected vMAJOR.MINOR.PATCH)",
		)
	}

	cv, _ := parseVersion(cluster)

	switch {
	case pv.minor > cv.minor || (pv.minor == cv.minor && pv.patch > cv.patch):
		return errParam(
			"The node pool Kubernetes version " + pool + " cannot be newer than the cluster version " + cluster,
		)
	case cv.minor-pv.minor > maxMinorSkew:
		return errParam(
			"The node pool Kubernetes version " + pool + " is more than three minor versions behind the cluster",
		)
	}

	return nil
}

func engineImageVersion(v string) string {
	if len(v) > 0 && v[0] == 'v' {
		return v[1:]
	}

	return v
}
