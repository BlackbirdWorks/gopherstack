// Package k3sengine runs single-node k3s clusters in containers for the EKS and GKE emulators.
package k3sengine

import (
	"strconv"
	"strings"
)

const (
	// ImageRepo is the image repository the engine runs.
	ImageRepo = "rancher/k3s"
	// DefaultToken is the static cluster-admin bearer token used when none is configured.
	DefaultToken = "gopherstack-eks-cluster-token" //nolint:gosec // documented default for a local emulator
	// APIPort is the k3s API server port inside the container.
	APIPort = 6443

	majorMinorParts = 2
	firstMinor      = 29
	tokenFileName   = "/etc/gopherstack-eks-tokens.csv"
	envAdminAuth    = "GOPHERSTACK_EKS_ADMIN_AUTH"
	envHost         = "GOPHERSTACK_EKS_HOST"
	envCACert       = "GOPHERSTACK_EKS_CA_CERT"
	envCAKey        = "GOPHERSTACK_EKS_CA_KEY"
	envNodeName     = "GOPHERSTACK_NODE_NAME"
	tlsDir          = "/var/lib/rancher/k3s/server/tls"
	// DataDir is the k3s state directory a persistent volume is mounted on.
	DataDir = "/var/lib/rancher/k3s"
	// CACertFile is where the node keeps the serving CA certificate.
	CACertFile = tlsDir + "/server-ca.crt"

	// rotatedMarker is left by a CA rotation so the next start drops the cached serving certificate.
	rotatedMarker = tlsDir + "/.gopherstack-rotated"
)

const scriptHead = `printf '%s,admin,admin,"system:masters"\n' "$` + envAdminAuth + `" > ` + tokenFileName +
	` && umask 077 && mkdir -p ` + tlsDir +
	` && { [ ! -e ` + rotatedMarker + ` ] || rm -f ` + rotatedMarker + ` ` + tlsDir + `/dynamic-cert.json; }` +
	` && { [ -s ` + tlsDir + `/server-ca.crt ] && [ -s ` + tlsDir + `/server-ca.key ] || {` +
	` printf '%s\n' "$` + envCACert + `" > ` + tlsDir + `/server-ca.crt` +
	` && printf '%s\n' "$` + envCAKey + `" > ` + tlsDir + `/server-ca.key; }; }` +
	` && unset ` + envCAKey + ` && exec /bin/k3s server --tls-san "$` + envHost + `"` +
	` --disable traefik${` + envNodeName + `:+ --node-name "$` + envNodeName + `"}`

const scriptTail = ` --disable metrics-server --kube-apiserver-arg=token-auth-file=` + tokenFileName

// Script builds the node command; servicelb stays on only when load-balancer ports are published.
func Script(servicelb bool) string {
	if servicelb {
		return scriptHead + scriptTail
	}

	return scriptHead + ` --disable servicelb` + scriptTail
}

// Tags are the newest published rancher/k3s tags for Kubernetes 1.29 through 1.36, in minor order.
func Tags() [8]string {
	return [...]string{
		"v1.29.15-k3s1", "v1.30.14-k3s2", "v1.31.14-k3s1", "v1.32.13-k3s1",
		"v1.33.13-k3s1", "v1.34.12-k3s1", "v1.35.9-k3s1", "v1.36.5-k3s1",
	}
}

// Tag returns the pinned k3s tag for a Kubernetes version's minor, or false when the minor is unsupported.
func Tag(version string) (string, bool) {
	parts := strings.Split(version, ".")
	if len(parts) < majorMinorParts || parts[0] != "1" {
		return "", false
	}

	tags := Tags()

	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < firstMinor || minor >= firstMinor+len(tags) {
		return "", false
	}

	return tags[minor-firstMinor], true
}

// Image maps a Kubernetes version to its pinned k3s image; exact is false when it fell back to fallbackVersion.
func Image(version, fallbackVersion string) (string, bool) {
	if tag, ok := Tag(version); ok {
		return ImageRepo + ":" + tag, true
	}

	tag, _ := Tag(fallbackVersion)

	return ImageRepo + ":" + tag, false
}

// MinorKey returns "1.N" for a "1.N[.x]" version.
func MinorKey(version string) (string, bool) {
	parts := strings.Split(version, ".")
	if len(parts) < majorMinorParts || parts[0] != "1" {
		return "", false
	}

	if _, err := strconv.Atoi(parts[1]); err != nil {
		return "", false
	}

	return parts[0] + "." + parts[1], true
}
