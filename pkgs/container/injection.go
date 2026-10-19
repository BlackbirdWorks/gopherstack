package container

import (
	"net/netip"
	"os"
	"slices"
	"strings"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

const (
	// EnvInjectDNS is a comma-separated DNS server list given to every spawned container.
	EnvInjectDNS = "GOPHERSTACK_CONTAINER_DNS"
	// EnvInjectCAFile is a host CA PEM mounted into every spawned container and exported as CA bundle vars.
	EnvInjectCAFile = "GOPHERSTACK_CONTAINER_CA_FILE"

	// InjectedCAPath is where the CA lands inside containers.
	InjectedCAPath = "/etc/gopherstack/ca.pem"
)

// Injection points spawned containers at the embedded DNS and trusts the gopherstack CA,
// so SDKs inside them reach real AWS endpoint names transparently.
type Injection struct {
	CAFile string
	DNS    []string
}

// InjectionFromEnv reads [EnvInjectDNS] and [EnvInjectCAFile]; nil when neither is set.
func InjectionFromEnv() *Injection {
	var dns []string

	for s := range strings.SplitSeq(os.Getenv(EnvInjectDNS), ",") {
		if s = strings.TrimSpace(s); s != "" {
			dns = append(dns, s)
		}
	}

	ca := os.Getenv(EnvInjectCAFile)
	if len(dns) == 0 && ca == "" {
		return nil
	}

	return &Injection{DNS: dns, CAFile: ca}
}

// apply adds DNS servers, the CA bind mount and CA bundle env vars.
func (i *Injection) apply(cfg *dockercontainer.Config, hostCfg *dockercontainer.HostConfig) {
	if i == nil {
		return
	}

	for _, s := range i.DNS {
		if addr, err := netip.ParseAddr(s); err == nil {
			hostCfg.DNS = append(hostCfg.DNS, addr)
		}
	}

	if i.CAFile == "" {
		return
	}

	hostCfg.Binds = append(slices.Clone(hostCfg.Binds), i.CAFile+":"+InjectedCAPath+":ro")

	for _, k := range []string{"AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "NODE_EXTRA_CA_CERTS", "CURL_CA_BUNDLE"} {
		if !slices.ContainsFunc(cfg.Env, func(e string) bool { return strings.HasPrefix(e, k+"=") }) {
			cfg.Env = append(slices.Clone(cfg.Env), k+"="+InjectedCAPath)
		}
	}
}
