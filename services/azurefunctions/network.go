package azurefunctions

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// Azurite's published development account; the emulator does not validate the key.
const (
	devAccount = "devstoreaccount1"
	devKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	devStorage = "usedevelopmentstorage=true"
)

var loopbackHostRe = regexp.MustCompile(`(?i)(://|@)(127\.0\.0\.1|localhost|\[::1\]|0\.0\.0\.0)([:/;,\s]|$)`)

// Network tells function hosts how to reach the emulator's data-plane listeners from inside a container.
type Network struct {
	// Host is the address containers use for the emulator; empty picks the container runtime's default gateway.
	Host      string
	BlobPort  int
	QueuePort int
	TablePort int
}

// SetNetwork wires the container-visible address of the emulator's storage listeners.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	defer b.mu.Unlock()

	if n.Host == "" {
		n.Host = b.network.Host
	}

	b.network = n
}

func defaultContainerHost() string {
	if h := os.Getenv("LAMBDA_DOCKER_HOST"); h != "" {
		return h
	}

	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		return "host.docker.internal"
	}

	return "172.17.0.1"
}

func (n Network) host() string {
	if n.Host != "" {
		return n.Host
	}

	return defaultContainerHost()
}

func (n Network) devConnectionString(host string) string {
	ep := func(port int) string {
		return "http://" + host + ":" + strconv.Itoa(port) + "/" + devAccount
	}

	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;QueueEndpoint=%s;TableEndpoint=%s;",
		devAccount, devKey, ep(n.BlobPort), ep(n.QueuePort), ep(n.TablePort),
	)
}

// rewrite points loopback addresses in an app setting at host, and expands UseDevelopmentStorage=true.
func (n Network) rewrite(value, host string) string {
	if strings.EqualFold(strings.TrimSuffix(strings.TrimSpace(value), ";"), devStorage) && n.BlobPort != 0 {
		return n.devConnectionString(host)
	}

	return loopbackHostRe.ReplaceAllString(value, "${1}"+host+"${3}")
}

// forContainer returns a copy of the app whose settings reach the emulator from inside a container.
func (b *InMemoryBackend) forContainer(st *appState) *appState {
	b.mu.RLock("forContainer")
	n := b.network
	b.mu.RUnlock()

	host := n.host()
	out := *st
	out.Settings = make(map[string]string, len(st.Settings))

	for k, v := range st.Settings {
		if k == settingRunFromPkg {
			out.Settings[k] = v

			continue
		}

		out.Settings[k] = n.rewrite(v, host)
	}

	return &out
}
