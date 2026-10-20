package app

import (
	"log/slog"
	"net"
	"os"
	"strings"
)

// NetworkSettings controls the listen address of the main HTTP listener.
type NetworkSettings struct {
	Host        string `name:"host"         env:"GOPHERSTACK_HOST"         default:"" help:"Listen host for the HTTP listener. Empty = all interfaces (unchanged default)."` //nolint:lll // config struct tags are intentionally verbose
	AllowRemote bool   `name:"allow-remote" env:"GOPHERSTACK_ALLOW_REMOTE"            help:"Acknowledge a non-loopback listen address (silences the startup warning)."`      //nolint:lll // config struct tags are intentionally verbose
}

// listenAddr joins host and port; an already-joined host:port passes through.
func listenAddr(host, port string) string {
	port = strings.TrimPrefix(port, ":")
	if host == "" {
		return ":" + port
	}

	return net.JoinHostPort(host, port)
}

// normalizeListenAddr turns a bare port into ":port" and leaves host:port alone.
func normalizeListenAddr(addr string) string {
	if strings.Contains(addr, ":") {
		return addr
	}

	return ":" + addr
}

// listenPortSuffix returns ":port" from a listen address, for display.
func listenPortSuffix(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i:]
	}

	return ":" + addr
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(strings.Trim(host, "[]"))

	return ip != nil && ip.IsLoopback()
}

func inContainer() bool {
	_, err := os.Stat("/.dockerenv")

	return err == nil
}

// warnIfRemote logs a prominent warning when the listener is reachable off-host
// and the operator has not acknowledged it (containers are expected to bind widely).
func warnIfRemote(log *slog.Logger, n NetworkSettings, container bool) bool {
	if isLoopbackHost(n.Host) || n.AllowRemote || container {
		return false
	}

	log.Warn("SECURITY: listening on a non-loopback address; Gopherstack has no authentication "+
		"and is reachable from the network. Use --host 127.0.0.1, or set --allow-remote / "+
		"GOPHERSTACK_ALLOW_REMOTE=1 to acknowledge.", "host", n.Host)

	return true
}

// hostedPorts prefixes each port with host so extra listeners follow --host.
func hostedPorts(host string, ports []string) []string {
	if host == "" {
		return ports
	}

	out := make([]string, 0, len(ports))
	for _, p := range ports {
		out = append(out, listenAddr(host, p))
	}

	return out
}
