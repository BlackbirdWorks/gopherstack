package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/transparent"
)

const defaultTransparentDNSAddr = ":53"

// TransparentSettings configures transparent endpoint injection: unmodified SDKs
// targeting real *.amazonaws.com names reach gopherstack via the embedded DNS and an HTTPS listener.
type TransparentSettings struct {
	DNSSkip      []string `name:"dns-skip"      env:"TRANSPARENT_DNS_SKIP"      help:"Names/zones that keep resolving upstream (e.g. sts.amazonaws.com)."`
	DNSUpstream  []string `name:"dns-upstream"  env:"TRANSPARENT_DNS_UPSTREAM"  help:"Upstream resolvers host:port for non-AWS names (default: /etc/resolv.conf)."`
	HTTPSAddr    string   `name:"https-addr"    env:"TRANSPARENT_HTTPS_ADDR"    help:"HTTPS listen address for real AWS endpoint names." default:":443"`
	ContainerDNS string   `name:"container-dns" env:"TRANSPARENT_CONTAINER_DNS" help:"DNS IP handed to spawned containers (default: --dns-resolve-ip unless loopback)."`
	CADir        string   `name:"ca-dir"        env:"TRANSPARENT_CA_DIR"        help:"Directory to persist the CA so it is trusted once (default: ephemeral)."`
	Enabled      bool     `name:"dns"           env:"TRANSPARENT_DNS"           help:"Resolve *.amazonaws.com (and .com.cn, api.aws) to this server and serve HTTPS for them."`
}

// transparentRuntime holds the CA used by the HTTPS listener and container injection.
type transparentRuntime struct {
	ca       *devtls.CA
	httpsAdr string
}

// setupTransparent starts the DNS server in transparent mode, creates the CA and
// configures container DNS/CA injection. Call before services initialize.
func setupTransparent(
	ctx context.Context,
	st TransparentSettings,
	dnsAddr, resolveIP string,
) (*transparentRuntime, *gopherDNS.Server, error) {
	log := logger.Load(ctx)

	ca, err := loadTransparentCA(st.CADir)
	if err != nil {
		return nil, nil, err
	}

	addr := dnsAddr
	if addr == "" {
		addr = defaultTransparentDNSAddr
	}

	dnsSrv, err := gopherDNS.New(gopherDNS.Config{
		ListenAddr: addr,
		ResolveIP:  resolveIP,
		Logger:     log,
		Transparent: gopherDNS.TransparentConfig{
			Suffixes:  gopherDNS.DefaultTransparentSuffixes(),
			Skip:      st.DNSSkip,
			Upstreams: st.DNSUpstream,
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("transparent dns: %w", err)
	}

	if startErr := dnsSrv.Start(ctx); startErr != nil {
		return nil, nil, fmt.Errorf("transparent dns start: %w", startErr)
	}

	if injErr := injectContainers(ctx, st.ContainerDNS, resolveIP, ca); injErr != nil {
		return nil, nil, injErr
	}

	httpsAddr := st.HTTPSAddr
	if httpsAddr == "" {
		httpsAddr = transparent.DefaultAddr
	}

	log.InfoContext(ctx, "Transparent endpoint injection enabled",
		"dns", addr, "https", httpsAddr, "ca", transparent.CAPath)

	return &transparentRuntime{ca: ca, httpsAdr: httpsAddr}, dnsSrv, nil
}

func loadTransparentCA(dir string) (*devtls.CA, error) {
	if dir == "" {
		ca, err := devtls.NewCA()
		if err != nil {
			return nil, fmt.Errorf("transparent ca: %w", err)
		}

		return ca, nil
	}

	ca, err := devtls.LoadOrCreateCA(dir)
	if err != nil {
		return nil, fmt.Errorf("transparent ca: %w", err)
	}

	return ca, nil
}

// injectContainers exports the env read by container.NewRuntime so every spawned
// engine container (Lambda, CodeBuild, ECS, ...) resolves via the embedded DNS and trusts the CA.
func injectContainers(ctx context.Context, containerDNS, resolveIP string, ca *devtls.CA) error {
	dnsIP := containerDNS
	if dnsIP == "" {
		if ip := net.ParseIP(resolveIP); ip != nil && !ip.IsLoopback() {
			dnsIP = resolveIP
		}
	}

	if dnsIP == "" {
		logger.Load(ctx).WarnContext(ctx,
			"transparent: containers keep default DNS; set --transparent-container-dns to a host IP reachable from containers")
	}

	dir, err := os.MkdirTemp("", "gopherstack-ca-")
	if err != nil {
		return fmt.Errorf("transparent ca dir: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = os.RemoveAll(dir)
	}()

	caFile := filepath.Join(dir, "ca.pem")
	if wErr := os.WriteFile(caFile, ca.CertPEM(), 0o644); wErr != nil { //nolint:gosec // public CA cert must be readable by container users
		return fmt.Errorf("transparent ca file: %w", wErr)
	}

	if dnsIP != "" {
		if sErr := os.Setenv(container.EnvInjectDNS, dnsIP); sErr != nil {
			return fmt.Errorf("transparent: %w", sErr)
		}
	}

	if sErr := os.Setenv(container.EnvInjectCAFile, caFile); sErr != nil {
		return fmt.Errorf("transparent: %w", sErr)
	}

	return nil
}

// serveHTTPS registers the CA download route and starts the HTTPS listener on the router.
// Call after the Echo server is fully built, before serving.
func (t *transparentRuntime) serveHTTPS(ctx context.Context, e *echo.Echo) error {
	e.GET(transparent.CAPath, echo.WrapHandler(transparent.CAHandler(t.ca)))

	if _, err := transparent.Start(ctx, t.httpsAdr, e, t.ca); err != nil {
		return fmt.Errorf("transparent https: %w", err)
	}

	return nil
}
