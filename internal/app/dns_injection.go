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
	HTTPSAddr    string   `name:"https-addr"        env:"TRANSPARENT_HTTPS_ADDR"        default:":443" help:"HTTPS listen addr."`                                                                   //nolint:lll // config struct tags are intentionally verbose
	ContainerDNS string   `name:"container-dns"     env:"TRANSPARENT_CONTAINER_DNS"                    help:"DNS IP for spawned containers."`                                                       //nolint:lll // config struct tags are intentionally verbose
	CADir        string   `name:"ca-dir"            env:"TRANSPARENT_CA_DIR"                           help:"Directory persisting the CA (default: ephemeral)."`                                    //nolint:lll // config struct tags are intentionally verbose
	CAHostPath   string   `name:"container-ca-path" env:"TRANSPARENT_CONTAINER_CA_PATH"                help:"CA PEM path visible to the Docker daemon (required when gopherstack runs in Docker)."` //nolint:lll // config struct tags are intentionally verbose
	DNSSkip      []string `name:"dns-skip"          env:"TRANSPARENT_DNS_SKIP"                         help:"Names/zones that keep resolving upstream."`                                            //nolint:lll // config struct tags are intentionally verbose
	DNSUpstream  []string `name:"dns-upstream"      env:"TRANSPARENT_DNS_UPSTREAM"                     help:"Upstream resolvers host:port."`                                                        //nolint:lll // config struct tags are intentionally verbose
	Enabled      bool     `name:"dns"               env:"TRANSPARENT_DNS"                              help:"Resolve AWS endpoint names here and serve HTTPS."`                                     //nolint:lll // config struct tags are intentionally verbose
}

// transparentRuntime holds the CA used by the HTTPS listener and container injection.
type transparentRuntime struct {
	ca       *devtls.CA
	srv      *transparent.Server
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

	if injErr := injectContainers(ctx, st.ContainerDNS, st.CAHostPath, resolveIP, ca); injErr != nil {
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
func injectContainers(ctx context.Context, containerDNS, caHostPath, resolveIP string, ca *devtls.CA) error {
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

	caFile, err := containerCAFile(ctx, caHostPath, inDocker(), ca)
	if err != nil {
		return err
	}

	if dnsIP != "" {
		if sErr := os.Setenv(container.EnvInjectDNS, dnsIP); sErr != nil {
			return fmt.Errorf("transparent: %w", sErr)
		}
	}

	if caFile == "" {
		return nil
	}

	if sErr := os.Setenv(container.EnvInjectCAFile, caFile); sErr != nil {
		return fmt.Errorf("transparent: %w", sErr)
	}

	return nil
}

func inDocker() bool {
	_, err := os.Stat("/.dockerenv")

	return err == nil
}

// containerCAFile returns the CA path to bind into containers, or "" to skip injection.
// A containerised gopherstack's private temp dir does not exist on the Docker daemon host.
func containerCAFile(ctx context.Context, hostPath string, containerised bool, ca *devtls.CA) (string, error) {
	if hostPath == "" && containerised {
		logger.Load(ctx).WarnContext(ctx,
			"transparent: skipping container CA injection; set --transparent-container-ca-path to a Docker-shared path")

		return "", nil
	}

	caFile := hostPath

	if caFile == "" {
		dir, err := os.MkdirTemp("", "gopherstack-ca-")
		if err != nil {
			return "", fmt.Errorf("transparent ca dir: %w", err)
		}

		go func() {
			<-ctx.Done()
			_ = os.RemoveAll(dir)
		}()

		caFile = filepath.Join(dir, "ca.pem")
	}

	if wErr := os.WriteFile(caFile, ca.CertPEM(), 0o644); wErr != nil { //nolint:gosec // public cert, world-readable
		return "", fmt.Errorf("transparent ca file: %w", wErr)
	}

	return caFile, nil
}

// serveHTTPS registers the CA download route and starts the HTTPS listener on the router.
// Call after the Echo server is fully built, before serving.
func (t *transparentRuntime) serveHTTPS(ctx context.Context, e *echo.Echo) error {
	e.GET(transparent.CAPath, echo.WrapHandler(transparent.CAHandler(t.ca)))

	srv, err := transparent.Start(ctx, t.httpsAdr, e, t.ca)
	if err != nil {
		return fmt.Errorf("transparent https: %w", err)
	}

	t.srv = srv

	return nil
}
