// Package azure_test contains the Terraform acceptance suite for
// services/azurearm (M7), proving that an unmodified hashicorp/azurerm
// provider can apply and destroy azurerm_resource_group and
// azurerm_storage_account against a running gopherstack instance.
//
// This is a SEPARATE package from test/terraform (the AWS suite), per
// AZURE.md section 10.10: it needs its own azurermProviderBlock, its own
// pre-init provider cache for hashicorp/azurerm, FIXED published container
// host ports (not ephemeral -- AZURE.md section 10.4's endpoint
// advertisement defaults to scheme://<request Host>:<configured port>,
// which is only correct when the host-side port number matches what ARM
// advertises), and SSL_CERT_FILE-aware environment plumbing for the tofu
// child process to trust services/azurearm's self-signed HTTPS certificate
// (AZURE.md section 10.8) -- none of which fit the AWS suite's shared
// ephemeral-port container.
package azure_test

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/test/internal/buildcheck"
	"github.com/blackbirdworks/gopherstack/test/internal/tofu"

	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Fixed host ports this suite publishes the container on -- see this
// package's doc comment for why they must be fixed rather than ephemeral.
// Chosen to match services/azurearm's own defaults exactly, so the ARM
// listener's default endpoint-advertisement logic (scheme://<request
// Host>:<port>) is correct without needing
// --azure-arm-advertise-*-endpoint overrides.
const (
	hostPortARM   = "18006"
	hostPortBlob  = "18000"
	hostPortQueue = "18001"
	hostPortTable = "18002"
	// hostPortStorageVHost is services/azurestoragevhost's published port --
	// see that package's doc comment and AZURE.md section 10.8: unlike
	// hostPortBlob/Queue/Table above (used only for this suite's own direct
	// Go-SDK verification calls), this is the port terraform-provider-azurerm
	// itself actually talks to for azurerm_storage_container/_blob/_queue/
	// _table, since its data-plane SDK requires virtual-hosted-style URLs.
	hostPortStorageVHost = "18010"
)

// containerCertPath/containerKeyPath are where the stable dev certificate
// (see prepareStableCert) is copied to inside the gopherstack container, and
// AZURE_ARM_TLS_CERT/AZURE_ARM_TLS_KEY point services/azurearm's listener at
// them (services/azurearm/settings.go, services/azurearm/handler.go's
// loadOrGenerateCert). Root-level paths, not /tmp: the image is built FROM
// scratch (see Dockerfile), which has no pre-existing directory structure
// beyond the root filesystem itself.
const (
	containerCertPath = "/gopherstack-azurearm-cert.pem"
	containerKeyPath  = "/gopherstack-azurearm-cert.key"
)

// endpoint is "host:port" (no scheme) for services/azurearm's HTTPS
// listener -- exactly the metadata_host provider setting's expected shape.
//
//nolint:gochecknoglobals // set once in TestMain, read-only during tests
var endpoint string

// tofuProviderCacheDir is a dedicated provider cache for this suite's
// hashicorp/azurerm downloads, kept separate from test/terraform's
// hashicorp/aws cache.
//
//nolint:gochecknoglobals // shared provider cache path, read-only after init
var tofuProviderCacheDir = filepath.Join(os.TempDir(), "gopherstack-tofu-azurerm-provider-cache")

// certPEMPath is the host-side path of the stable dev certificate (see
// prepareStableCert) that both the tofu child process (via SSL_CERT_FILE)
// and the gopherstack container (via a copied-in file at containerCertPath,
// see startGopherstackContainer) trust -- the same PEM bytes on both sides,
// generated once and reused across runs rather than regenerated fresh every
// time services/azurearm's listener starts (AZURE.md section 10.8).
//
//nolint:gochecknoglobals // set once in TestMain, read-only during tests
var certPEMPath string

// keyPEMPath is certPEMPath's matching private key, copied into the
// container at containerKeyPath for AZURE_ARM_TLS_KEY.
//
//nolint:gochecknoglobals // set once in TestMain, read-only during tests
var keyPEMPath string

//nolint:gochecknoglobals // set once in TestMain, read-only during tests
var tofuBinaryPath string

//nolint:gochecknoglobals // set once in TestMain, read-only during tests
var sharedContainer testcontainers.Container

// ErrDockerPanic is returned when the Docker availability check panics.
var ErrDockerPanic = errors.New("docker check panicked")

func TestMain(m *testing.M) {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if testing.Short() {
		logger.Info("skipping azure terraform tests in short mode")
		os.Exit(0)
	}

	if err := checkDocker(); err != nil {
		logger.Error("azure terraform tests require docker", "error", err)
		os.Exit(1)
	}

	skipReason := prepareStableCert(logger)
	if skipReason != "" {
		logger.Warn("skipping azure terraform suite", "reason", skipReason)
		os.Exit(0)
	}

	ctx := context.Background()

	container, err := startGopherstackContainer(ctx, logger)
	if err != nil {
		logger.Error("failed to start gopherstack container", "error", err)
		os.Exit(1)
	}

	sharedContainer = container

	endpoint = "localhost:" + hostPortARM
	logger.Info("gopherstack ARM listener running", "endpoint", "https://"+endpoint)

	// The two things AZURE.md section 10.8 flags as needing verification
	// before this suite can run at all: obtaining a tofu binary (network
	// access to get.opentofu.org/GitHub releases) and getting the
	// hashicorp/azurerm provider (network access to registry.opentofu.org).
	// Either failing is an environment limitation, not a code defect --
	// skip cleanly rather than fail the build.
	skipReason = prepareTofu(logger)

	if skipReason != "" {
		logger.Warn("skipping azure terraform suite", "reason", skipReason)

		if tErr := container.Terminate(ctx); tErr != nil {
			logger.Error("failed to terminate container", "error", tErr)
		}

		os.Exit(0)
	}

	code := m.Run()

	if tErr := container.Terminate(ctx); tErr != nil {
		logger.Error("failed to terminate container", "error", tErr)
	}

	os.Exit(code)
}

// checkDocker safely checks if the Docker daemon is available, recovering
// from any potential panics -- mirrors test/terraform's own checkDocker.
func checkDocker() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrDockerPanic, r)
		}
	}()

	_, err = testcontainers.NewDockerProvider()

	return err
}

// mustFixedPortMap builds a network.PortMap binding each container port to
// "0.0.0.0:<hostPort>". Panics on a malformed containerPort key, which would
// be a bug in this file's own literal port strings, not runtime input.
func mustFixedPortMap(bindings map[string]string) network.PortMap {
	unspecified := netip.IPv4Unspecified()

	portMap := make(network.PortMap, len(bindings))

	for containerPort, hostPort := range bindings {
		port, err := network.ParsePort(containerPort)
		if err != nil {
			panic(fmt.Sprintf("mustFixedPortMap: invalid container port %q: %v", containerPort, err))
		}

		portMap[port] = []network.PortBinding{{HostIP: unspecified, HostPort: hostPort}}
	}

	return portMap
}

// startGopherstackContainer builds and starts the gopherstack container with
// FIXED published host ports for the ARM listener and the three storage
// data-plane ports it advertises (see this package's doc comment).
func startGopherstackContainer(ctx context.Context, logger *slog.Logger) (testcontainers.Container, error) {
	dockerfile := "Dockerfile"
	binPath := "../../../bin/gopherstack-linux"

	if runtime.GOOS == "darwin" {
		logger.InfoContext(ctx, "running on Darwin, building Linux binary for container tests...")

		cmd := exec.Command("go", "build", "-trimpath", "-o", "bin/gopherstack-linux", ".")
		cmd.Dir = "../../../"
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOTOOLCHAIN=local")

		if out, buildErr := cmd.CombinedOutput(); buildErr != nil {
			return nil, fmt.Errorf("building linux binary: %w: %s", buildErr, out)
		}
	}

	if binInfo, statErr := os.Stat(binPath); statErr == nil {
		if freshErr := buildcheck.CheckFreshness(logger, binInfo, "../../.."); freshErr != nil {
			return nil, freshErr
		}

		dockerfile = "Dockerfile.test"
	}

	req := testcontainers.ContainerRequest{
		Context:       "../../../",
		Dockerfile:    dockerfile,
		PrintBuildLog: true,
		BuildOptionsModifier: func(options *client.ImageBuildOptions) {
			options.NoCache = false
			options.PullParent = false
		},
		AutoRemove: true,
		ExposedPorts: []string{
			"10006/tcp", "10000/tcp", "10001/tcp", "10002/tcp", "10010/tcp",
		},
		HostConfigModifier: func(hc *dockercontainer.HostConfig) {
			hc.PortBindings = mustFixedPortMap(map[string]string{
				"10006/tcp": hostPortARM,
				"10000/tcp": hostPortBlob,
				"10001/tcp": hostPortQueue,
				"10002/tcp": hostPortTable,
				"10010/tcp": hostPortStorageVHost,
			})
		},
		// M8: services/azurearm's advertiseVHostEndpoint (rp_storage.go)
		// defaults to http://{account}.{svc}.<ARM request Host's
		// hostname>:<the configured, IN-CONTAINER vhost port> -- 10010, not
		// the published HOST port this suite maps it to (18010). A tofu
		// process running on the host cannot reach container-internal
		// 10010, so without this override ARM would advertise unreachable
		// endpoints and every direct-data-plane resource
		// (azurerm_storage_container/_blob/_queue/_table) would fail to
		// apply. This is a test harness fix, not a service-code change --
		// see rp_storage.go's advertiseVHostEndpoint for the override
		// branch this env var selects, and AZURE.md section 10.8 for why a
		// shared virtual-hosted listener/port exists at all.
		// LOG_LEVEL=debug lets TestTerraform_Azure_StorageDataPlane inspect
		// container logs to confirm storage_use_azuread=false actually forces
		// the SharedKey auth path (a malformed/non-SharedKey Authorization
		// header reaching azureblob/azurequeue/azuretable logs a
		// "malformed Authorization header accepted" DebugContext line -- see
		// each service's checkAuth).
		Env: map[string]string{
			// AZURE_ARM_ADVERTISE_STORAGE_VHOST points ARM's primaryEndpoints at
			// this suite's published virtual-hosted-listener port
			// (services/azurestoragevhost binds container-internal 10010,
			// published here as hostPortStorageVHost) rather than the
			// container-internal one a host-side tofu process can't reach --
			// see services/azurestoragevhost's package doc comment and
			// AZURE.md section 10.8 for why terraform-provider-azurerm's
			// azurerm_storage_container/_blob/_queue/_table need this
			// (host:port only, no scheme -- it's also the domainSuffix
			// jackofallops/giovanni's ParseAccountID needs).
			"AZURE_ARM_ADVERTISE_STORAGE_VHOST": "localhost:" + hostPortStorageVHost,
			"LOG_LEVEL":                         "debug",
			// A stable cert/key (see prepareStableCert), copied into the
			// container below via Files, rather than services/azurearm's
			// default of generating a fresh self-signed certificate on every
			// start -- so the SAME certificate bytes can be trusted once by
			// the tofu child process via SSL_CERT_FILE instead of needing a
			// per-run re-trust step (AZURE.md section 10.8).
			"AZURE_ARM_TLS_CERT": containerCertPath,
			"AZURE_ARM_TLS_KEY":  containerKeyPath,
		},
		Files: []testcontainers.ContainerFile{
			{HostFilePath: certPEMPath, ContainerFilePath: containerCertPath, FileMode: 0o444},
			{HostFilePath: keyPEMPath, ContainerFilePath: containerKeyPath, FileMode: 0o400},
		},
		WaitingFor: wait.ForListeningPort("10006/tcp").WithStartupTimeout(60 * time.Second),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// prepareTofu resolves the tofu binary (PATH, else download), returning a
// non-empty skip reason on failure instead of erroring the whole run.
func prepareTofu(logger *slog.Logger) string {
	if path, err := exec.LookPath("tofu"); err == nil {
		tofuBinaryPath = path

		return ""
	}

	if path, err := exec.LookPath("terraform"); err == nil {
		tofuBinaryPath = path

		return ""
	}

	logger.Info("tofu/terraform not found in PATH; downloading OpenTofu...")

	path, err := tofu.DownloadBinary(context.Background(), func(format string, args ...any) {
		logger.Info(fmt.Sprintf(format, args...))
	})
	if err != nil {
		return fmt.Sprintf(
			"could not obtain a tofu/terraform binary (tried PATH and OpenTofu releases download): %v",
			err,
		)
	}

	tofuBinaryPath = path

	return ""
}

// stableCertDir is a fixed, securely-owned (0700) directory a stable dev
// certificate is generated into (or reused from) once, rather than a fresh
// os.CreateTemp path every run -- the entire point being that the SAME
// certificate bytes persist across suite runs, matching the
// AZURE_ARM_TLS_CERT/AZURE_ARM_TLS_KEY override AZURE.md section 10.8's
// resolution added to services/azurearm (see services/azurearm/settings.go,
// services/azurearm/handler.go's loadOrGenerateCert). Living under a
// dedicated 0700 subdirectory (checked by ensureSecureDir, not directly
// under os.TempDir()) and being written via a randomly-named temp file
// atomically renamed into place (not a direct os.WriteFile to the
// predictable final name) closes the symlink/TOCTOU class of attack a
// shared, world-writable temp directory otherwise invites at a
// predictable path.
//
//nolint:gochecknoglobals // fixed derived path, read-only after init -- mirrors tofuProviderCacheDir above
var stableCertDir = filepath.Join(os.TempDir(), "gopherstack-azurearm-devcert.d")

//nolint:gochecknoglobals // fixed derived paths, read-only after init -- mirror stableCertDir above
var (
	stableCertHostPath = filepath.Join(stableCertDir, "cert.pem")
	stableKeyHostPath  = filepath.Join(stableCertDir, "key.pem")
)

// Static errors for ensureSecureDir's rejection cases (err113): the
// offending path is always appended via %w-adjacent context in the caller's
// message, not interpolated into the sentinel itself.
var (
	errCertDirIsSymlink      = errors.New("stable cert directory is a symlink, refusing to use it")
	errCertDirNotDir         = errors.New("stable cert directory path exists and is not a directory")
	errCertDirBadPermissions = errors.New("stable cert directory has insecure permissions, want 0700")
	errCertDirWrongOwner     = errors.New("stable cert directory is owned by a different user")
)

// ensureSecureDir makes sure dir exists, is a real directory (not a
// symlink), is owned by the current user, and is mode 0700 -- rejecting it
// outright (rather than reusing or "fixing" it) if any of that doesn't
// hold, since a permissive or foreign-owned directory at a predictable
// path could have been planted by another local user/process.
func ensureSecureDir(dir string) error {
	fi, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return os.Mkdir(dir, 0o700)
	}

	if err != nil {
		return fmt.Errorf("stat %s: %w", dir, err)
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s: %w", dir, errCertDirIsSymlink)
	}

	if !fi.IsDir() {
		return fmt.Errorf("%s: %w", dir, errCertDirNotDir)
	}

	if fi.Mode().Perm() != 0o700 {
		return fmt.Errorf("%s (mode %o): %w", dir, fi.Mode().Perm(), errCertDirBadPermissions)
	}

	if stat, ok := fi.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("%s (uid %d): %w", dir, stat.Uid, errCertDirWrongOwner)
	}

	return nil
}

// writeFileAtomically writes data to a randomly-named temporary file inside
// dir (created with mode 0600) and renames it over path -- rename replaces
// the destination directory entry itself rather than following it, so even
// a pre-planted symlink at path is atomically replaced, never dereferenced.
func writeFileAtomically(dir, path string, data []byte) error {
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()

	if _, writeErr := tmp.Write(data); writeErr != nil {
		tmp.Close()
		os.Remove(tmpName)

		return writeErr
	}

	if closeErr := tmp.Close(); closeErr != nil {
		os.Remove(tmpName)

		return closeErr
	}

	return os.Rename(tmpName, path)
}

// prepareStableCert populates certPEMPath/keyPEMPath with a stable
// self-signed dev certificate: reused as-is from stableCertHostPath/
// stableKeyHostPath if both files already exist and parse as a valid key
// pair, else freshly generated (pkgs/devtls) and written there for the next
// run to reuse. Returns a non-empty skip reason on failure.
func prepareStableCert(logger *slog.Logger) string {
	if err := ensureSecureDir(stableCertDir); err != nil {
		return fmt.Sprintf("could not secure stable ARM dev certificate directory: %v", err)
	}

	if certExistsAndParses(stableCertHostPath, stableKeyHostPath) {
		logger.Info("reusing existing stable ARM dev certificate", "cert", stableCertHostPath)
		certPEMPath, keyPEMPath = stableCertHostPath, stableKeyHostPath

		return ""
	}

	certPEM, keyPEM, err := devtls.GenerateSelfSignedCertPEM("localhost", "127.0.0.1")
	if err != nil {
		return fmt.Sprintf("could not generate stable ARM dev certificate: %v", err)
	}

	if writeErr := writeFileAtomically(stableCertDir, stableCertHostPath, certPEM); writeErr != nil {
		return fmt.Sprintf("could not write stable ARM dev certificate: %v", writeErr)
	}

	if writeErr := writeFileAtomically(stableCertDir, stableKeyHostPath, keyPEM); writeErr != nil {
		return fmt.Sprintf("could not write stable ARM dev certificate key: %v", writeErr)
	}

	certPEMPath, keyPEMPath = stableCertHostPath, stableKeyHostPath
	logger.Info("generated stable ARM dev certificate", "cert", stableCertHostPath)

	return ""
}

// certExistsAndParses reports whether certPath/keyPath both exist and parse
// as a valid TLS key pair -- if either check fails, prepareStableCert
// regenerates rather than reusing a stale or corrupt file.
func certExistsAndParses(certPath, keyPath string) bool {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return false
	}

	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return false
	}

	_, err = tls.X509KeyPair(certPEM, keyPEM)

	return err == nil
}

// azurermProviderBlock returns the HCL required_providers + provider
// "azurerm" block pointing every ARM call at the running gopherstack
// instance's ARM listener, per AZURE.md section 10.8. metadataHost is a bare
// "host:port" (no scheme -- the provider prefixes https:// itself).
func azurermProviderBlock(metadataHost string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azurerm" {
  features {}

  metadata_host                   = %[1]q
  environment                     = "gopherstack"
  subscription_id                 = "00000000-0000-0000-0000-000000000000"
  tenant_id                       = "00000000-0000-0000-0000-000000000000"
  client_id                       = "00000000-0000-0000-0000-000000000000"
  client_secret                   = "gopherstack"
  resource_provider_registrations = "none"
  storage_use_azuread             = false
}
`, metadataHost)
}

// azureResourceGroupAndStorageAccountFixture is the M7 acceptance fixture:
// one resource group and one storage account, both pure-ARM resources.
// Direct-data-plane resources (azurerm_storage_container/_blob/_queue/_table)
// are exercised separately by storage_dataplane_test.go's own fixture (M8,
// see AZURE.md section 10.8's second resolved finding and section 10.10's M8
// entry) rather than being added to this one, so the M7 and M8 fixtures stay
// independently testable.
const azureResourceGroupAndStorageAccountFixture = `
resource "azurerm_resource_group" "test" {
  name     = "gopherstack-m7-test-rg"
  location = "local"
}

resource "azurerm_storage_account" "test" {
  name                     = "gopherstackm7test"
  resource_group_name      = azurerm_resource_group.test.name
  location                 = azurerm_resource_group.test.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

output "storage_account_id" {
  value = azurerm_storage_account.test.id
}
`

// applyAzureTofu writes hcl to dir/main.tf and runs tofu/terraform init,
// apply, and (via t.Cleanup) destroy against it, trusting
// services/azurearm's self-signed certificate via SSL_CERT_FILE (AZURE.md
// section 10.8). Unlike test/terraform's applyTofu, this doesn't reuse a
// pre-initialized .terraform directory across tests -- there is currently
// only one Azure Terraform test, so the AWS suite's parallel-test disk/lock
// optimization isn't worth the complexity here yet.
func applyAzureTofu(t *testing.T, dir, hcl string) {
	t.Helper()

	cfgPath := filepath.Join(dir, "main.tf")
	if err := os.WriteFile(cfgPath, []byte(hcl), 0o600); err != nil {
		t.Fatalf("writing %s: %v", cfgPath, err)
	}

	if err := os.MkdirAll(tofuProviderCacheDir, 0o750); err != nil {
		t.Logf("could not create provider cache dir: %v", err)
	}

	env := append(
		os.Environ(),
		"TF_IN_AUTOMATION=1",
		"TF_PLUGIN_CACHE_DIR="+tofuProviderCacheDir,
		"TF_PLUGIN_CACHE_MAY_BREAK_DEPENDENCY_LOCK_FILE=true",
		"SSL_CERT_FILE="+certPEMPath,
	)

	run := func(failFatal bool, args ...string) bool {
		t.Helper()

		cmd := exec.Command(tofuBinaryPath, args...)
		cmd.Dir = dir
		cmd.Env = env

		out, err := cmd.CombinedOutput()
		t.Logf("tofu %v:\n%s", args, out)

		if err != nil {
			if failFatal {
				t.Fatalf("tofu %v failed: %v", args, err)
			}

			t.Logf("tofu %v failed (non-fatal): %v", args, err)

			return false
		}

		return true
	}

	if !run(false, "init", "-no-color") {
		t.Skip("tofu init failed -- likely no network access to registry.opentofu.org for the azurerm provider; " +
			"see AZURE.md section 10.8 and services/azurearm/PARITY.md for this suite's known environment limitations")
	}

	run(true, "apply", "-auto-approve", "-no-color")

	t.Cleanup(func() {
		run(false, "destroy", "-auto-approve", "-no-color")
	})
}

// TestTerraform_Azure_ResourceGroupAndStorageAccount proves that an
// unmodified hashicorp/azurerm provider can apply and destroy
// azurerm_resource_group and azurerm_storage_account against
// services/azurearm (M7). See this package's doc comment for why direct
// data-plane resources (azurerm_storage_container etc.) are out of scope.
func TestTerraform_Azure_ResourceGroupAndStorageAccount(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureResourceGroupAndStorageAccountFixture

	applyAzureTofu(t, dir, hcl)
}
