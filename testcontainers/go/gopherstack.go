// Package gopherstack is a testcontainers-go module that runs the Gopherstack
// multi-cloud emulator (AWS, Azure, GCP) as a throwaway container.
package gopherstack

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// DefaultImage is the published Gopherstack image.
	DefaultImage = "ghcr.io/blackbirdworks/gopherstack:latest"

	// HealthPath is polled to decide the container is ready.
	HealthPath = "/_gopherstack/health"

	// APIPort is the main AWS-compatible HTTP port.
	APIPort = "8000"

	// DefaultRegion is the region AWSConfig signs for.
	DefaultRegion = "us-east-1"

	// AzureAccountName is the Azurite-compatible well-known account.
	AzureAccountName = "devstoreaccount1"
	// AzureAccountKey is the published key for AzureAccountName.
	AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	dataDir        = "/data"
	dockerSock     = "/var/run/docker.sock"
	startupTimeout = 2 * time.Minute
)

// Fixed Azure listener ports.
const (
	AzureBlobPort     = "10000"
	AzureQueuePort    = "10001"
	AzureTablePort    = "10002"
	AzureServiceBus   = "10003"
	AzureKeyVault     = "10004"
	AzureEventHubs    = "10005"
	AzureARM          = "10006"
	AzureEventGrid    = "10007"
	AzureAppConfig    = "10008"
	AzureFile         = "10009"
	AzureMonitor      = "10013"
	AzureACR          = "10011"
	AzureStorageVHost = "10010"
	AzureCosmosDB     = "8081"
)

func ociPorts() map[string]string {
	return map[string]string{"objectstorage": "8150", "vault": "8151", "streaming": "8152", "identity": "8153", "functions": "8154", "core": "8155", "queue": "8156", "notifications": "8157", "containerengine": "8158", "mysql": "8159", "logging": "8160", "monitoring": "8161", "nosql": "8162", "apigateway": "8163", "events": "8164", "loadbalancer": "8165", "artifacts": "8166"}
}

func gcpPorts() map[string]string {
	return map[string]string{
		"storage":          "4443",
		"firestore":        "8080",
		"pubsub":           "8085",
		"secretmanager":    "8086",
		"kms":              "8087",
		"cloudtasks":       "8123",
		"scheduler":        "8124",
		"iam":              "8125",
		"run":              "8126",
		"functions":        "8128",
		"sql":              "8130",
		"logging":          "8131",
		"monitoring":       "8132",
		"redis":            "8133",
		"artifactregistry": "8134",
		"spanner":          "8135",
		"cloudbuild":       "8136",
		"bigtable":         "8137",
		"eventarc":         "8138",
		"workflows":        "8139",
		"dns":              "8140",
		"dnsserver":        "8141",
		"compute":          "8142",
		"datastore":        "8143",
		"container":        "8144",
		"identitytoolkit":  "8145",
		"apigateway":       "8146",
		"batch":            "8147",
		"bigquery":         "9050",
	}
}

func azurePorts() []string {
	return []string{
		AzureBlobPort, AzureQueuePort, AzureTablePort, AzureServiceBus, AzureKeyVault,
		AzureEventHubs, AzureARM, AzureEventGrid, AzureAppConfig, AzureFile, AzureMonitor, AzureACR, AzureStorageVHost,
		AzureCosmosDB,
	}
}

// ErrUnknownGCPService is returned by GCPEndpoint for an unsupported service name.
var ErrUnknownGCPService = errors.New("unknown gcp service")

// ErrUnknownOCIService is returned by OCIEndpoint for an unknown service name.
var ErrUnknownOCIService = errors.New("unknown oci service")

// Container is a running Gopherstack container.
type Container struct {
	testcontainers.Container
}

type settings struct {
	env         map[string]string
	persistDir  string
	services    []string
	customizers []testcontainers.ContainerCustomizer
	dockerSock  bool
}

// Option configures Run.
type Option func(*settings)

// WithServices restricts the emulator to the named services (GOPHERSTACK_SERVICES).
func WithServices(names ...string) Option {
	return func(s *settings) { s.services = append(s.services, names...) }
}

// WithPersistence enables snapshot persistence, bind-mounting hostDir at /data.
func WithPersistence(hostDir string) Option {
	return func(s *settings) { s.persistDir = hostDir }
}

// WithDockerSocket mounts the host Docker socket so Lambda/RDS/ECS docker
// engines can start sibling containers. This grants the emulator control of
// the host Docker daemon; use only in trusted test environments.
func WithDockerSocket() Option {
	return func(s *settings) { s.dockerSock = true }
}

// WithEnv sets an environment variable in the container.
func WithEnv(key, value string) Option {
	return func(s *settings) { s.env[key] = value }
}

// WithCustomizer applies arbitrary testcontainers customizers.
func WithCustomizer(c ...testcontainers.ContainerCustomizer) Option {
	return func(s *settings) { s.customizers = append(s.customizers, c...) }
}

func exposed() []string {
	azure, gcp, oci := azurePorts(), gcpPorts(), ociPorts()
	ports := make([]string, 0, 1+len(azure)+len(gcp)+len(oci))
	ports = append(ports, APIPort+"/tcp")

	for _, p := range azure {
		ports = append(ports, p+"/tcp")
	}

	for _, p := range gcp {
		ports = append(ports, p+"/tcp")
	}

	for _, p := range oci {
		ports = append(ports, p+"/tcp")
	}

	return ports
}

// Run starts Gopherstack from image (DefaultImage when empty) and waits for
// its health endpoint.
func Run(ctx context.Context, image string, opts ...Option) (*Container, error) {
	if image == "" {
		image = DefaultImage
	}

	s := &settings{env: map[string]string{}}
	for _, o := range opts {
		o(s)
	}

	if len(s.services) > 0 {
		s.env["GOPHERSTACK_SERVICES"] = strings.Join(s.services, ",")
	}

	var binds []string

	if s.persistDir != "" {
		s.env["PERSIST"] = "true"
		s.env["GOPHERSTACK_DATA_DIR"] = dataDir
		binds = append(binds, s.persistDir+":"+dataDir)
	}

	if s.dockerSock {
		binds = append(binds, dockerSock+":"+dockerSock)
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: exposed(),
		Env:          s.env,
		WaitingFor: wait.ForHTTP(HealthPath).WithPort(APIPort + "/tcp").
			WithStartupTimeout(startupTimeout),
	}

	if len(binds) > 0 {
		req.HostConfigModifier = func(hc *container.HostConfig) { hc.Binds = append(hc.Binds, binds...) }
	}

	genericReq := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	for _, c := range s.customizers {
		if err := c.Customize(&genericReq); err != nil {
			return nil, fmt.Errorf("customize gopherstack container: %w", err)
		}
	}

	ctr, err := testcontainers.GenericContainer(ctx, genericReq)
	if err != nil {
		if ctr != nil {
			return &Container{Container: ctr}, fmt.Errorf("run gopherstack container: %w", err)
		}

		return nil, fmt.Errorf("run gopherstack container: %w", err)
	}

	return &Container{Container: ctr}, nil
}

func (c *Container) hostPort(ctx context.Context, port string) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("container host: %w", err)
	}

	mapped, err := c.MappedPort(ctx, port+"/tcp")
	if err != nil {
		return "", fmt.Errorf("mapped port %s: %w", port, err)
	}

	return host + ":" + mapped.Port(), nil
}

// Endpoint returns the AWS API base URL, e.g. http://localhost:32768.
func (c *Container) Endpoint(ctx context.Context) (string, error) {
	hp, err := c.hostPort(ctx, APIPort)
	if err != nil {
		return "", err
	}

	return "http://" + hp, nil
}

// AWSConfig returns an aws.Config with static test credentials and BaseEndpoint
// pointing at the container.
func (c *Container) AWSConfig(ctx context.Context) (aws.Config, error) {
	ep, err := c.Endpoint(ctx)
	if err != nil {
		return aws.Config{}, err
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(DefaultRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load aws config: %w", err)
	}

	cfg.BaseEndpoint = aws.String(ep)

	return cfg, nil
}

// AzureEndpoints returns the base URL (http://host:port, no account path) of
// every Azure listener, keyed by "blob", "queue", "table", "servicebus",
// "keyvault", "eventhubs", "arm", "eventgrid", "appconfig", "file", "monitor", "acr", "storagevhost", "cosmosdb".
func (c *Container) AzureEndpoints(ctx context.Context) (map[string]string, error) {
	named := map[string]string{
		"blob": AzureBlobPort, "queue": AzureQueuePort, "table": AzureTablePort,
		"servicebus": AzureServiceBus, "keyvault": AzureKeyVault, "eventhubs": AzureEventHubs,
		"arm": AzureARM, "eventgrid": AzureEventGrid, "appconfig": AzureAppConfig, "file": AzureFile, "monitor": AzureMonitor, "acr": AzureACR,
		"storagevhost": AzureStorageVHost, "cosmosdb": AzureCosmosDB,
	}
	out := make(map[string]string, len(named))

	for name, port := range named {
		hp, err := c.hostPort(ctx, port)
		if err != nil {
			return nil, err
		}

		out[name] = "http://" + hp
	}

	return out, nil
}

// AzureConnectionString returns an Azurite-style Azure Storage connection
// string for the well-known devstoreaccount1 identity.
func (c *Container) AzureConnectionString(ctx context.Context) (string, error) {
	eps, err := c.AzureEndpoints(ctx)
	if err != nil {
		return "", err
	}

	acct := "/" + AzureAccountName

	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;"+
			"BlobEndpoint=%s%s;QueueEndpoint=%s%s;TableEndpoint=%s%s;FileEndpoint=%s%s;",
		AzureAccountName, AzureAccountKey,
		eps["blob"], acct, eps["queue"], acct, eps["table"], acct, eps["file"], acct,
	), nil
}

// GCPEndpoint returns host:port for a GCP service (storage, firestore, pubsub,
// secretmanager, kms, cloudtasks, scheduler, iam, run, functions, sql,
// bigquery). For storage this is the value of STORAGE_EMULATOR_HOST.
func (c *Container) GCPEndpoint(ctx context.Context, service string) (string, error) {
	port, ok := gcpPorts()[strings.ToLower(service)]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownGCPService, service)
	}

	return c.hostPort(ctx, port)
}

// OCIEndpoint returns the http URL of an OCI service (objectstorage, vault, streaming, identity, functions, core, queue, notifications), usable as the OCI SDK client Host.
func (c *Container) OCIEndpoint(ctx context.Context, service string) (string, error) {
	port, ok := ociPorts()[strings.ToLower(service)]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownOCIService, service)
	}

	hp, err := c.hostPort(ctx, port)
	if err != nil {
		return "", err
	}

	return "http://" + hp, nil
}
