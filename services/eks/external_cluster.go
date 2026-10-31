package eks

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/container"

	"gopkg.in/yaml.v3"
)

// ErrInvalidKubeconfig is returned when the existing-cluster kubeconfig cannot be used.
var ErrInvalidKubeconfig = errors.New("invalid EKS existing-cluster kubeconfig")

type externalCluster struct {
	endpoint string
	ca       string
}

type kubeconfigFile struct {
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server string `yaml:"server"`
			CAData string `yaml:"certificate-authority-data"`
			CAFile string `yaml:"certificate-authority"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster string `yaml:"cluster"`
		} `yaml:"context"`
	} `yaml:"contexts"`
}

// loadExternalCluster reads the endpoint and CA of kubeContext (or current-context) from a kubeconfig file.
func loadExternalCluster(kubeconfigPath, kubeContext string) (*externalCluster, error) {
	raw, err := os.ReadFile(kubeconfigPath) //nolint:gosec // operator-configured kubeconfig path
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
	}

	var kc kubeconfigFile
	if err = yaml.Unmarshal(raw, &kc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
	}

	if kubeContext == "" {
		kubeContext = kc.CurrentContext
	}

	clusterName := ""

	for _, c := range kc.Contexts {
		if c.Name == kubeContext {
			clusterName = c.Context.Cluster
		}
	}

	if clusterName == "" {
		return nil, fmt.Errorf("%w: context %q not found", ErrInvalidKubeconfig, kubeContext)
	}

	for _, c := range kc.Clusters {
		if c.Name == clusterName {
			return externalFromEntry(kubeconfigPath, clusterName, c.Cluster.Server, c.Cluster.CAData, c.Cluster.CAFile)
		}
	}

	return nil, fmt.Errorf("%w: cluster %q not found", ErrInvalidKubeconfig, clusterName)
}

func externalFromEntry(kubeconfigPath, name, server, caData, caFile string) (*externalCluster, error) {
	if server == "" {
		return nil, fmt.Errorf("%w: cluster %q has no server", ErrInvalidKubeconfig, name)
	}

	if caData != "" || caFile == "" {
		return &externalCluster{endpoint: server, ca: caData}, nil
	}

	if !filepath.IsAbs(caFile) {
		caFile = filepath.Join(filepath.Dir(kubeconfigPath), caFile)
	}

	pem, err := os.ReadFile(caFile) //nolint:gosec // path comes from the operator's kubeconfig
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKubeconfig, err)
	}

	return &externalCluster{endpoint: server, ca: base64.StdEncoding.EncodeToString(pem)}, nil
}

// EnableExistingCluster fronts the kubeconfig's cluster: DescribeCluster returns its endpoint and CA, no
// container starts, and DeleteCluster leaves the external cluster untouched.
func (b *InMemoryBackend) EnableExistingCluster(kubeconfigPath, kubeContext string) error {
	ext, err := loadExternalCluster(kubeconfigPath, kubeContext)
	if err != nil {
		return err
	}

	b.mu.Lock("EnableExistingCluster")
	defer b.mu.Unlock()

	b.external = ext

	return nil
}

func (b *InMemoryBackend) externalProvider() *externalCluster {
	b.mu.RLock("externalProvider")
	defer b.mu.RUnlock()

	return b.external
}

func (b *InMemoryBackend) applyExternalEndpoint(cp *Cluster) {
	if b.external != nil && cp.Status == statusActive {
		cp.Endpoint = b.external.endpoint
		cp.CertificateAuthority = b.external.ca
	}
}

// PublishedLBPorts returns the host ports published for the cluster's load-balancer ports.
func (b *InMemoryBackend) PublishedLBPorts(name string) ([]PublishedPort, error) {
	b.mu.RLock("PublishedLBPorts")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(name); !ok {
		return nil, errMsgf(ErrNotFound, "No cluster found for name: %s.", name)
	}

	out := []PublishedPort{}

	if b.clusterEng == nil {
		return out, nil
	}

	lc, ok := b.clusterEng.clusters[name]
	if !ok {
		return out, nil
	}

	host := container.BindHostFor(b.clusterEng.cfg.Host)
	if host == container.AllInterfacesHost {
		host = b.clusterEng.cfg.Host
	}

	for i, h := range lc.lbHost {
		out = append(out, PublishedPort{Host: host, ContainerPort: lc.lbPorts[i], HostPort: h})
	}

	return out, nil
}
