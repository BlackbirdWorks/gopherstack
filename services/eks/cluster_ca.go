package eks

import (
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const clusterCASerialBits = 127

var errNotReady = k3sengine.ErrNotReady

// clusterCA is the server CA the emulator pre-provisions into k3s so the probe never has to trust-on-first-use.
type clusterCA struct {
	certPEM []byte
	keyPEM  []byte
}

func newClusterCA(name string) (*clusterCA, error) {
	ca, err := k3sengine.NewCA("gopherstack-eks-" + name + "-ca")
	if err != nil {
		return nil, err
	}

	return &clusterCA{certPEM: ca.CertPEM, keyPEM: ca.KeyPEM}, nil
}
