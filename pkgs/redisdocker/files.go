package redisdocker

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
)

const (
	subdirMode = 0o755
	fileMode   = 0o644
)

// writeFiles lays out the read-only directory every node mounts: shard seeds and, for TLS, the node certificate.
func (g *Group) writeFiles() error {
	for i, sh := range g.spec.Shards {
		if len(sh.Seed) == 0 {
			continue
		}

		if err := os.WriteFile(filepath.Join(g.dir, seedName(i)), sh.Seed, fileMode); err != nil {
			return fmt.Errorf("write seed: %w", err)
		}
	}

	if !g.spec.TLS {
		return nil
	}

	return g.writeTLS()
}

func (g *Group) writeTLS() error {
	ca := g.engine.cfg.CA
	hosts := append([]string{g.engine.cfg.Host, "localhost", defaultHost}, g.spec.TLSNames...)

	cert, err := ca.IssueServerClient(hosts...)
	if err != nil {
		return fmt.Errorf("issue redis certificate: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return fmt.Errorf("marshal redis key: %w", err)
	}

	var certPEM []byte
	for _, der := range cert.Certificate {
		certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}

	dir := filepath.Join(g.dir, tlsDir)
	if err = os.Mkdir(dir, subdirMode); err != nil {
		return fmt.Errorf("tls directory: %w", err)
	}

	files := map[string][]byte{
		certFile: certPEM,
		keyFile:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		caFile:   ca.CertPEM(),
	}

	for name, data := range files {
		if err = os.WriteFile(filepath.Join(dir, name), data, fileMode); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}

	return nil
}
