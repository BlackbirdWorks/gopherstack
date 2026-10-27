package transfer

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	sftpDefaultHost       = "127.0.0.1"
	sftpHandshakeTimeout  = 30 * time.Second
	sftpSessionExtension  = "gopherstack-sftp-session"
	sftpSubsystem         = "sftp"
	sftpChannelSession    = "session"
	sftpAuthPublicKeyOnly = "PUBLIC_KEY"
	sftpAuthPwdOnly       = "PASSWORD"
	sftpMaxHostKeyTypes   = 3
)

// SFTPConfig turns on real SFTP listeners for ONLINE SFTP servers.
type SFTPConfig struct {
	// Store is the S3 backend file operations map onto.
	Store ObjectStore
	// Lambda backs AWS_LAMBDA identity providers; nil rejects those logins.
	Lambda LambdaInvoker
	// APIGateway backs API_GATEWAY identity providers; nil rejects those logins.
	APIGateway APIGatewayClient
	// Policies supplies IAM role policies; nil (or an unknown role) disables role enforcement.
	Policies RolePolicySource
	// EFS backs Domain=EFS servers; nil rejects their logins.
	EFS EFSStorage
	// Host is the interface listeners bind to; default 127.0.0.1.
	Host string
}

type sftpListener struct {
	ln     net.Listener
	cancel context.CancelFunc
	addr   string
	wg     sync.WaitGroup
}

func (l *sftpListener) stop() {
	l.cancel()
	_ = l.ln.Close()
	l.wg.Wait()
}

// EnableSFTP makes every ONLINE server with the SFTP protocol bind a real SSH listener.
func (b *InMemoryBackend) EnableSFTP(cfg SFTPConfig) {
	if cfg.Host == "" {
		cfg.Host = sftpDefaultHost
	}

	b.sftpMu.Lock("EnableSFTP")
	b.sftp = &cfg
	b.sftpMu.Unlock()

	for _, s := range b.ListServers() {
		b.reconcileSFTP(s.ServerID)
	}
}

// SFTPAddress returns the host:port the server's SFTP listener is bound to, or "" when not listening.
func (b *InMemoryBackend) SFTPAddress(serverID string) string {
	b.sftpMu.RLock("SFTPAddress")
	defer b.sftpMu.RUnlock()

	if l := b.sftpListeners[serverID]; l != nil {
		return l.addr
	}

	return ""
}

// closeSFTP stops every listener and bars new ones; in-flight timer callbacks can still call reconcileSFTP.
func (b *InMemoryBackend) closeSFTP() {
	b.sftpMu.Lock("closeSFTP")
	b.sftpClosed = true
	b.sftpMu.Unlock()

	b.stopAllSFTP()
}

func (b *InMemoryBackend) stopAllSFTP() {
	b.sftpMu.Lock("stopAllSFTP")
	defer b.sftpMu.Unlock()

	for id, l := range b.sftpListeners {
		l.stop()
		delete(b.sftpListeners, id)
	}
}

func (b *InMemoryBackend) sftpWanted(serverID string) bool {
	b.mu.RLock("sftpWanted")
	defer b.mu.RUnlock()

	s, ok := b.servers.Get(serverID)

	return ok && s.State == serverStatusOnline && slices.Contains(s.Protocols, protocolSFTP)
}

// reconcileSFTP starts or stops the server's listener to match its state. Never call with b.mu held.
func (b *InMemoryBackend) reconcileSFTP(serverID string) {
	b.sftpMu.Lock("reconcileSFTP")
	defer b.sftpMu.Unlock()

	if b.sftp == nil || b.sftpClosed {
		return
	}

	cur := b.sftpListeners[serverID]
	want := b.sftpWanted(serverID)

	switch {
	case want && cur == nil:
		l, err := b.startSFTPListener(serverID, b.sftp)
		if err != nil {
			logger.Load(context.Background()).Warn("transfer: sftp listener failed", "server", serverID, "error", err)

			return
		}

		b.sftpListeners[serverID] = l
	case !want && cur != nil:
		cur.stop()
		delete(b.sftpListeners, serverID)
	}
}

func (b *InMemoryBackend) startSFTPListener(serverID string, cfg *SFTPConfig) (*sftpListener, error) {
	signers, err := b.sftpHostSigners(serverID)
	if err != nil {
		return nil, err
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(cfg.Host, "0"))
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &sftpListener{ln: ln, cancel: cancel, addr: ln.Addr().String()}

	srv := &sftpServer{backend: b, cfg: cfg, serverID: serverID, ctx: ctx}
	srv.ssh = srv.sshConfig(signers)

	l.wg.Go(func() { srv.accept(l) })

	logger.Load(ctx).Info("transfer: sftp listener started", "server", serverID, "addr", l.addr)

	return l, nil
}

// sftpHostSigners returns signers for the server's imported host keys, falling back to a generated persisted key.
func (b *InMemoryBackend) sftpHostSigners(serverID string) ([]ssh.Signer, error) {
	b.mu.RLock("sftpHostSigners")
	s, ok := b.servers.Get(serverID)

	if !ok {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	bodies := make([]string, 0, sftpMaxHostKeyTypes)
	if s.HostKey != "" {
		bodies = append(bodies, s.HostKey)
	}

	for _, hk := range b.hostKeysByServer.Get(serverID) {
		bodies = append(bodies, hk.Value)
	}

	generated := s.SFTPHostKey
	b.mu.RUnlock()

	var signers []ssh.Signer

	seen := map[string]bool{}

	for _, body := range bodies {
		sg, err := ssh.ParsePrivateKey([]byte(body))
		if err != nil || seen[sg.PublicKey().Type()] {
			continue
		}

		seen[sg.PublicKey().Type()] = true
		signers = append(signers, sg)
	}

	if len(signers) > 0 {
		return signers, nil
	}

	if generated == "" {
		var err error
		if generated, err = b.persistGeneratedHostKey(serverID); err != nil {
			return nil, err
		}
	}

	sg, err := ssh.ParsePrivateKey([]byte(generated))
	if err != nil {
		return nil, fmt.Errorf("parse generated host key: %w", err)
	}

	return []ssh.Signer{sg}, nil
}

func (b *InMemoryBackend) persistGeneratedHostKey(serverID string) (string, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate host key: %w", err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("marshal host key: %w", err)
	}

	pemBody := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))

	b.mu.Lock("persistGeneratedHostKey")
	defer b.mu.Unlock()

	s, ok := b.servers.Get(serverID)
	if !ok {
		return "", fmt.Errorf("%w: server %s not found", ErrServerNotFound, serverID)
	}

	if s.SFTPHostKey == "" {
		s.SFTPHostKey = pemBody
	}

	return s.SFTPHostKey, nil
}

// SFTPHostPublicKey returns the authorized_keys-format host public key the server presents (first signer).
func (b *InMemoryBackend) SFTPHostPublicKey(serverID string) (string, error) {
	signers, err := b.sftpHostSigners(serverID)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signers[0].PublicKey()))), nil
}
