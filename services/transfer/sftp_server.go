package transfer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var errSFTPDenied = errors.New("transfer: sftp authentication failed")

// LambdaInvoker is the Lambda surface a custom AWS_LAMBDA identity provider needs.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// sftpSession is the resolved identity of an authenticated SFTP login.
type sftpSession struct {
	Home     string                  `json:"home"`
	Mappings []HomeDirectoryMapEntry `json:"mappings,omitempty"`
	Logical  bool                    `json:"logical"`
}

type sftpServer struct {
	backend  *InMemoryBackend
	cfg      *SFTPConfig
	ctx      context.Context
	ssh      *ssh.ServerConfig
	serverID string
}

func (s *sftpServer) sshConfig(signers []ssh.Signer) *ssh.ServerConfig {
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			return s.authenticate(c, key, "")
		},
		PasswordCallback: func(c ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
			return s.authenticate(c, nil, string(pw))
		},
	}

	if banner := s.backend.sftpPreBanner(s.serverID); banner != "" {
		cfg.BannerCallback = func(ssh.ConnMetadata) string { return banner }
	}

	for _, sg := range signers {
		cfg.AddHostKey(sg)
	}

	return cfg
}

func (b *InMemoryBackend) sftpPreBanner(serverID string) string {
	b.mu.RLock("sftpPreBanner")
	defer b.mu.RUnlock()

	if s, ok := b.servers.Get(serverID); ok {
		return s.PreAuthenticationLoginBanner
	}

	return ""
}

func (s *sftpServer) accept(l *sftpListener) {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return
		}

		l.wg.Go(func() { s.serve(conn) })
	}
}

func (s *sftpServer) serve(conn net.Conn) {
	defer conn.Close()

	stop := context.AfterFunc(s.ctx, func() { _ = conn.Close() })
	defer stop()

	_ = conn.SetDeadline(time.Now().Add(sftpHandshakeTimeout))

	sc, chans, reqs, err := ssh.NewServerConn(conn, s.ssh)
	if err != nil {
		return
	}

	_ = conn.SetDeadline(time.Time{})

	defer sc.Close()

	go ssh.DiscardRequests(reqs)

	var sess sftpSession
	if raw := sc.Permissions.Extensions[sftpSessionExtension]; raw != "" {
		if jerr := json.Unmarshal([]byte(raw), &sess); jerr != nil {
			return
		}
	}

	for nc := range chans {
		if nc.ChannelType() != sftpChannelSession {
			_ = nc.Reject(ssh.UnknownChannelType, "only session channels are supported")

			continue
		}

		ch, creqs, aerr := nc.Accept()
		if aerr != nil {
			return
		}

		s.runChannel(ch, creqs, &sess)
	}
}

// runChannel serves one session channel; only the sftp subsystem is accepted.
func (s *sftpServer) runChannel(ch ssh.Channel, reqs <-chan *ssh.Request, sess *sftpSession) {
	defer ch.Close()

	for req := range reqs {
		if req.Type != "subsystem" || !bytes.HasSuffix(req.Payload, []byte(sftpSubsystem)) {
			_ = req.Reply(false, nil)

			continue
		}

		_ = req.Reply(true, nil)

		fs := &sftpFS{ctx: s.ctx, store: s.cfg.Store, sess: sess, setStat: s.backend.sftpSetStatNoOp(s.serverID)}
		rs := sftp.NewRequestServer(ch, sftp.Handlers{
			FileGet: fs, FilePut: fs, FileCmd: fs, FileList: fs,
		}, sftp.WithStartDirectory(fs.startDir()))

		if err := rs.Serve(); err != nil && !errors.Is(err, io.EOF) {
			logger.Load(s.ctx).Debug("transfer: sftp session ended", "error", err)
		}

		_ = rs.Close()

		return
	}
}

func (b *InMemoryBackend) sftpSetStatNoOp(serverID string) bool {
	b.mu.RLock("sftpSetStat")
	defer b.mu.RUnlock()

	s, ok := b.servers.Get(serverID)

	return ok && s.ProtocolDetails != nil && s.ProtocolDetails.SetStatOption == "ENABLE_NO_OP"
}

func (s *sftpServer) authenticate(c ssh.ConnMetadata, key ssh.PublicKey, password string) (*ssh.Permissions, error) {
	sess, err := s.resolveLogin(c, key, password)
	if err != nil {
		return nil, errSFTPDenied
	}

	raw, err := json.Marshal(sess)
	if err != nil {
		return nil, errSFTPDenied
	}

	return &ssh.Permissions{Extensions: map[string]string{sftpSessionExtension: string(raw)}}, nil
}

func (s *sftpServer) resolveLogin(c ssh.ConnMetadata, key ssh.PublicKey, password string) (*sftpSession, error) {
	srv, err := s.backend.DescribeServer(s.serverID)
	if err != nil {
		return nil, err
	}

	methods := ""
	if srv.IdentityProviderDetails != nil {
		methods = srv.IdentityProviderDetails.SftpAuthenticationMethods
	}

	if (key == nil && methods == sftpAuthPublicKeyOnly) || (key != nil && methods == sftpAuthPwdOnly) {
		return nil, errSFTPDenied
	}

	switch srv.IdentityProviderType {
	case identityProviderLambda:
		return s.lambdaLogin(srv, c, key, password)
	case identityProviderServiceManaged, "":
		if key == nil {
			return nil, errSFTPDenied
		}

		return s.managedLogin(c.User(), key)
	default:
		return nil, errSFTPDenied
	}
}

func (s *sftpServer) managedLogin(userName string, key ssh.PublicKey) (*sftpSession, error) {
	u, err := s.backend.DescribeUser(s.serverID, userName)
	if err != nil {
		return nil, err
	}

	for _, k := range s.backend.ListSSHPublicKeys(s.serverID, userName) {
		if keyMatches(k.SSHPublicKeyBody, key) {
			return sessionFor(u.HomeDir, u.HomeDirectoryType, u.HomeDirectoryMappings), nil
		}
	}

	return nil, errSFTPDenied
}

func keyMatches(authorized string, key ssh.PublicKey) bool {
	pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(authorized))

	return err == nil && bytes.Equal(pk.Marshal(), key.Marshal())
}

func sessionFor(home, homeType string, mappings []HomeDirectoryMapEntry) *sftpSession {
	if homeType == homeDirectoryTypeLogical {
		return &sftpSession{Logical: true, Home: "/", Mappings: mappings}
	}

	if home == "" {
		home = "/"
	}

	return &sftpSession{Home: home}
}

type lambdaAuthResponse struct {
	Role                 string   `json:"Role"`
	HomeDirectory        string   `json:"HomeDirectory"`
	HomeDirectoryType    string   `json:"HomeDirectoryType"`
	HomeDirectoryDetails string   `json:"HomeDirectoryDetails"`
	PublicKeys           []string `json:"PublicKeys"`
}

func (s *sftpServer) lambdaLogin(
	srv *Server, c ssh.ConnMetadata, key ssh.PublicKey, password string,
) (*sftpSession, error) {
	if s.cfg.Lambda == nil || srv.IdentityProviderDetails == nil || srv.IdentityProviderDetails.Function == "" {
		return nil, errSFTPDenied
	}

	fn := srv.IdentityProviderDetails.Function
	if i := strings.LastIndex(fn, ":"); i >= 0 {
		fn = fn[i+1:]
	}

	event := map[string]string{
		"username": c.User(), "protocol": protocolSFTP,
		"serverId": s.serverID, "sourceIp": c.RemoteAddr().String(),
	}
	if password != "" {
		event["password"] = password
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	out, _, err := s.cfg.Lambda.InvokeFunction(s.ctx, fn, "RequestResponse", payload)
	if err != nil {
		return nil, err
	}

	var resp lambdaAuthResponse
	if jerr := json.Unmarshal(out, &resp); jerr != nil || resp.Role == "" {
		return nil, errSFTPDenied
	}

	if key != nil && !slices.ContainsFunc(resp.PublicKeys, func(pk string) bool { return keyMatches(pk, key) }) {
		return nil, errSFTPDenied
	}

	mappings, err := parseLambdaMappings(resp.HomeDirectoryDetails)
	if err != nil {
		return nil, err
	}

	if resp.HomeDirectoryType == "" && len(mappings) > 0 {
		resp.HomeDirectoryType = homeDirectoryTypeLogical
	}

	return sessionFor(resp.HomeDirectory, resp.HomeDirectoryType, mappings), nil
}

func parseLambdaMappings(details string) ([]HomeDirectoryMapEntry, error) {
	if details == "" {
		return nil, nil
	}

	var raw []struct {
		Entry  string `json:"Entry"`
		Target string `json:"Target"`
	}
	if err := json.Unmarshal([]byte(details), &raw); err != nil {
		return nil, errSFTPDenied
	}

	out := make([]HomeDirectoryMapEntry, 0, len(raw))
	for _, m := range raw {
		out = append(out, HomeDirectoryMapEntry{Entry: m.Entry, Target: m.Target})
	}

	return out, nil
}
