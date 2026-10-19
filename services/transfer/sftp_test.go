package transfer_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

type fakeLambda struct{ resp string }

func (f fakeLambda) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	if !strings.Contains(string(payload), `"password":"hunter2"`) {
		return []byte(`{}`), 200, nil
	}

	return []byte(f.resp), 200, nil
}

type sftpEnv struct {
	signer   ssh.Signer
	t        *testing.T
	backend  *transfer.InMemoryBackend
	store    *s3.InMemoryBackend
	serverID string
}

func newSFTPEnv(t *testing.T, lambda transfer.LambdaInvoker, in *transfer.CreateServerInput) *sftpEnv {
	t.Helper()

	store := s3.NewInMemoryBackend(nil)
	_, err := store.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("bkt")})
	require.NoError(t, err)

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(b.Close)
	b.EnableSFTP(transfer.SFTPConfig{Store: store, Lambda: lambda})

	if in == nil {
		in = &transfer.CreateServerInput{}
	}

	in.Protocols = []string{"SFTP"}
	srv, err := b.CreateServerFull(in)
	require.NoError(t, err)

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)

	e := &sftpEnv{t: t, backend: b, store: store, serverID: srv.ServerID, signer: signer}
	require.Eventually(t, func() bool { return b.SFTPAddress(e.serverID) != "" }, 5*time.Second, 10*time.Millisecond)

	return e
}

func (e *sftpEnv) addUser(name string, in transfer.CreateUserInput) {
	e.t.Helper()

	in.ServerID, in.UserName, in.Role = e.serverID, name, "arn:aws:iam::000000000000:role/r"
	_, err := e.backend.CreateUserFull(&in)
	require.NoError(e.t, err)
	_, err = e.backend.ImportSSHPublicKey(
		e.serverID,
		name,
		strings.TrimSpace(string(ssh.MarshalAuthorizedKey(e.signer.PublicKey()))),
	)
	require.NoError(e.t, err)
}

func (e *sftpEnv) dial(user string, auth ssh.AuthMethod) (*sftp.Client, func(), error) {
	e.t.Helper()

	hostKey, err := e.backend.SFTPHostPublicKey(e.serverID)
	require.NoError(e.t, err)
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(hostKey))
	require.NoError(e.t, err)

	c, err := ssh.Dial("tcp", e.backend.SFTPAddress(e.serverID), &ssh.ClientConfig{
		User: user, Auth: []ssh.AuthMethod{auth}, HostKeyCallback: ssh.FixedHostKey(pub), Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, nil, err
	}

	cl, err := sftp.NewClient(c)
	if err != nil {
		_ = c.Close()

		return nil, nil, err
	}

	return cl, func() { _ = cl.Close(); _ = c.Close() }, nil
}

func (e *sftpEnv) object(key string) (string, bool) {
	out, err := e.store.GetObject(e.t.Context(), &awss3.GetObjectInput{Bucket: aws.String("bkt"), Key: aws.String(key)})
	if err != nil {
		return "", false
	}
	defer out.Body.Close()

	b, _ := io.ReadAll(out.Body)

	return string(b), true
}

func TestSFTPFileRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prefix  string
		workdir string
		user    transfer.CreateUserInput
	}{
		{name: "path home", user: transfer.CreateUserInput{HomeDir: "/bkt/home"}, prefix: "home/", workdir: "."},
		{
			name: "logical chroot",
			user: transfer.CreateUserInput{
				HomeDirectoryType:     "LOGICAL",
				HomeDirectoryMappings: []transfer.HomeDirectoryMapEntry{{Entry: "/", Target: "/bkt/chroot"}},
			},
			prefix: "chroot/", workdir: ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSFTPEnv(t, nil, nil)
			e.addUser("alice", tt.user)

			cl, closeFn, err := e.dial("alice", ssh.PublicKeys(e.signer))
			require.NoError(t, err)
			defer closeFn()

			require.NoError(t, cl.Mkdir("sub"))

			f, err := cl.Create("sub/a.txt")
			require.NoError(t, err)
			_, err = f.Write([]byte("hello sftp"))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			body, ok := e.object(tt.prefix + "sub/a.txt")
			require.True(t, ok)
			assert.Equal(t, "hello sftp", body)

			infos, err := cl.ReadDir("sub")
			require.NoError(t, err)
			require.Len(t, infos, 1)
			assert.Equal(t, "a.txt", infos[0].Name())
			assert.Equal(t, int64(10), infos[0].Size())

			st, err := cl.Stat("sub")
			require.NoError(t, err)
			assert.True(t, st.IsDir())

			rf, err := cl.Open("sub/a.txt")
			require.NoError(t, err)
			got, err := io.ReadAll(rf)
			require.NoError(t, err)
			require.NoError(t, rf.Close())
			assert.Equal(t, "hello sftp", string(got))

			require.NoError(t, cl.Rename("sub/a.txt", "sub/b.txt"))
			_, ok = e.object(tt.prefix + "sub/a.txt")
			assert.False(t, ok)
			_, ok = e.object(tt.prefix + "sub/b.txt")
			assert.True(t, ok)

			require.Error(t, cl.RemoveDirectory("sub"), "non-empty rmdir must fail")
			require.NoError(t, cl.Remove("sub/b.txt"))
			require.NoError(t, cl.RemoveDirectory("sub"))

			_, err = cl.Stat("sub")
			require.Error(t, err)
		})
	}
}

func TestSFTPLogicalMappings(t *testing.T) {
	t.Parallel()

	e := newSFTPEnv(t, nil, nil)
	e.addUser("bob", transfer.CreateUserInput{
		HomeDirectoryType:     "LOGICAL",
		HomeDirectoryMappings: []transfer.HomeDirectoryMapEntry{{Entry: "/data", Target: "/bkt/pre"}},
	})

	cl, closeFn, err := e.dial("bob", ssh.PublicKeys(e.signer))
	require.NoError(t, err)
	defer closeFn()

	infos, err := cl.ReadDir("/")
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Equal(t, "data", infos[0].Name())

	f, err := cl.Create("/data/x.bin")
	require.NoError(t, err)
	_, err = f.Write(bytes.Repeat([]byte("z"), 4096))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	body, ok := e.object("pre/x.bin")
	require.True(t, ok)
	assert.Len(t, body, 4096)

	_, err = cl.Create("/outside.txt")
	require.Error(t, err)
}

func TestSFTPAuthRejections(t *testing.T) {
	t.Parallel()

	e := newSFTPEnv(t, nil, nil)
	e.addUser("carol", transfer.CreateUserInput{HomeDir: "/bkt"})

	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	other, err := ssh.NewSignerFromKey(otherPriv)
	require.NoError(t, err)

	tests := []struct {
		auth ssh.AuthMethod
		name string
		user string
	}{
		{name: "wrong key", user: "carol", auth: ssh.PublicKeys(other)},
		{name: "unknown user", user: "nobody", auth: ssh.PublicKeys(e.signer)},
		{name: "password on managed", user: "carol", auth: ssh.Password("x")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, closeFn, derr := e.dial(tt.user, tt.auth)
			if closeFn != nil {
				closeFn()
			}

			require.Error(t, derr)
		})
	}
}

func TestSFTPLambdaPasswordAuth(t *testing.T) {
	t.Parallel()

	resp := `{"Role":"arn:aws:iam::000000000000:role/r","HomeDirectory":"/bkt/lam"}`
	e := newSFTPEnv(t, fakeLambda{resp: resp}, &transfer.CreateServerInput{
		IdentityProviderType: "AWS_LAMBDA",
		IdentityProviderDetails: &transfer.IdentityProviderDetails{
			Function: "arn:aws:lambda:us-east-1:000000000000:function:auth",
		},
	})

	tests := []struct {
		name    string
		pw      string
		wantErr bool
	}{
		{name: "good password", pw: "hunter2"},
		{name: "bad password", pw: "nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cl, closeFn, err := e.dial("dave", ssh.Password(tt.pw))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			defer closeFn()

			f, err := cl.Create("up.txt")
			require.NoError(t, err)
			_, err = f.Write([]byte("lambda"))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			body, ok := e.object("lam/up.txt")
			require.True(t, ok)
			assert.Equal(t, "lambda", body)
		})
	}
}

func TestSFTPListenerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(*sftpEnv) error
		name string
	}{
		{name: "stop server", stop: func(e *sftpEnv) error { return e.backend.StopServer(e.serverID) }},
		{name: "delete server", stop: func(e *sftpEnv) error { return e.backend.DeleteServer(e.serverID) }},
		{name: "reset", stop: func(e *sftpEnv) error {
			e.backend.Reset()

			return nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSFTPEnv(t, nil, nil)
			addr := e.backend.SFTPAddress(e.serverID)

			conn, err := net.DialTimeout("tcp", addr, time.Second)
			require.NoError(t, err)
			_ = conn.Close()

			require.NoError(t, tt.stop(e))
			assert.Empty(t, e.backend.SFTPAddress(e.serverID))

			_, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
			require.Error(t, err)
		})
	}
}

func TestSFTPRestartRebinds(t *testing.T) {
	t.Parallel()

	e := newSFTPEnv(t, nil, nil)
	hostKey, err := e.backend.SFTPHostPublicKey(e.serverID)
	require.NoError(t, err)

	require.NoError(t, e.backend.StopServer(e.serverID))
	require.NoError(t, e.backend.StartServer(e.serverID))
	require.Eventually(
		t,
		func() bool { return e.backend.SFTPAddress(e.serverID) != "" },
		5*time.Second,
		10*time.Millisecond,
	)

	again, err := e.backend.SFTPHostPublicKey(e.serverID)
	require.NoError(t, err)
	assert.Equal(t, hostKey, again, "generated host key must persist across restarts")
}
