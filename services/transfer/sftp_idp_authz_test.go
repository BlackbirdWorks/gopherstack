package transfer_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/services/efs"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

type fakeGateway struct {
	gotURL  string
	gotPass string
	body    string
	status  int
	mu      sync.Mutex
}

func (f *fakeGateway) Get(_ context.Context, rawURL string, h map[string]string) (int, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.gotURL, f.gotPass = rawURL, h["Password"]
	if h["Password"] != "hunter2" {
		return http.StatusOK, []byte(`{}`), nil
	}

	return f.status, []byte(f.body), nil
}

func gatewayServer(url string) *transfer.CreateServerInput {
	return &transfer.CreateServerInput{
		IdentityProviderType: "API_GATEWAY",
		IdentityProviderDetails: &transfer.IdentityProviderDetails{
			URL: url, InvocationRole: "arn:aws:iam::000000000000:role/inv",
		},
	}
}

func TestSFTPAPIGatewayIdentityProvider(t *testing.T) {
	t.Parallel()

	const okBody = `{"Role":"arn:aws:iam::000000000000:role/r","HomeDirectory":"/bkt/gw"}`

	tests := []struct {
		name     string
		body     string
		password string
		status   int
		wantOK   bool
	}{
		{name: "valid", body: okBody, password: "hunter2", status: 200, wantOK: true},
		{name: "wrong password", body: okBody, password: "nope", status: 200},
		{name: "non 200", body: okBody, password: "hunter2", status: 403},
		{name: "no role", body: `{"HomeDirectory":"/bkt"}`, password: "hunter2", status: 200},
		{name: "bad json", body: `not json`, password: "hunter2", status: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &fakeGateway{body: tt.body, status: tt.status}
			e := newSFTPEnvCfg(
				t,
				transfer.SFTPConfig{APIGateway: gw},
				gatewayServer("https://abc.execute-api.us-east-1.amazonaws.com/prod"),
			)

			cl, closeFn, err := e.dial("bob", ssh.Password(tt.password))
			if !tt.wantOK {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			defer closeFn()

			f, err := cl.Create("gw.txt")
			require.NoError(t, err)
			_, err = f.Write([]byte("x"))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			_, ok := e.object("gw/gw.txt")
			assert.True(t, ok)

			gw.mu.Lock()
			defer gw.mu.Unlock()
			assert.Contains(t, gw.gotURL, "/prod/servers/"+e.serverID+"/users/bob/config")
			assert.Contains(t, gw.gotURL, "protocol=SFTP")
			assert.Contains(t, gw.gotURL, "sourceIp=127.0.0.1")
		})
	}
}

func TestSFTPAPIGatewayOverHTTP(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Password") != "hunter2" || !strings.HasSuffix(r.URL.Path, "/users/carol/config") {
			_, _ = io.WriteString(w, `{}`)

			return
		}

		_, _ = io.WriteString(w, `{"Role":"arn:aws:iam::000000000000:role/r","HomeDirectory":"/bkt/http"}`)
	}))
	t.Cleanup(ts.Close)

	e := newSFTPEnvCfg(
		t,
		transfer.SFTPConfig{APIGateway: transfer.NewHTTPAPIGatewayClient()},
		gatewayServer(ts.URL+"/prod"),
	)

	_, closeFn, err := e.dial("carol", ssh.Password("hunter2"))
	require.NoError(t, err)
	closeFn()

	_, _, err = e.dial("carol", ssh.Password("bad"))
	require.Error(t, err)
}

func TestSFTPDirectoryServiceRefused(t *testing.T) {
	t.Parallel()

	e := newSFTPEnv(t, nil, &transfer.CreateServerInput{
		IdentityProviderType:    "AWS_DIRECTORY_SERVICE",
		IdentityProviderDetails: &transfer.IdentityProviderDetails{DirectoryID: "d-1234567890"},
	})

	_, _, err := e.dial("dave", ssh.Password("pw"))
	require.Error(t, err)
}

const (
	policyReadOnly = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":["s3:GetObject","s3:ListBucket"],"Resource":"*"}]}`
	policyAll     = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	policyDenyDel = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}]}`
	policyScoped = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*",` +
		`"Resource":["arn:aws:s3:::${transfer:HomeBucket}","arn:aws:s3:::${transfer:HomeBucket}/${transfer:HomeFolder}/*"]}]}`
)

func TestSFTPRolePolicyEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		allowed map[string]bool
		name    string
		rolePol string
		session string
	}{
		{
			name: "read only", rolePol: policyReadOnly,
			allowed: map[string]bool{"read": true, "list": true, "write": false, "mkdir": false, "delete": false},
		},
		{
			name: "full", rolePol: policyAll,
			allowed: map[string]bool{"read": true, "list": true, "write": true, "mkdir": true, "delete": true},
		},
		{
			name: "explicit deny delete", rolePol: policyDenyDel,
			allowed: map[string]bool{"read": true, "list": true, "write": true, "mkdir": true, "delete": false},
		},
		{
			name: "session policy narrows", rolePol: policyAll, session: policyReadOnly,
			allowed: map[string]bool{"read": true, "list": true, "write": false, "mkdir": false, "delete": false},
		},
		{
			name: "session policy variables", rolePol: policyAll, session: policyScoped,
			allowed: map[string]bool{"read": true, "list": true, "write": true, "mkdir": true, "delete": true},
		},
		{
			name: "no policy implicit deny", rolePol: "",
			allowed: map[string]bool{"read": false, "list": false, "write": false, "mkdir": false, "delete": false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ib := iam.NewInMemoryBackend()
			_, err := ib.CreateRole("r", "/", "", "")
			require.NoError(t, err)

			if tt.rolePol != "" {
				require.NoError(t, ib.PutRolePolicy("r", "p", tt.rolePol))
			}

			e := newSFTPEnvCfg(t, transfer.SFTPConfig{Policies: ib}, nil)
			e.addUser("alice", transfer.CreateUserInput{HomeDir: "/bkt/home", Policy: tt.session})

			_, err = e.store.PutObject(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String("bkt"), Key: aws.String("home/seed.txt"), Body: strings.NewReader("seed"),
			})
			require.NoError(t, err)

			cl, closeFn, err := e.dial("alice", ssh.PublicKeys(e.signer))
			require.NoError(t, err)
			defer closeFn()

			ops := map[string]func() error{
				"read": func() error {
					f, oerr := cl.Open("seed.txt")
					if oerr != nil {
						return oerr
					}
					defer f.Close()
					_, rerr := io.ReadAll(f)

					return rerr
				},
				"list": func() error {
					_, lerr := cl.ReadDir(".")

					return lerr
				},
				"write": func() error {
					f, cerr := cl.Create("new.txt")
					if cerr != nil {
						return cerr
					}

					_, werr := f.Write([]byte("n"))
					cerr = f.Close()
					if werr != nil {
						return werr
					}

					return cerr
				},
				"mkdir":  func() error { return cl.Mkdir("d") },
				"delete": func() error { return cl.Remove("seed.txt") },
			}

			for _, op := range []string{"read", "list", "write", "mkdir", "delete"} {
				err = ops[op]()
				if tt.allowed[op] {
					assert.NoError(t, err, op)
				} else {
					require.Error(t, err, op)
					assert.Contains(t, err.Error(), "permission denied", op)
				}
			}
		})
	}
}

func TestSFTPRolePolicyUnknownRoleUnrestricted(t *testing.T) {
	t.Parallel()

	e := newSFTPEnvCfg(t, transfer.SFTPConfig{Policies: iam.NewInMemoryBackend()}, nil)
	e.addUser("alice", transfer.CreateUserInput{HomeDir: "/bkt/home"})

	cl, closeFn, err := e.dial("alice", ssh.PublicKeys(e.signer))
	require.NoError(t, err)
	defer closeFn()

	require.NoError(t, cl.Mkdir("ok"))
}

func TestSFTPEFSDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rolePol string
		wantErr bool
	}{
		{
			name:    "allowed",
			rolePol: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"elasticfilesystem:*","Resource":"*"}]}`,
		},
		{
			name: "mount only", wantErr: true,
			rolePol: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Action":"elasticfilesystem:ClientMount","Resource":"*"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eb := efs.NewInMemoryBackend("000000000000", "us-east-1")
			eb.SetStorageRoot(t.TempDir())

			fs, err := eb.CreateFileSystem(t.Context(), efs.CreateFileSystemRequest{CreationToken: "tok-" + tt.name})
			require.NoError(t, err)

			ib := iam.NewInMemoryBackend()
			_, err = ib.CreateRole("r", "/", "", "")
			require.NoError(t, err)
			require.NoError(t, ib.PutRolePolicy("r", "p", tt.rolePol))

			e := newSFTPEnvCfg(
				t,
				transfer.SFTPConfig{EFS: eb, Policies: ib},
				&transfer.CreateServerInput{Domain: "EFS"},
			)
			e.addUser("erin", transfer.CreateUserInput{HomeDir: "/" + fs.FileSystemID + "/home"})

			cl, closeFn, err := e.dial("erin", ssh.PublicKeys(e.signer))
			require.NoError(t, err)
			defer closeFn()

			f, err := cl.Create("a.txt")
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "permission denied")

				return
			}

			require.NoError(t, err)
			_, err = f.Write([]byte("efs data"))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			dir, err := eb.FileSystemHostDir("us-east-1", fs.FileSystemID)
			require.NoError(t, err)
			raw, err := readFile(dir + "/home/a.txt")
			require.NoError(t, err)
			assert.Equal(t, "efs data", raw)

			require.NoError(t, cl.Mkdir("sub"))
			require.NoError(t, cl.Rename("a.txt", "sub/b.txt"))

			infos, err := cl.ReadDir("sub")
			require.NoError(t, err)
			require.Len(t, infos, 1)
			assert.Equal(t, "b.txt", infos[0].Name())

			require.NoError(t, cl.Remove("sub/b.txt"))
			require.NoError(t, cl.RemoveDirectory("sub"))
		})
	}
}

func readFile(p string) (string, error) {
	b, err := os.ReadFile(p)

	return string(b), err
}

func TestSFTPAPIGatewayRejectsForeignHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{"metadata ip", "http://169.254.169.254/latest"},
		{"public host", "https://example.com/prod"},
		{"lookalike execute api", "https://abc.execute-api.us-east-1.amazonaws.com.evil.test/prod"},
		{"non http scheme", "ftp://localhost/prod"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gw := &fakeGateway{body: `{"Role":"arn:aws:iam::000000000000:role/r"}`, status: 200}
			e := newSFTPEnvCfg(t, transfer.SFTPConfig{APIGateway: gw}, gatewayServer(tt.url))

			_, _, err := e.dial("bob", ssh.Password("hunter2"))
			require.Error(t, err)

			gw.mu.Lock()
			defer gw.mu.Unlock()
			assert.Empty(t, gw.gotURL)
		})
	}
}

func TestHTTPAPIGatewayClientRefusesNonLoopback(t *testing.T) {
	t.Parallel()

	c := transfer.NewHTTPAPIGatewayClient()

	urls := []string{
		"http://169.254.169.254/",
		"https://abc.execute-api.us-east-1.amazonaws.com/p",
		"http://localhost:99999/",
	}

	for _, u := range urls {
		_, _, err := c.Get(t.Context(), u, nil)
		require.Error(t, err, u)
	}
}
