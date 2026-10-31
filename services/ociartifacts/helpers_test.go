package ociartifacts_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/genericartifactscontent"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testNamespace   = "gopherstack"
	loginUser       = testNamespace + "/alice@example.com"
	loginToken      = "s3cr3t-auth-token"
)

type keyPair struct {
	privPEM string
	pubPEM  string
	fprint  string
}

func newKeyPair(t *testing.T) keyPair {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pub, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)

	fp, err := ociauth.Fingerprint(&k.PublicKey)
	require.NoError(t, err)

	return keyPair{
		privPEM: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})),
		pubPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})),
		fprint:  fp,
	}
}

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Now().UTC().Truncate(time.Second).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *ociartifacts.InMemoryBackend
	handler *ociartifacts.Handler
	clock   *fakeClock
	client  artifacts.ArtifactsClient
	content genericartifactscontent.GenericArtifactsContentClient
	base    string
}

type envOpts struct {
	dataDir  string
	enforce  bool
	register bool
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer l.Close()

	tcp, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return tcp.Port
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	be := ociartifacts.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)
	be.SetContext(t.Context())
	be.SetDataDir(opts.dataDir)

	h := ociartifacts.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, ociartifacts.Region, kp.fprint, kp.privPEM, nil)
	e := &env{backend: be, handler: h, clock: clock, base: fmt.Sprintf("http://localhost:%d", h.Port)}

	var err error

	e.client, err = artifacts.NewArtifactsClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.client.Host = e.base

	e.content, err = genericartifactscontent.NewGenericArtifactsContentClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.content.Host = e.base

	return e
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, method, url, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// registry returns a conformance client that logs in with Basic credentials.
func (e *env) registry(t *testing.T) ocitest.Client {
	t.Helper()

	return ocitest.Client{T: t, Base: e.base, Prepare: func(r *http.Request) {
		r.Header.Set("Authorization", basic(loginUser, loginToken))
	}}
}

// anonymous returns a registry client without credentials.
func (e *env) anonymous(t *testing.T) ocitest.Client {
	t.Helper()

	return ocitest.Client{T: t, Base: e.base}
}

func (e *env) createRepo(
	t *testing.T,
	name string,
	mutate func(*artifacts.CreateContainerRepositoryDetails),
) artifacts.ContainerRepository {
	t.Helper()

	d := artifacts.CreateContainerRepositoryDetails{CompartmentId: new(testCompartment), DisplayName: new(name)}
	if mutate != nil {
		mutate(&d)
	}

	resp, err := e.client.CreateContainerRepository(t.Context(),
		artifacts.CreateContainerRepositoryRequest{CreateContainerRepositoryDetails: d})
	require.NoError(t, err)

	return resp.ContainerRepository
}

func (e *env) listImages(
	t *testing.T,
	mutate func(*artifacts.ListContainerImagesRequest),
) []artifacts.ContainerImageSummary {
	t.Helper()

	r := artifacts.ListContainerImagesRequest{CompartmentId: new(testCompartment)}
	if mutate != nil {
		mutate(&r)
	}

	resp, err := e.client.ListContainerImages(t.Context(), r)
	require.NoError(t, err)

	return resp.Items
}

func (e *env) createGenericRepo(t *testing.T, immutable bool) artifacts.GenericRepository {
	t.Helper()

	resp, err := e.client.CreateRepository(t.Context(), artifacts.CreateRepositoryRequest{
		CreateRepositoryDetails: artifacts.CreateGenericRepositoryDetails{
			CompartmentId: new(testCompartment), IsImmutable: new(immutable), DisplayName: new("generic-repo"),
		}})
	require.NoError(t, err)

	repo, ok := resp.Repository.(artifacts.GenericRepository)
	require.True(t, ok, "%T", resp.Repository)

	return repo
}

func (e *env) upload(
	t *testing.T,
	repoID, path, version string,
	body []byte,
) (genericartifactscontent.GenericArtifact, error) {
	t.Helper()

	resp, err := e.content.PutGenericArtifactContentByPath(
		t.Context(),
		genericartifactscontent.PutGenericArtifactContentByPathRequest{
			RepositoryId: new(repoID), ArtifactPath: new(path), Version: new(version),
			GenericArtifactContentBody: io.NopCloser(bytes.NewReader(body)),
		},
	)
	if err != nil {
		return genericartifactscontent.GenericArtifact{}, err
	}

	return resp.GenericArtifact, nil
}

func jsonBody(t *testing.T, v any) string {
	t.Helper()

	out, err := json.Marshal(v)
	require.NoError(t, err)

	return string(out)
}
