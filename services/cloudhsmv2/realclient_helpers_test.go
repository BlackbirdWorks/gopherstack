package cloudhsmv2_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	hsm "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
)

type fakeNet struct {
	subnets map[string]hsm.Subnet
	enis    map[string]string
	groups  map[string]string
	mu      sync.Mutex
	seq     int
}

func newFakeNet() *fakeNet {
	return &fakeNet{
		subnets: map[string]hsm.Subnet{
			"subnet-a": {VpcID: "vpc-1", Zone: "us-east-1a"},
			"subnet-b": {VpcID: "vpc-1", Zone: "us-east-1b"},
			"subnet-c": {VpcID: "vpc-2", Zone: "us-east-1c"},
			"subnet-d": {VpcID: "vpc-1", Zone: "us-east-1a"},
		},
		enis:   map[string]string{},
		groups: map[string]string{},
	}
}

func (f *fakeNet) SubnetInfo(_ string, ids []string) map[string]hsm.Subnet {
	out := map[string]hsm.Subnet{}

	for _, id := range ids {
		if sn, ok := f.subnets[id]; ok {
			out[id] = sn
		}
	}

	return out
}

func (f *fakeNet) CreateSecurityGroup(_, name, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seq++
	id := fmt.Sprintf("sg-%04d", f.seq)
	f.groups[id] = name

	return id, nil
}

func (f *fakeNet) DeleteSecurityGroup(_, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.groups, id)
}

func (f *fakeNet) CreateENI(_, subnetID, ip, _, _ string) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seq++
	if ip == "" {
		ip = fmt.Sprintf("10.0.0.%d", f.seq)
	}

	id := fmt.Sprintf("eni-%04d", f.seq)
	f.enis[id] = subnetID

	return id, ip, nil
}

func (f *fakeNet) DeleteENI(_, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.enis, id)
}

func (f *fakeNet) eniCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.enis)
}

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type fixture struct {
	client  *sdk.Client
	backend *hsm.InMemoryBackend
	clock   *fakeClock
	net     *fakeNet
	t       *testing.T
	url     string
}

func (f *fixture) ctx() context.Context { return f.t.Context() }

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureFromBackend(t, hsm.NewInMemoryBackend(testAccount, testRegion))
}

func newFixtureFromBackend(t *testing.T, backend *hsm.InMemoryBackend) *fixture {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	fn := newFakeNet()

	backend.SetClock(clock.now)
	backend.SetNetwork(fn)

	url := sdktest.Serve(t, hsm.NewHandler(backend))

	return &fixture{client: clientFor(t, url, testAccount), backend: backend, clock: clock, net: fn, url: url}
}

// clientFor returns a client whose access key (a 12-digit account ID) selects the calling account.
func clientFor(t *testing.T, url, account string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(account, "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) })
}

func newOtherAccountClient(t *testing.T, f *fixture) *sdk.Client {
	t.Helper()

	return clientFor(t, f.url, "210987654321")
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

// signCSR issues a certificate for the CSR from a fresh self-signed root, returning (leaf, root) PEM.
func signCSR(t *testing.T, csrPEM string) (string, string) {
	t.Helper()

	blk, _ := pem.Decode([]byte(csrPEM))
	require.NotNil(t, blk)

	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	require.NoError(t, err)
	require.NoError(t, csr.CheckSignature())

	return issue(t, csr.PublicKey)
}

func issue(t *testing.T, leafKey any) (string, string) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test root"},
		NotBefore: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "cluster"},
		NotBefore: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), NotAfter: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, leafKey, caKey)
	require.NoError(t, err)

	enc := func(der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}

	return enc(leafDER), enc(caDER)
}
