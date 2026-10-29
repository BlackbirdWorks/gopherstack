package ociqueue_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
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

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/queue"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ociqueue"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
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
	c.ns.Store(time.Now().UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *oci.InMemoryBackend
	handler *oci.Handler
	clock   *fakeClock
	prov    common.ConfigurationProvider
	kp      keyPair
	admin   queue.QueueAdminClient
	base    string
}

type envOpts struct {
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

	be := oci.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	h := oci.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	e := &env{
		backend: be, handler: h, clock: clock, kp: kp,
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, oci.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.admin, err = queue.NewQueueAdminClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.admin.Host = e.base

	return e
}

func (e *env) data(t *testing.T, q queue.Queue) queue.QueueClient {
	t.Helper()

	c, err := queue.NewQueueClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	c.Host = *q.MessagesEndpoint

	return c
}

// settle advances past the CREATING/UPDATING/DELETING transition.
func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func (e *env) mkQueue(t *testing.T, name string, mod func(*queue.CreateQueueDetails)) queue.Queue {
	t.Helper()

	d := queue.CreateQueueDetails{CompartmentId: new(testCompartment), DisplayName: new(name)}
	if mod != nil {
		mod(&d)
	}

	resp, err := e.admin.CreateQueue(t.Context(), queue.CreateQueueRequest{CreateQueueDetails: d})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)
	e.settle()

	list, err := e.admin.ListQueues(t.Context(), queue.ListQueuesRequest{
		CompartmentId: new(testCompartment), DisplayName: new(name),
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.Items)

	got, err := e.admin.GetQueue(t.Context(), queue.GetQueueRequest{QueueId: list.Items[len(list.Items)-1].Id})
	require.NoError(t, err)

	return got.Queue
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

func put(t *testing.T, c queue.QueueClient, q queue.Queue, channel string, contents ...string) {
	t.Helper()

	entries := make([]queue.PutMessagesDetailsEntry, 0, len(contents))

	for _, content := range contents {
		e := queue.PutMessagesDetailsEntry{Content: new(content)}
		if channel != "" {
			e.Metadata = &queue.MessageMetadata{ChannelId: new(channel)}
		}

		entries = append(entries, e)
	}

	resp, err := c.PutMessages(t.Context(), queue.PutMessagesRequest{
		QueueId: q.Id, Messages: entries,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, len(contents))
}

func get(t *testing.T, c queue.QueueClient, q queue.Queue, mod func(*queue.GetMessagesRequest)) []queue.GetMessage {
	t.Helper()

	r := queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(0)}
	if mod != nil {
		mod(&r)
	}

	resp, err := c.GetMessages(t.Context(), r)
	require.NoError(t, err)

	return resp.Messages
}

func quote(s string) string {
	b, _ := json.Marshal(s)

	return string(b)
}
