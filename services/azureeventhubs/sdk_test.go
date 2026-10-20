package azureeventhubs_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

const (
	sdkTimeout = 20 * time.Second
	testNS     = "ns1"
	testHub    = "hub1"
)

type env struct {
	h         *azureeventhubs.Handler
	b         *azureeventhubs.InMemoryBackend
	pool      *x509.CertPool
	plainAddr string
	tlsAddr   string
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return ln
}

func newEnv(t *testing.T, validateSAS bool) *env {
	t.Helper()

	b := azureeventhubs.NewInMemoryBackend()
	h := azureeventhubs.NewHandler(b)
	h.ValidateSAS = validateSAS

	require.NoError(t, b.EnsureNamespace(testNS))
	_, err := b.CreateHub(
		azureeventhubs.HubRef{Namespace: testNS, Hub: testHub},
		azureeventhubs.HubConfig{PartitionCount: 4},
	)
	require.NoError(t, err)

	cert, err := devtls.GenerateSelfSignedCert()
	require.NoError(t, err)

	h.AMQPCert = &cert

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	plain, secure := listenLocal(t), listenLocal(t)
	e := &env{h: h, b: b, pool: pool, plainAddr: plain.Addr().String(), tlsAddr: secure.Addr().String()}

	require.NoError(t, h.ServeAMQP(plain, false))
	require.NoError(t, h.ServeAMQP(secure, true))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return e
}

func (e *env) connStr(key string) string {
	return fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=%s;SharedAccessKey=%s;UseDevelopmentEmulator=true",
		e.plainAddr, azureeventhubs.DefaultKeyName, key)
}

func (e *env) producer(t *testing.T, key string) *azeventhubs.ProducerClient {
	t.Helper()

	c, err := azeventhubs.NewProducerClientFromConnectionString(e.connStr(key), testHub, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	return c
}

func (e *env) consumer(t *testing.T) *azeventhubs.ConsumerClient {
	t.Helper()

	c, err := azeventhubs.NewConsumerClientFromConnectionString(
		e.connStr(azureeventhubs.DefaultKeyValue), testHub, azeventhubs.DefaultConsumerGroup, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	return c
}

func (e *env) tlsProducerFor(t *testing.T) *azeventhubs.ProducerClient {
	t.Helper()

	_, port, err := net.SplitHostPort(e.tlsAddr)
	require.NoError(t, err)

	cs := fmt.Sprintf("Endpoint=sb://localhost:%s;SharedAccessKeyName=%s;SharedAccessKey=%s",
		port, azureeventhubs.DefaultKeyName, azureeventhubs.DefaultKeyValue)

	c, err := azeventhubs.NewProducerClientFromConnectionString(cs, testHub, &azeventhubs.ProducerClientOptions{
		TLSConfig: &tls.Config{RootCAs: e.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	return c
}

func sdkCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), sdkTimeout)
	t.Cleanup(cancel)

	return ctx
}

func sendBatch(t *testing.T, p *azeventhubs.ProducerClient, opts *azeventhubs.EventDataBatchOptions, bodies ...string) {
	t.Helper()

	ctx := sdkCtx(t)

	batch, err := p.NewEventDataBatch(ctx, opts)
	require.NoError(t, err)

	for _, body := range bodies {
		require.NoError(t, batch.AddEventData(&azeventhubs.EventData{Body: []byte(body)}, nil))
	}

	require.NoError(t, p.SendEventDataBatch(ctx, batch, nil))
}

func receive(
	t *testing.T, c *azeventhubs.ConsumerClient, pid string, pos azeventhubs.StartPosition, count int,
) []*azeventhubs.ReceivedEventData {
	t.Helper()

	pc, err := c.NewPartitionClient(pid, &azeventhubs.PartitionClientOptions{StartPosition: pos})
	require.NoError(t, err)

	defer pc.Close(context.Background())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	evs, err := pc.ReceiveEvents(ctx, count, nil)
	require.NoError(t, err)

	return evs
}

func bodies(evs []*azeventhubs.ReceivedEventData) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = string(e.Body)
	}

	return out
}

func earliest() azeventhubs.StartPosition { return azeventhubs.StartPosition{Earliest: new(true)} }

func TestSDK_Properties(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	p := e.producer(t, azureeventhubs.DefaultKeyValue)
	ctx := sdkCtx(t)

	props, err := p.GetEventHubProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, testHub, props.Name)
	assert.Equal(t, []string{"0", "1", "2", "3"}, props.PartitionIDs)
	assert.False(t, props.CreatedOn.IsZero())

	pp, err := p.GetPartitionProperties(ctx, "2", nil)
	require.NoError(t, err)
	assert.True(t, pp.IsEmpty)
	assert.EqualValues(t, -1, pp.LastEnqueuedSequenceNumber)
	assert.Equal(t, "2", pp.PartitionID)

	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: new("2")}, "a", "b", "c")

	pp, err = p.GetPartitionProperties(ctx, "2", nil)
	require.NoError(t, err)
	assert.False(t, pp.IsEmpty)
	assert.EqualValues(t, 0, pp.BeginningSequenceNumber)
	assert.EqualValues(t, 2, pp.LastEnqueuedSequenceNumber)
	assert.NotEmpty(t, pp.LastEnqueuedOffset)
	assert.WithinDuration(t, time.Now(), pp.LastEnqueuedOn, time.Minute)
}

func TestSDK_ReceivePositions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	p, c := e.producer(t, azureeventhubs.DefaultKeyValue), e.consumer(t)

	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: new("1")}, "e0", "e1", "e2", "e3")

	all := receive(t, c, "1", earliest(), 4)
	require.Equal(t, []string{"e0", "e1", "e2", "e3"}, bodies(all))

	for i, ev := range all {
		assert.EqualValues(t, i, ev.SequenceNumber)
		assert.NotEmpty(t, ev.Offset)
		assert.NotNil(t, ev.EnqueuedTime)
	}

	tests := []struct {
		pos  azeventhubs.StartPosition
		name string
		want []string
	}{
		{name: "offset", pos: azeventhubs.StartPosition{Offset: &all[1].Offset}, want: []string{"e2", "e3"}},
		{
			name: "offset inclusive",
			pos:  azeventhubs.StartPosition{Offset: &all[1].Offset, Inclusive: true},
			want: []string{"e1", "e2", "e3"},
		},
		{
			name: "sequence",
			pos:  azeventhubs.StartPosition{SequenceNumber: &all[2].SequenceNumber},
			want: []string{"e3"},
		},
		{
			name: "sequence inclusive",
			pos:  azeventhubs.StartPosition{SequenceNumber: &all[2].SequenceNumber, Inclusive: true},
			want: []string{"e2", "e3"},
		},
		{
			name: "enqueued time",
			pos:  azeventhubs.StartPosition{EnqueuedTime: new(all[0].EnqueuedTime.Add(-time.Second))},
			want: []string{"e0", "e1", "e2", "e3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, bodies(receive(t, c, "1", tt.pos, len(tt.want))))
		})
	}
}

func TestSDK_ReceiveLatest(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	p, c := e.producer(t, azureeventhubs.DefaultKeyValue), e.consumer(t)

	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: new("0")}, "old")

	pc, err := c.NewPartitionClient("0", &azeventhubs.PartitionClientOptions{
		StartPosition: azeventhubs.StartPosition{Latest: new(true)},
	})
	require.NoError(t, err)

	defer pc.Close(context.Background())

	got := make(chan []*azeventhubs.ReceivedEventData, 1)

	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		evs, _ := pc.ReceiveEvents(ctx, 1, nil)
		got <- evs
	}()

	require.Eventually(
		t,
		func() bool { return azureeventhubs.ReceiverCount(e.b) == 1 },
		10*time.Second,
		10*time.Millisecond,
	)

	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: new("0")}, "new")

	select {
	case evs := <-got:
		assert.Equal(t, []string{"new"}, bodies(evs))
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the latest event")
	}
}

func TestSDK_PartitionKeyAndTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		producer func(*testing.T, *env) *azeventhubs.ProducerClient
		name     string
	}{
		{name: "plain", producer: func(t *testing.T, e *env) *azeventhubs.ProducerClient {
			t.Helper()

			return e.producer(t, azureeventhubs.DefaultKeyValue)
		}},
		{name: "tls", producer: func(t *testing.T, e *env) *azeventhubs.ProducerClient {
			t.Helper()

			return e.tlsProducerFor(t)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			p := tt.producer(t, e)
			key := "key-" + tt.name

			sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionKey: &key}, "k1", "k2", "k3")
			sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionKey: &key}, "k4")

			ctx := sdkCtx(t)
			holders := 0

			for _, id := range []string{"0", "1", "2", "3"} {
				pp, err := p.GetPartitionProperties(ctx, id, nil)
				require.NoError(t, err)

				if pp.IsEmpty {
					continue
				}

				holders++

				evs := receive(t, e.consumer(t), id, earliest(), 4)
				require.Len(t, evs, 4)
				assert.Equal(t, &key, evs[0].PartitionKey)
			}

			assert.Equal(t, 1, holders, "all events with one key land in one partition")
		})
	}
}

func TestSDK_EpochReceiver(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	p := e.producer(t, azureeventhubs.DefaultKeyValue)
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: new("3")}, "x")

	open := func(epoch int64) *azeventhubs.PartitionClient {
		pc, err := e.consumer(t).NewPartitionClient("3", &azeventhubs.PartitionClientOptions{
			StartPosition: azeventhubs.StartPosition{Earliest: new(true)}, OwnerLevel: &epoch,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = pc.Close(context.Background()) })

		return pc
	}

	old := open(1)

	evs, err := old.ReceiveEvents(sdkCtx(t), 1, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, bodies(evs))

	newer := open(2)
	evs, err = newer.ReceiveEvents(sdkCtx(t), 1, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, bodies(evs))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err = old.ReceiveEvents(ctx, 1, nil)
	var ehErr *azeventhubs.Error
	require.ErrorAs(t, err, &ehErr)
	assert.Equal(t, azeventhubs.ErrorCodeOwnershipLost, ehErr.Code)

	_, err = open(0).ReceiveEvents(ctx, 1, nil)
	require.ErrorAs(t, err, &ehErr)
	assert.Equal(t, azeventhubs.ErrorCodeOwnershipLost, ehErr.Code)
}

func TestSDK_SAS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "valid", key: azureeventhubs.DefaultKeyValue},
		{name: "invalid", key: "d3Jvbmcta2V5LXdyb25nLWtleS13cm9uZy1rZXktMTIzNDU=", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			p := e.producer(t, tt.key)

			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()

			_, err := p.GetEventHubProperties(ctx, &azeventhubs.GetEventHubPropertiesOptions{})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			sendBatch(t, p, nil, "authorized")
		})
	}
}

func TestSDK_RestrictedRule(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)

	rule, err := e.b.PutRule(testNS, "", "listen-only", []string{azureeventhubs.RightListen})
	require.NoError(t, err)

	cs := fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=listen-only;SharedAccessKey=%s;UseDevelopmentEmulator=true",
		e.plainAddr, rule.PrimaryKey)

	p, err := azeventhubs.NewProducerClientFromConnectionString(cs, testHub, nil)
	require.NoError(t, err)

	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()

	batch, err := p.NewEventDataBatch(ctx, nil)
	if err == nil {
		require.NoError(t, batch.AddEventData(&azeventhubs.EventData{Body: []byte("x")}, nil))
		err = p.SendEventDataBatch(ctx, batch, nil)
	}

	require.Error(t, err, "a Listen-only rule cannot send")

	info, err := e.b.PartitionInfo(azureeventhubs.HubRef{Namespace: testNS, Hub: testHub}, 0)
	require.NoError(t, err)
	assert.True(t, info.Empty)
}
