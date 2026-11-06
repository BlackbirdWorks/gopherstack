package azurefunctions_test

import (
	"context"
	"net"
	"encoding/base64"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

const (
	devAccount = "devstoreaccount1"
	devKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func triggersZip(t *testing.T) []byte {
	t.Helper()

	queueOut := func(name string) string {
		return `{"type":"queue","direction":"out","name":"out","queueName":"` + name + `","connection":"AzureWebJobsStorage"}`
	}

	return zipOf(t, map[string]string{
		"host.json": `{"version":"2.0","extensionBundle":{"id":"Microsoft.Azure.Functions.ExtensionBundle",` +
			`"version":"[4.*, 5.0.0)"}}`,
		"ticker/function.json": `{"bindings":[{"type":"timerTrigger","direction":"in","name":"t","schedule":"*/3 * * * * *"},` +
			queueOut("timer-out") + `]}`,
		"ticker/index.js": `module.exports = async function (context) { context.bindings.out = "tick"; };`,
		"fromqueue/function.json": `{"bindings":[{"type":"queueTrigger","direction":"in","name":"msg","queueName":"qin",` +
			`"connection":"AzureWebJobsStorage"},` + queueOut("qout") + `]}`,
		"fromqueue/index.js": `module.exports = async function (context, msg) { context.bindings.out = "got:" + msg; };`,
		"fromsb/function.json": `{"bindings":[{"type":"serviceBusTrigger","direction":"in","name":"msg","queueName":"sbq",` +
			`"connection":"SB_CONN"},` + queueOut("sb-out") + `]}`,
		"fromsb/index.js": `module.exports = async function (context, msg) { context.bindings.out = "sb:" + msg; };`,
		"fromeh/function.json": `{"bindings":[{"type":"eventHubTrigger","direction":"in","name":"events","eventHubName":"hub1",` +
			`"connection":"EH_CONN","cardinality":"one","consumerGroup":"$Default"},` + queueOut("eh-out") + `]}`,
		"fromeh/index.js": `module.exports = async function (context, ev) { context.bindings.out = "eh:" + ev; };`,
		"fromblob/function.json": `{"bindings":[{"type":"blobTrigger","direction":"in","name":"blob","path":"blobin/{name}",` +
			`"connection":"AzureWebJobsStorage"},` + queueOut("blob-out") + `]}`,
		"fromblob/index.js": `module.exports = async function (context, blob) { context.bindings.out = ` +
			`"blob:" + context.bindingData.name + ":" + blob.toString(); };`,
	})
}

func decodeQueueText(s string) string {
	if raw, err := base64.StdEncoding.DecodeString(s); err == nil {
		return string(raw)
	}

	return s
}

// TestDocker_BackgroundTriggers runs a real Functions host whose timer, queue and blob triggers talk to the
// emulator's Storage listeners through the container-reachable address. Opt-in: GOPHERSTACK_AZFN_DOCKER_TEST=1.
func TestDocker_BackgroundTriggers(t *testing.T) {
	t.Parallel()

	if os.Getenv(dockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", dockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "azurefunctions.triggers.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	ports := portalloctest.New(t, 4)
	blobPort, err := ports.Acquire("blob")
	require.NoError(t, err)

	queuePort, err := ports.Acquire("queue")
	require.NoError(t, err)

	blobH := azureblob.NewHandler(azureblob.NewInMemoryBackend())
	blobH.Port = blobPort
	require.NoError(t, blobH.StartWorker(t.Context()))
	t.Cleanup(func() { blobH.Shutdown(t.Context()) })

	queueH := azurequeue.NewHandler(azurequeue.NewInMemoryBackend())
	queueH.Port = queuePort
	require.NoError(t, queueH.StartWorker(t.Context()))
	t.Cleanup(func() { queueH.Shutdown(t.Context()) })

	sbH := azureservicebus.NewHandler(azureservicebus.NewInMemoryBackend())
	sbLn, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	require.NoError(t, sbH.ServeAMQP(sbLn, false))
	t.Cleanup(func() { sbH.Shutdown(t.Context()) })

	_, sbPort, err := net.SplitHostPort(sbLn.Addr().String())
	require.NoError(t, err)

	_, err = sbH.Backend.CreateQueue("sbq")
	require.NoError(t, err)

	sbConn := "Endpoint=sb://127.0.0.1:" + sbPort + ";SharedAccessKeyName=" + azureservicebus.DefaultKeyName +
		";SharedAccessKey=" + azureservicebus.DefaultKeyValue + ";UseDevelopmentEmulator=true"

	ehB := azureeventhubs.NewInMemoryBackend()
	ehH := azureeventhubs.NewHandler(ehB)
	require.NoError(t, ehB.EnsureNamespace("ns1"))
	_, err = ehB.CreateHub(azureeventhubs.HubRef{Namespace: "ns1", Hub: "hub1"}, azureeventhubs.HubConfig{PartitionCount: 2})
	require.NoError(t, err)

	ehLn, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	require.NoError(t, ehH.ServeAMQP(ehLn, false))
	t.Cleanup(func() { ehH.Shutdown(t.Context()) })

	_, ehPort, err := net.SplitHostPort(ehLn.Addr().String())
	require.NoError(t, err)

	ehConn := "Endpoint=sb://127.0.0.1:" + ehPort + ";SharedAccessKeyName=" + azureeventhubs.DefaultKeyName +
		";SharedAccessKey=" + azureeventhubs.DefaultKeyValue + ";UseDevelopmentEmulator=true"

	blobURL := "http://127.0.0.1:" + strconv.Itoa(blobPort) + "/" + devAccount
	queueURL := "http://127.0.0.1:" + strconv.Itoa(queuePort) + "/" + devAccount
	conn := "DefaultEndpointsProtocol=http;AccountName=" + devAccount + ";AccountKey=" + devKey +
		";BlobEndpoint=" + blobURL + ";QueueEndpoint=" + queueURL + ";"

	e := newEnv(t, false)
	e.backend.SetRuntime(func() (azurefunctions.Runner, error) { return lr.Get() })
	e.backend.Configure(azurefunctions.Settings{StartupTimeout: 8 * time.Minute})
	e.backend.SetNetwork(azurefunctions.Network{BlobPort: blobPort, QueuePort: queuePort})

	e.backend.PutApp("bg", azurefunctions.AppConfig{Kind: "functionapp,linux", LinuxFx: "NODE|20"})
	require.NoError(t, e.backend.SetSettings("bg", map[string]string{
		"AzureWebJobsStorage": conn, "FUNCTIONS_WORKER_RUNTIME": "node", "SB_CONN": sbConn, "EH_CONN": ehConn,
	}))

	cred, err := azqueue.NewSharedKeyCredential(devAccount, devKey)
	require.NoError(t, err)

	svc, err := azqueue.NewServiceClientWithSharedKeyCredential(queueURL, cred, nil)
	require.NoError(t, err)

	bcred, err := azblob.NewSharedKeyCredential(devAccount, devKey)
	require.NoError(t, err)

	blobs, err := azblob.NewClientWithSharedKeyCredential(blobURL, bcred, nil)
	require.NoError(t, err)

	_, _ = svc.CreateQueue(t.Context(), "qin", nil)
	_, err = blobs.CreateContainer(t.Context(), "blobin", nil)
	require.NoError(t, err)

	_, err = svc.NewQueueClient("qin").EnqueueMessage(
		t.Context(), base64.StdEncoding.EncodeToString([]byte("hello")), nil)
	require.NoError(t, err)

	sbClient, err := azservicebus.NewClientFromConnectionString(sbConn, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sbClient.Close(context.Background()) })

	sender, err := sbClient.NewSender("sbq", nil)
	require.NoError(t, err)

	require.NoError(t, sender.SendMessage(t.Context(), &azservicebus.Message{Body: []byte("bus")}, nil))
	require.NoError(t, sender.Close(t.Context()))

	producer, err := azeventhubs.NewProducerClientFromConnectionString(ehConn, "hub1", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	batch, err := producer.NewEventDataBatch(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, batch.AddEventData(&azeventhubs.EventData{Body: []byte("hub")}, nil))
	require.NoError(t, producer.SendEventDataBatch(t.Context(), batch, nil))

	_, err = blobs.UploadBuffer(t.Context(), "blobin", "doc.txt", []byte("payload"), nil)
	require.NoError(t, err)

	resp, _ := e.do(t, http.MethodPost, "bg.scm."+testDomain, "/api/zipdeploy", triggersZip(t), e.kuduAuth(t, "bg"))
	require.Equal(t, http.StatusOK, resp.StatusCode, "deploying starts the host: nothing sends it a request")

	tests := []struct {
		name  string
		queue string
		want  string
	}{
		{"timer", "timer-out", "tick"},
		{"queue", "qout", "got:hello"},
		{"servicebus", "sb-out", "sb:bus"},
		{"eventhubs", "eh-out", "eh:hub"},
		{"blob", "blob-out", "blob:doc.txt:payload"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				res, peekErr := svc.NewQueueClient(tt.queue).PeekMessages(t.Context(), &azqueue.PeekMessagesOptions{
					NumberOfMessages: new(int32(10)),
				})
				if !assert.NoError(c, peekErr) {
					return
				}

				got := make([]string, 0, len(res.Messages))
				for _, m := range res.Messages {
					got = append(got, decodeQueueText(*m.MessageText))
				}

				assert.Contains(c, got, tt.want)
			}, 6*time.Minute, 3*time.Second)
		})
	}
}
