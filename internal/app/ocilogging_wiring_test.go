package app

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocifunctionsbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocilogging "github.com/blackbirdworks/gopherstack/services/ocilogging"
	ocimonitoring "github.com/blackbirdworks/gopherstack/services/ocimonitoring"
	ocinotificationsbackend "github.com/blackbirdworks/gopherstack/services/ocinotifications"
)

const wireCompartment = "ocid1.compartment.oc1..c"

func TestReserveFixedServicePorts_OCILoggingMonitoring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cli        CLI
		port       int
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{
			"logging inside pool",
			CLI{OCILogging: ocilogging.Settings{Port: ocilogging.DefaultPort}},
			ocilogging.DefaultPort,
			8100,
			8200,
			true,
		},
		{
			"logging outside pool",
			CLI{OCILogging: ocilogging.Settings{Port: ocilogging.DefaultPort}},
			ocilogging.DefaultPort,
			10000,
			10100,
			false,
		},
		{
			"monitoring inside pool", CLI{OCIMonitoring: ocimonitoring.Settings{Port: ocimonitoring.DefaultPort}},
			ocimonitoring.DefaultPort, 8100, 8200, true,
		},
		{
			"monitoring outside pool", CLI{OCIMonitoring: ocimonitoring.Settings{Port: ocimonitoring.DefaultPort}},
			ocimonitoring.DefaultPort, 10000, 10100, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, tt.cli)
			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(tt.port))
		})
	}
}

func TestOCILoggingMonitoringDefaultPorts(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8160, ocilogging.DefaultPort)
	assert.Equal(t, 8161, ocimonitoring.DefaultPort)
}

func TestWireOCIFunctionInvocationLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		category string
		wantHits int
	}{
		{"log enabled for the application", "ocid1.fnapp.oc1..app", "invoke", 1},
		{"log for another application", "ocid1.fnapp.oc1..other", "invoke", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logH := ocilogging.NewHandler(ocilogging.NewInMemoryBackend())
			fnH := ocifunctionsbackend.NewHandler(ocifunctionsbackend.NewInMemoryBackend())
			wireOCILoggingMonitoring(map[string]service.Registerable{"OCILogging": logH, "OCIFunctions": fnH})

			b := logH.Backend
			b.SetTransition(0)

			_, err := b.CreateLogGroup(ocilogging.CreateLogGroupIn{Name: "g", Compartment: wireCompartment})
			require.NoError(t, err)

			groups, err := b.ListLogGroups(wireCompartment, "", ocilogging.ListOpts{})
			require.NoError(t, err)
			require.Len(t, groups.Data, 1)

			_, err = b.CreateLog(groups.Data[0].ID, ocilogging.CreateLogIn{
				Name: "fn", LogType: "SERVICE",
				Configuration: &ocilogging.ConfigWire{Source: &ocilogging.SourceWire{
					Service: "functions", Resource: tt.resource, Category: tt.category,
				}},
			})
			require.NoError(t, err)

			functionLogObserver(b)(ocifunctionsbackend.InvokeEvent{
				Time:         time.Now(),
				AppID:        "ocid1.fnapp.oc1..app",
				FunctionName: "hello",
				FunctionID:   "ocid1.fnfunc.oc1..f",
				CallID:       "call",
				Status:       200,
				Duration:     5 * time.Millisecond,
			})

			now := time.Now()
			out, _, err := b.SearchLogs(ocilogging.SearchIn{
				TimeStart: new(now.Add(-time.Hour)), TimeEnd: new(now.Add(time.Hour)),
				SearchQuery: `search "` + wireCompartment + `/g/fn" | where data.functionName = 'hello'`,
			}, "", 0)
			require.NoError(t, err)
			assert.Equal(t, tt.wantHits, out.Summary.ResultCount)
		})
	}
}

func TestWireOCIMonitoringNotifications(t *testing.T) {
	t.Parallel()

	monH := ocimonitoring.NewHandler(ocimonitoring.NewInMemoryBackend())
	notifyH := ocinotificationsbackend.NewHandler(ocinotificationsbackend.NewInMemoryBackend())
	identityH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	wireOCILoggingMonitoring(map[string]service.Registerable{
		"OCIMonitoring": monH, "OCINotifications": notifyH, "OCIIdentity": identityH,
	})

	nb := notifyH.Backend
	nb.SetTransition(0)

	topic, err := nb.CreateTopic(
		ocinotificationsbackend.CreateTopicIn{Name: "t", Compartment: wireCompartment},
		"http://x",
	)
	require.NoError(t, err)

	_, err = nb.CreateSubscription(ocinotificationsbackend.CreateSubscriptionIn{
		TopicID: topic.TopicID, Compartment: wireCompartment, Protocol: "ORACLE_FUNCTIONS",
		Endpoint: "ocid1.fnfunc.oc1..ghost",
	}, "http://x")
	require.NoError(t, err)

	mb := monH.Backend

	create := func(dest string) error {
		_, _, cerr := mb.CreateAlarm(ocimonitoring.CreateAlarmIn{
			Name: "a", Compartment: wireCompartment, MetricComp: wireCompartment, Namespace: "ns",
			Query: "m[1m].max() > 1", Severity: "CRITICAL", Destinations: []string{dest}, Enabled: new(true),
		})

		return cerr
	}

	require.Error(t, create("ocid1.onstopic.oc1..ghost"), "unknown topics are rejected once Notifications is wired")
	require.NoError(t, create(topic.TopicID))

	now := time.Now()
	_, err = mb.PostMetricData(ocimonitoring.PostMetricDataIn{MetricData: []ocimonitoring.MetricDataIn{{
		Namespace: "ns", CompartmentID: wireCompartment, Name: "m", Dimensions: map[string]string{"k": "v"},
		Datapoints: []ocimonitoring.DatapointIn{{Timestamp: &now, Value: new(5.0)}},
	}}})
	require.NoError(t, err)

	mb.Sweep(t.Context())

	deliveries := nb.Deliveries(topic.TopicID, "")
	require.Len(t, deliveries, 1)
	assert.Contains(t, deliveries[0].Body, "OK_TO_FIRING")
	assert.Equal(t, "JSON", deliveries[0].MessageType)
}
