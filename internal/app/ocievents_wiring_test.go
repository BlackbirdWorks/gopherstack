package app

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocievents "github.com/blackbirdworks/gopherstack/services/ocievents"
	ocifunctions "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocinotifications "github.com/blackbirdworks/gopherstack/services/ocinotifications"
	ociobjectstorage "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	ocistreaming "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

func TestReserveFixedServicePorts_OCIEvents(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8164, ocievents.DefaultPort)

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", 8100, 8200, true},
		{"port outside pool range", 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{OCIEvents: ocievents.Settings{Port: ocievents.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocievents.DefaultPort))
		})
	}
}

func TestWireOCIEvents(t *testing.T) {
	t.Parallel()

	evH := ocievents.NewHandler(ocievents.NewInMemoryBackend())
	fnH := ocifunctions.NewHandler(ocifunctions.NewInMemoryBackend())
	osH := ociobjectstorage.NewHandler(ociobjectstorage.NewInMemoryBackend())
	byName := map[string]service.Registerable{
		"OCIEvents": evH, "OCIFunctions": fnH, "OCIObjectStorage": osH,
		"OCIIdentity":      ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend()),
		"OCINotifications": ocinotifications.NewHandler(ocinotifications.NewInMemoryBackend()),
		"OCIStreaming":     ocistreaming.NewHandler(ocistreaming.NewInMemoryBackend()),
	}

	assert.NotPanics(t, func() {
		wireOCIEvents(byName)
		wireOCIEvents(map[string]service.Registerable{"OCIEvents": evH})
		wireOCIEvents(map[string]service.Registerable{})
	})

	cli := CLI{OCIEvents: ocievents.Settings{Port: 8164}}
	assert.Equal(t, 8164, cli.GetOCIEventsSettings().Port)
}

func TestOCIEventsTargetsRejectUnknown(t *testing.T) {
	t.Parallel()

	fn := ociEventsFunctions{b: ocifunctions.NewInMemoryBackend()}
	assert.False(t, fn.FunctionActive("ocid1.fnfunc.oc1..nope"))
	require.Error(t, fn.InvokeFunction(t.Context(), "ocid1.fnfunc.oc1..nope", []byte("{}")))

	ons := ociEventsNotifier{b: ocinotifications.NewInMemoryBackend()}
	assert.False(t, ons.TopicActive("ocid1.onstopic.oc1..nope"))
	require.Error(t, ons.PublishEvent("ocid1.onstopic.oc1..nope", "{}"))

	oss := ociEventsStreamer{b: ocistreaming.NewInMemoryBackend()}
	assert.False(t, oss.StreamActive("ocid1.stream.oc1..nope"))
	require.Error(t, oss.AppendMessage("ocid1.stream.oc1..nope", nil, []byte("{}")))
}

func TestOCIEventsMapping(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	obj := objectEvent(ociobjectstorage.ObjectEvent{
		Time: at, EventType: ociobjectstorage.EventCreateObject, CompartmentID: "c", Namespace: "ns",
		BucketName: "b", BucketID: "bid", ObjectName: "o", ETag: "e",
	})
	assert.Equal(t, "objectstorage", obj.Source)
	assert.Equal(t, "o", obj.ResourceName)
	assert.Equal(t, "b", obj.AdditionalDetails["bucketName"])
	assert.Nil(t, obj.AdditionalDetails["archivalState"])

	fn := functionsEvent(ocifunctions.ResourceEvent{
		Time: at, EventType: ocifunctions.EventCreateFunction, CompartmentID: "c", ResourceName: "f", ResourceID: "fid",
	})
	assert.Equal(t, "functions", fn.Source)
	assert.Equal(t, "fid", fn.ResourceID)
	assert.Equal(t, ocifunctions.EventCreateFunction, fn.EventType)
}
