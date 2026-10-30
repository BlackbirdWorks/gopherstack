package ocifunctions_test

import (
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

func TestResourceEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	var (
		mu  sync.Mutex
		got []oci.ResourceEvent
	)

	e.backend.SetEventSink(func(ev oci.ResourceEvent) {
		mu.Lock()
		got = append(got, ev)
		mu.Unlock()
	})

	app := e.mkApp(t, "evapp", nil)
	fn := e.mkFn(t, app, "evfn", nil)

	_, err := e.mgmt.DeleteFunction(t.Context(), functions.DeleteFunctionRequest{FunctionId: fn.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.mgmt.DeleteApplication(t.Context(), functions.DeleteApplicationRequest{ApplicationId: app.Id})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	wantTypes := []string{
		oci.EventCreateApplication,
		oci.EventCreateFunction,
		oci.EventDeleteFunction,
		oci.EventDeleteApplication,
	}
	wantNames := []string{"evapp", "evfn", "evfn", "evapp"}
	wantIDs := []string{*app.Id, *fn.Id, *fn.Id, *app.Id}

	require.Len(t, got, len(wantTypes))

	for i := range wantTypes {
		assert.Equal(t, wantTypes[i], got[i].EventType)
		assert.Equal(t, wantNames[i], got[i].ResourceName)
		assert.Equal(t, wantIDs[i], got[i].ResourceID)
		assert.Equal(t, testCompartment, got[i].CompartmentID)
	}
}
