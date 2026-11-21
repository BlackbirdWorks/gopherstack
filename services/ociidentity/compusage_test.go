package ociidentity_test

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeHolder struct {
	held  map[string]string
	onAsk func()
	mu    sync.Mutex
}

func (f *fakeHolder) set(id, what string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if what == "" {
		delete(f.held, id)

		return
	}

	f.held[id] = what
}

func (f *fakeHolder) holds(id string) string {
	if f.onAsk != nil {
		f.onAsk()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.held[id]
}

func TestDeleteCompartmentConsultsOtherServices(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	h := &fakeHolder{held: map[string]string{}}
	h.onAsk = func() { e.backend.CompartmentActive(testTenancy) }
	e.backend.AddCompartmentUsage(h.holds)

	c := e.mkCompartment(t, testTenancy, "busy")
	h.set(*c.Id, "Object Storage bucket data")

	_, err := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
	assert.Contains(t, se.GetMessage(), "Object Storage bucket data")

	got, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, identity.CompartmentLifecycleStateActive, got.LifecycleState)

	h.set(*c.Id, "")

	del, err := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)

	e.clock.Advance(3 * time.Second)

	wr, err := e.client.GetWorkRequest(ctx, identity.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, identity.WorkRequestStatusSucceeded, wr.Status)
}

func TestDeleteCompartmentFailsWhenResourceAppearsDuringWindow(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	h := &fakeHolder{held: map[string]string{}}
	e.backend.AddCompartmentUsage(h.holds)

	c := e.mkCompartment(t, testTenancy, "racy")

	del, err := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)

	mid, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, identity.CompartmentLifecycleStateDeleting, mid.LifecycleState)

	h.set(*c.Id, "DNS zone example.com")
	e.clock.Advance(3 * time.Second)

	got, err := e.client.GetCompartment(ctx, identity.GetCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, identity.CompartmentLifecycleStateActive, got.LifecycleState, "back to ACTIVE")

	wr, err := e.client.GetWorkRequest(ctx, identity.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, identity.WorkRequestStatusFailed, wr.Status)
	require.Len(t, wr.Errors, 1)
	assert.Equal(t, "Conflict", *wr.Errors[0].Code)
	assert.Contains(t, *wr.Errors[0].Message, "DNS zone example.com")

	h.set(*c.Id, "")

	_, err = e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)
}
