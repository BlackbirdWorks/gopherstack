package ocicore_test

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

func sshKey(t *testing.T, bits int) (string, string) {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, bits)
	require.NoError(t, err)

	pub, err := ssh.NewPublicKey(&k.PublicKey)
	require.NoError(t, err)

	return string(ssh.MarshalAuthorizedKey(pub)), ssh.FingerprintLegacyMD5(pub)
}

func TestInstanceConsoleConnections(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	inst := e.mustLaunch(t, f.pub)
	e.settle()

	key, fp := sshKey(t, 2048)
	small, _ := sshKey(t, 1024)

	tests := []struct {
		inst     *string
		name     string
		key      string
		wantCode int
	}{
		{inst.Id, "not a key", "garbage", http.StatusBadRequest},
		{inst.Id, "small key", small, http.StatusBadRequest},
		{new("ocid1.instance.oc1.iad.nope"), "unknown instance", key, http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.compute.CreateInstanceConsoleConnection(
				t.Context(),
				core.CreateInstanceConsoleConnectionRequest{
					InstanceId: tc.inst, PublicKey: new(tc.key),
				},
			)
			assert.Equal(t, tc.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	c, err := e.compute.CreateInstanceConsoleConnection(ctx, core.CreateInstanceConsoleConnectionRequest{
		InstanceId: inst.Id, PublicKey: new(key),
	})
	require.NoError(t, err)
	assert.Equal(t, fp, *c.Fingerprint)
	assert.Equal(t, core.InstanceConsoleConnectionLifecycleStateCreating, c.LifecycleState)
	assert.Contains(t, *c.ConnectionString, *c.Id)
	assert.Contains(t, *c.ConnectionString, *inst.Id)

	e.settle()

	l, err := e.compute.ListInstanceConsoleConnections(ctx, core.ListInstanceConsoleConnectionsRequest{
		CompartmentId: new(testComp), InstanceId: inst.Id,
	})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)
	assert.Equal(t, core.InstanceConsoleConnectionLifecycleStateActive, l.Items[0].LifecycleState)

	_, err = e.compute.DeleteInstanceConsoleConnection(ctx, core.DeleteInstanceConsoleConnectionRequest{
		InstanceConsoleConnectionId: c.Id,
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.compute.GetInstanceConsoleConnection(ctx, core.GetInstanceConsoleConnectionRequest{
		InstanceConsoleConnectionId: c.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, core.InstanceConsoleConnectionLifecycleStateDeleted, got.LifecycleState)
}

func TestConsoleHistory(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	inst := e.mustLaunch(t, f.pub)
	e.settle()

	h, err := e.compute.CaptureConsoleHistory(ctx, core.CaptureConsoleHistoryRequest{
		InstanceId: inst.Id, DisplayName: new("snap"),
	})
	require.NoError(t, err)
	assert.Equal(t, core.ConsoleHistoryLifecycleStateRequested, h.LifecycleState)
	assert.Equal(t, *inst.AvailabilityDomain, *h.AvailabilityDomain)

	_, err = e.compute.GetConsoleHistoryContent(
		ctx,
		core.GetConsoleHistoryContentRequest{InstanceConsoleHistoryId: h.Id},
	)
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "still being captured")

	e.settle()

	c, err := e.compute.GetConsoleHistoryContent(
		ctx,
		core.GetConsoleHistoryContentRequest{InstanceConsoleHistoryId: h.Id},
	)
	require.NoError(t, err)

	assert.Empty(t, ptrconv.String(c.Value), "no instance runs an OS, so there is no console output")
	assert.Equal(t, 0, *c.OpcBytesRemaining)

	l, err := e.compute.ListConsoleHistories(ctx, core.ListConsoleHistoriesRequest{
		CompartmentId: new(testComp), InstanceId: inst.Id, LifecycleState: core.ConsoleHistoryLifecycleStateSucceeded,
	})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)

	up, err := e.compute.UpdateConsoleHistory(ctx, core.UpdateConsoleHistoryRequest{
		InstanceConsoleHistoryId: h.Id,
		DisplayName:              new("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *up.DisplayName)

	_, err = e.compute.DeleteConsoleHistory(ctx, core.DeleteConsoleHistoryRequest{InstanceConsoleHistoryId: h.Id})
	require.NoError(t, err)

	_, err = e.compute.GetConsoleHistory(ctx, core.GetConsoleHistoryRequest{InstanceConsoleHistoryId: h.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
