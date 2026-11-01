package ocifilestorage_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	svc "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true})
	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)
	x := e.createExport(t, mt.ExportSetId, fs.Id, "/p", nil)
	sn := e.createSnapshot(t, fs.Id, "snap")
	pol := e.createPolicy(t, nil)
	dst := e.createFS(t, nil)
	rep := e.createReplication(t, fs.Id, dst.Id)
	conn := e.createConnector(t, nil)

	_, err := e.client.CreateQuotaRule(t.Context(), filestorage.CreateQuotaRuleRequest{
		FileSystemId: fs.Id, PrincipalType: "DEFAULT_USER", IsHardQuota: new(true), QuotaLimitInGigabytes: new(3),
	})
	require.NoError(t, err)

	_, err = e.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment),
		OpcRetryToken: new("tok"),
	})
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())

	e2 := newEnv(t, envOpts{})
	require.NoError(t, e2.backend.Restore(t.Context(), snap))

	assert.Equal(t, "snap", *mustGet(t, e2, func() (*string, error) {
		r, gerr := e2.client.GetSnapshot(t.Context(), filestorage.GetSnapshotRequest{SnapshotId: sn.Id})

		return r.Name, gerr
	}))
	assert.Equal(t, *mt.ExportSetId, *e2.getMT(t, mt.Id).ExportSetId)
	assert.Equal(t, "/p", *mustGet(t, e2, func() (*string, error) {
		r, gerr := e2.client.GetExport(t.Context(), filestorage.GetExportRequest{ExportId: x.Id})

		return r.Path, gerr
	}))
	assert.Equal(t, int64(15), *e2.getReplication(t, rep.Id).ReplicationInterval)

	gotPol, err := e2.client.GetFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.GetFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: pol.Id},
	)
	require.NoError(t, err)
	assert.Len(t, gotPol.Schedules, 1)

	_, err = e2.client.GetOutboundConnector(
		t.Context(),
		filestorage.GetOutboundConnectorRequest{OutboundConnectorId: conn.Id},
	)
	require.NoError(t, err)

	list, err := e2.client.ListQuotaRules(t.Context(), filestorage.ListQuotaRulesRequest{FileSystemId: fs.Id})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	again, err := e2.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment),
		OpcRetryToken: new("tok"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *again.Id, "retry tokens survive")

	e2.clock.Advance(time.Minute)
	e2.backend.Sweep()
	assert.NotEmpty(t, string(e2.getReplication(t, rep.Id).DeltaStatus), "replication cycles resume on the clock")
}

func mustGet(t *testing.T, _ *env, fn func() (*string, error)) *string {
	t.Helper()

	v, err := fn()
	require.NoError(t, err)

	return v
}

func TestRestoreRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{name: "null-file-system", data: `{"version":1,"fileSystems":{"a":null}}`, wantErr: true},
		{name: "null-wire", data: `{"version":1,"exports":{"a":{"w":null}}}`, wantErr: true},
		{name: "null-quota", data: `{"version":1,"quotaRules":{"a":null}}`, wantErr: true},
		{name: "future-version", data: `{"version":99,"fileSystems":{"a":null}}`},
		{name: "empty", data: `{"version":1}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := svc.NewInMemoryBackend()
			err := b.Restore(t.Context(), []byte(tc.data))

			if tc.wantErr {
				require.ErrorIs(t, err, svc.ErrNullEntry)

				return
			}

			require.NoError(t, err)

			_, _, getErr := b.GetFileSystem("a")
			require.Error(t, getErr)
		})
	}
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)

	e.handler.Reset()

	_, err := e.client.GetFileSystem(t.Context(), filestorage.GetFileSystemRequest{FileSystemId: fs.Id})
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")
	assert.Equal(t, []string{"ocid1.vnic.oc1.iad.v1"}, e.net.deletedVnics(), "reset releases the VNICs")
	assert.NotEmpty(t, *mt.Id)
}

func TestAdminEndpoints(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	do := func(method, path string) int {
		req, err := http.NewRequestWithContext(t.Context(), method, e.base+path, http.NoBody)
		require.NoError(t, err)

		resp, err := testclient.New(t).Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, do(http.MethodGet, "/_gopherstack/oci/keys"))
	assert.Equal(t, http.StatusOK, do(http.MethodGet, "/_gopherstack/oci/nfs"))
	assert.Equal(t, http.StatusNotFound, do(http.MethodPost, "/_gopherstack/oci/nfs"))
	assert.Equal(t, http.StatusNotFound, do(http.MethodGet, "/20171215/nothing"))
	assert.Equal(t, http.StatusNotFound, do(http.MethodDelete, "/_gopherstack/oci/keys?keyId=none"))
}

func TestOperationCount(t *testing.T) {
	t.Parallel()

	assert.Len(
		t,
		svc.NewHandler(svc.NewInMemoryBackend()).GetSupportedOperations(),
		74,
		"every FileStorageClient operation is served",
	)
}
