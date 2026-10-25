package ocistreaming_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	created, err := e.admin.CreateStream(ctx, streaming.CreateStreamRequest{CreateStreamDetails: streaming.CreateStreamDetails{
		Name: new("events"), Partitions: new(3), CompartmentId: new(testCompartment),
		FreeformTags: map[string]string{"a": "b"},
	}})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamLifecycleStateCreating, created.LifecycleState)
	assert.Equal(t, 24, *created.RetentionInHours)
	assert.Equal(t, 3, *created.Partitions)
	assert.Equal(t, e.base, *created.MessagesEndpoint)
	require.NotNil(t, created.OpcWorkRequestId)
	require.NotNil(t, created.Etag)

	wr, err := e.admin.GetWorkRequest(ctx, streaming.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, streaming.OperationStatusInProgress, wr.Status)
	assert.Equal(t, streaming.OperationTypeCreateStream, wr.OperationType)

	got, err := e.admin.GetStream(ctx, streaming.GetStreamRequest{StreamId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamLifecycleStateCreating, got.LifecycleState)

	e.settle()

	got, err = e.admin.GetStream(ctx, streaming.GetStreamRequest{StreamId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamLifecycleStateActive, got.LifecycleState)

	wr, err = e.admin.GetWorkRequest(ctx, streaming.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, streaming.OperationStatusSucceeded, wr.Status)

	del, err := e.admin.DeleteStream(ctx, streaming.DeleteStreamRequest{StreamId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, del.RawResponse.StatusCode)
	assert.NotNil(t, del.OpcWorkRequestId)

	got, err = e.admin.GetStream(ctx, streaming.GetStreamRequest{StreamId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamLifecycleStateDeleting, got.LifecycleState)

	e.settle()

	got, err = e.admin.GetStream(ctx, streaming.GetStreamRequest{StreamId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamLifecycleStateDeleted, got.LifecycleState)

	_, err = e.admin.DeleteStream(ctx, streaming.DeleteStreamRequest{StreamId: created.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCreateStreamValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod      func(*streaming.CreateStreamDetails)
		name     string
		wantCode string
		wantHTTP int
	}{
		{func(d *streaming.CreateStreamDetails) { d.Partitions = new(0) }, "zero partitions", "InvalidParameter", 400},
		{func(d *streaming.CreateStreamDetails) { d.Partitions = new(1001) }, "too many partitions", "InvalidParameter", 400},
		{func(d *streaming.CreateStreamDetails) { d.RetentionInHours = new(23) }, "retention low", "InvalidParameter", 400},
		{func(d *streaming.CreateStreamDetails) { d.RetentionInHours = new(169) }, "retention high", "InvalidParameter", 400},
		{func(d *streaming.CreateStreamDetails) { d.CompartmentId = nil }, "no compartment", "MissingParameter", 400},
		{func(d *streaming.CreateStreamDetails) { d.StreamPoolId = new("ocid1.streampool.oc1..nope") }, "unknown pool", "NotAuthorizedOrNotFound", 404},
		{func(d *streaming.CreateStreamDetails) { d.Name = new(" ") }, "blank name", "MissingParameter", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := streaming.CreateStreamDetails{Name: new("s"), Partitions: new(1), CompartmentId: new(testCompartment)}
			tt.mod(&d)

			_, err := e.admin.CreateStream(t.Context(), streaming.CreateStreamRequest{CreateStreamDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, tt.wantHTTP, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestStreamDefaultPoolAndNames(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	a := e.mkStream(t, "dup", 1)

	pool, err := e.admin.GetStreamPool(ctx, streaming.GetStreamPoolRequest{StreamPoolId: a.StreamPoolId})
	require.NoError(t, err)
	assert.Equal(t, "DefaultPool", *pool.Name)
	assert.Equal(t, streaming.StreamPoolLifecycleStateActive, pool.LifecycleState)

	_, err = e.admin.CreateStream(ctx, streaming.CreateStreamRequest{CreateStreamDetails: streaming.CreateStreamDetails{
		Name: new("dup"), Partitions: new(1), CompartmentId: new(testCompartment),
	}})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	other := e.mkPool(t, "other", nil)
	b, err := e.admin.CreateStream(ctx, streaming.CreateStreamRequest{CreateStreamDetails: streaming.CreateStreamDetails{
		Name: new("dup"), Partitions: new(2), StreamPoolId: other.Id,
	}})
	require.NoError(t, err)
	assert.Equal(t, testCompartment, *b.CompartmentId)

	_, err = e.admin.DeleteStreamPool(ctx, streaming.DeleteStreamPoolRequest{StreamPoolId: other.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestListStreams(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	for _, n := range []string{"c", "a", "b"} {
		e.mkStream(t, n, 1)
	}

	tests := []struct {
		mod  func(*streaming.ListStreamsRequest)
		name string
		want []string
	}{
		{func(r *streaming.ListStreamsRequest) {
			r.SortBy = streaming.ListStreamsSortByName
			r.SortOrder = streaming.ListStreamsSortOrderAsc
		}, "by name", []string{"a", "b", "c"}},
		{func(r *streaming.ListStreamsRequest) { r.Name = new("b") }, "name filter", []string{"b"}},
		{func(r *streaming.ListStreamsRequest) { r.LifecycleState = streaming.StreamLifecycleStateDeleted }, "state filter", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := streaming.ListStreamsRequest{CompartmentId: new(testCompartment)}
			tt.mod(&r)

			resp, err := e.admin.ListStreams(ctx, r)
			require.NoError(t, err)

			var got []string
			for _, s := range resp.Items {
				got = append(got, *s.Name)
			}

			assert.Equal(t, tt.want, got)
		})
	}

	page1, err := e.admin.ListStreams(ctx, streaming.ListStreamsRequest{
		CompartmentId: new(testCompartment), Limit: new(2), SortBy: streaming.ListStreamsSortByName,
		SortOrder: streaming.ListStreamsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, page1.Items, 2)
	require.NotNil(t, page1.OpcNextPage)

	page2, err := e.admin.ListStreams(ctx, streaming.ListStreamsRequest{
		CompartmentId: new(testCompartment), Limit: new(2), Page: page1.OpcNextPage,
		SortBy: streaming.ListStreamsSortByName, SortOrder: streaming.ListStreamsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, page2.Items, 1)
	assert.Equal(t, "c", *page2.Items[0].Name)

	_, err = e.admin.ListStreams(ctx, streaming.ListStreamsRequest{})
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())
}

func TestUpdateAndChangeCompartmentStream(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	s := e.mkStream(t, "mv", 1)
	pool := e.mkPool(t, "target", nil)

	_, err := e.admin.UpdateStream(ctx, streaming.UpdateStreamRequest{
		StreamId: s.Id, IfMatch: new("stale"), UpdateStreamDetails: streaming.UpdateStreamDetails{StreamPoolId: pool.Id},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	up, err := e.admin.UpdateStream(ctx, streaming.UpdateStreamRequest{
		StreamId: s.Id, UpdateStreamDetails: streaming.UpdateStreamDetails{
			StreamPoolId: pool.Id, FreeformTags: map[string]string{"k": "v"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, *pool.Id, *up.StreamPoolId)
	assert.Equal(t, "v", up.FreeformTags["k"])

	_, err = e.admin.ChangeStreamCompartment(ctx, streaming.ChangeStreamCompartmentRequest{
		StreamId:                       s.Id,
		ChangeStreamCompartmentDetails: streaming.ChangeStreamCompartmentDetails{CompartmentId: new("ocid1.compartment.oc1..two")},
	})
	require.NoError(t, err)

	got, err := e.admin.GetStream(ctx, streaming.GetStreamRequest{StreamId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..two", *got.CompartmentId)

	wrs, err := e.admin.ListWorkRequests(ctx, streaming.ListWorkRequestsRequest{
		CompartmentId: new("ocid1.compartment.oc1..two"), ResourceId: s.Id,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, wrs.Items)

	logs, err := e.admin.ListWorkRequestLogs(ctx, streaming.ListWorkRequestLogsRequest{WorkRequestId: wrs.Items[0].Id})
	require.NoError(t, err)
	assert.NotEmpty(t, logs.Items)

	errs, err := e.admin.ListWorkRequestErrors(ctx, streaming.ListWorkRequestErrorsRequest{WorkRequestId: wrs.Items[0].Id})
	require.NoError(t, err)
	assert.Empty(t, errs.Items)
}

func TestStreamPoolCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	pool := e.mkPool(t, "p1", func(d *streaming.CreateStreamPoolDetails) {
		d.KafkaSettings = &streaming.KafkaSettings{
			AutoCreateTopicsEnable: new(true), LogRetentionHours: new(48), NumPartitions: new(4),
		}
		d.PrivateEndpointDetails = &streaming.PrivateEndpointDetails{
			SubnetId: new("ocid1.subnet.oc1..s"), NsgIds: []string{"ocid1.nsg.oc1..n"},
		}
		d.FreeformTags = map[string]string{"t": "1"}
	})
	assert.True(t, *pool.IsPrivate)
	assert.Equal(t, 48, *pool.KafkaSettings.LogRetentionHours)
	assert.Equal(t, "ocid1.subnet.oc1..s", *pool.PrivateEndpointSettings.SubnetId)
	assert.Equal(t, streaming.CustomEncryptionKeyKeyStateNone, pool.CustomEncryptionKey.KeyState)

	got, err := e.admin.GetStreamPool(ctx, streaming.GetStreamPoolRequest{StreamPoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamPoolLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, 4, *got.KafkaSettings.NumPartitions)

	up, err := e.admin.UpdateStreamPool(ctx, streaming.UpdateStreamPoolRequest{
		StreamPoolId: pool.Id, UpdateStreamPoolDetails: streaming.UpdateStreamPoolDetails{
			Name: new("renamed"), KafkaSettings: &streaming.KafkaSettings{NumPartitions: new(8)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *up.Name)
	assert.Equal(t, 8, *up.KafkaSettings.NumPartitions)
	assert.Equal(t, 48, *up.KafkaSettings.LogRetentionHours)

	_, err = e.admin.UpdateStreamPool(ctx, streaming.UpdateStreamPoolRequest{
		StreamPoolId: pool.Id, UpdateStreamPoolDetails: streaming.UpdateStreamPoolDetails{
			KafkaSettings: &streaming.KafkaSettings{LogRetentionHours: new(500)},
		},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	list, err := e.admin.ListStreamPools(ctx, streaming.ListStreamPoolsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.admin.ChangeStreamPoolCompartment(ctx, streaming.ChangeStreamPoolCompartmentRequest{
		StreamPoolId: pool.Id,
		ChangeStreamPoolCompartmentDetails: streaming.ChangeStreamPoolCompartmentDetails{
			CompartmentId: new("ocid1.compartment.oc1..two"),
		},
	})
	require.NoError(t, err)

	_, err = e.admin.DeleteStreamPool(ctx, streaming.DeleteStreamPoolRequest{StreamPoolId: pool.Id})
	require.NoError(t, err)

	e.settle()

	got, err = e.admin.GetStreamPool(ctx, streaming.GetStreamPoolRequest{StreamPoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamPoolLifecycleStateDeleted, got.LifecycleState)
}

func TestPoolCustomEncryptionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       string
		state     string
		known     bool
		wantError bool
	}{
		{"enabled key", "ocid1.key.oc1..good", "ENABLED", true, false},
		{"unknown key", "ocid1.key.oc1..ghost", "", false, true},
		{"disabled key", "ocid1.key.oc1..off", "DISABLED", true, true},
		{"not a key ocid", "ocid1.vault.oc1..v", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetKeyChecker(func(id string) (string, bool) {
				if id == tt.key && tt.known {
					return tt.state, true
				}

				return "", false
			})

			resp, err := e.admin.CreateStreamPool(t.Context(), streaming.CreateStreamPoolRequest{
				CreateStreamPoolDetails: streaming.CreateStreamPoolDetails{
					CompartmentId: new(testCompartment), Name: new("enc"),
					CustomEncryptionKeyDetails: &streaming.CustomEncryptionKeyDetails{KmsKeyId: new(tt.key)},
				},
			})
			if tt.wantError {
				assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.key, *resp.CustomEncryptionKey.KmsKeyId)
			assert.Equal(t, streaming.CustomEncryptionKeyKeyStateActive, resp.CustomEncryptionKey.KeyState)
		})
	}
}

func TestConnectHarness(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	created, err := e.admin.CreateConnectHarness(ctx, streaming.CreateConnectHarnessRequest{
		CreateConnectHarnessDetails: streaming.CreateConnectHarnessDetails{
			Name: new("h"), CompartmentId: new(testCompartment),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, streaming.ConnectHarnessLifecycleStateCreating, created.LifecycleState)
	e.settle()

	up, err := e.admin.UpdateConnectHarness(ctx, streaming.UpdateConnectHarnessRequest{
		ConnectHarnessId:            created.Id,
		UpdateConnectHarnessDetails: streaming.UpdateConnectHarnessDetails{FreeformTags: map[string]string{"x": "y"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "y", up.FreeformTags["x"])

	list, err := e.admin.ListConnectHarnesses(ctx, streaming.ListConnectHarnessesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.admin.ChangeConnectHarnessCompartment(ctx, streaming.ChangeConnectHarnessCompartmentRequest{
		ConnectHarnessId: created.Id,
		ChangeConnectHarnessCompartmentDetails: streaming.ChangeConnectHarnessCompartmentDetails{
			CompartmentId: new("ocid1.compartment.oc1..two"),
		},
	})
	require.NoError(t, err)

	_, err = e.admin.DeleteConnectHarness(ctx, streaming.DeleteConnectHarnessRequest{ConnectHarnessId: created.Id})
	require.NoError(t, err)
	e.settle()

	got, err := e.admin.GetConnectHarness(ctx, streaming.GetConnectHarnessRequest{ConnectHarnessId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, streaming.ConnectHarnessLifecycleStateDeleted, got.LifecycleState)
}
