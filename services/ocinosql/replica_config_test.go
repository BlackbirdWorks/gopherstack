package ocinosql_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testKey   = "ocid1.key.oc1.iad.keyid"
	testVault = "ocid1.vault.oc1.iad.vaultid"
)

func (e *env) alter(t *testing.T, table, ddl string) error {
	t.Helper()

	_, err := e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
		TableNameOrId: new(table),
		CompartmentId: new(testCompartment), DdlStatement: new(ddl),
	})

	return err
}

func (e *env) mkFrozen(t *testing.T) string {
	t.Helper()

	name := "mr"
	id := e.mkTable(t, name, usersDDL(name))
	require.NoError(t, e.alter(t, name, "ALTER TABLE "+name+" FREEZE SCHEMA"))
	e.settle()

	return id
}

func (e *env) addReplica(t *testing.T, table, region string) (nosql.CreateReplicaResponse, error) {
	t.Helper()

	return e.client.CreateReplica(t.Context(), nosql.CreateReplicaRequest{
		TableNameOrId: new(table),
		Region:        new(region), CompartmentId: new(testCompartment),
	})
}

func TestReplicaLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := e.mkFrozen(t)
	require.Equal(t, nosql.TableSchemaStateFrozen, e.getTable(t, "mr").SchemaState)

	resp, err := e.addReplica(t, "mr", "us-phoenix-1")
	require.NoError(t, err)

	wr := e.workRequest(t, resp.OpcWorkRequestId)
	assert.Equal(t, nosql.WorkRequestOperationTypeUpdateTable, wr.OperationType)
	assert.Equal(t, nosql.WorkRequestStatusInProgress, wr.Status)

	got := e.getTable(t, "mr")
	require.Len(t, got.Replicas, 1)
	assert.True(t, *got.IsMultiRegion)
	assert.Equal(t, 100, *got.LocalReplicaInitializationInPercent)
	assert.Equal(t, nosql.ReplicaLifecycleStateCreating, got.Replicas[0].LifecycleState)
	assert.Equal(t, "us-phoenix-1", *got.Replicas[0].Region)
	assert.Equal(t, nosql.ReplicaCapacityModeProvisioned, got.Replicas[0].CapacityMode)
	assert.Equal(t, 50, *got.Replicas[0].MaxWriteUnits)

	e.settle()

	got = e.getTable(t, "mr")
	assert.Equal(t, nosql.ReplicaLifecycleStateActive, got.Replicas[0].LifecycleState)
	assert.Equal(t, nosql.WorkRequestStatusSucceeded, e.workRequest(t, resp.OpcWorkRequestId).Status)

	snap := e.backend.Snapshot(t.Context())
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(t.Context(), snap))
	assert.Len(t, fresh.getTable(t, id).Replicas, 1)

	del, err := e.client.DeleteReplica(t.Context(), nosql.DeleteReplicaRequest{
		TableNameOrId: new("mr"), Region: new("us-phoenix-1"), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *del.OpcWorkRequestId)
	assert.Equal(t, nosql.ReplicaLifecycleStateDeleting, e.getTable(t, "mr").Replicas[0].LifecycleState)

	e.settle()
	assert.Empty(t, e.getTable(t, "mr").Replicas)
	assert.False(t, *e.getTable(t, "mr").IsMultiRegion)
}

func TestReplicaErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run    func(t *testing.T, e *env) error
		name   string
		code   string
		status int
	}{
		{
			name: "mutable", status: http.StatusBadRequest, code: "InvalidParameter",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkTable(t, "plain", usersDDL("plain"))
				_, err := e.addReplica(t, "plain", "us-phoenix-1")

				return err
			},
		},
		{
			name: "badregion", status: http.StatusBadRequest, code: "InvalidParameter",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.addReplica(t, "mr", "ca-montral")

				return err
			},
		},
		{
			name: "localregion", status: http.StatusBadRequest, code: "InvalidParameter",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.addReplica(t, "mr", "us-ashburn-1")

				return err
			},
		},
		{
			name: "duplicate", status: http.StatusConflict, code: "NotAuthorizedOrResourceAlreadyExists",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.addReplica(t, "mr", "us-phoenix-1")
				require.NoError(t, err)
				_, err = e.addReplica(t, "mr", "us-phoenix-1")

				return err
			},
		},
		{
			name: "missingtable", status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				_, err := e.addReplica(t, "nope", "us-phoenix-1")

				return err
			},
		},
		{
			name: "missingreplica", status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.client.DeleteReplica(t.Context(), nosql.DeleteReplicaRequest{
					TableNameOrId: new("mr"), Region: new("us-phoenix-1"), CompartmentId: new(testCompartment),
				})

				return err
			},
		},
		{
			name: "unitsrange", status: http.StatusBadRequest, code: "InvalidParameter",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.client.CreateReplica(t.Context(), nosql.CreateReplicaRequest{
					TableNameOrId: new("mr"),
					Region:        new("us-phoenix-1"), CompartmentId: new(testCompartment), MaxWriteUnits: new(0),
				})

				return err
			},
		},
		{
			name: "deletewithreplicas", status: http.StatusConflict, code: "IncorrectState",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.addReplica(t, "mr", "us-phoenix-1")
				require.NoError(t, err)
				e.settle()
				_, err = e.client.DeleteTable(t.Context(), nosql.DeleteTableRequest{
					TableNameOrId: new("mr"), CompartmentId: new(testCompartment),
				})

				return err
			},
		},
		{
			name: "unfreezewithreplicas", status: http.StatusConflict, code: "IncorrectState",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.addReplica(t, "mr", "us-phoenix-1")
				require.NoError(t, err)
				e.settle()

				return e.alter(t, "mr", "ALTER TABLE mr UNFREEZE SCHEMA")
			},
		},
		{
			name: "addcolumnfrozen", status: http.StatusBadRequest, code: "InvalidParameter",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)

				return e.alter(t, "mr", "ALTER TABLE mr (ADD extra STRING)")
			},
		},
		{
			name: "dropcolumnfrozen", status: http.StatusBadRequest, code: "InvalidParameter",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)

				return e.alter(t, "mr", "ALTER TABLE mr (DROP age)")
			},
		},
		{
			name: "etag", status: http.StatusPreconditionFailed, code: "NoEtagMatch",
			run: func(t *testing.T, e *env) error {
				t.Helper()
				e.mkFrozen(t)
				_, err := e.client.CreateReplica(t.Context(), nosql.CreateReplicaRequest{
					TableNameOrId: new("mr"), IfMatch: new("stale"),
					Region: new("us-phoenix-1"), CompartmentId: new(testCompartment),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			se := svcErr(t, tt.run(t, e))
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestFrozenSchemaDDL(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createTable(t, "born", usersDDL("born")+" WITH SCHEMA FROZEN", nil)
	e.settle()

	got := e.getTable(t, "born")
	assert.Equal(t, nosql.TableSchemaStateFrozen, got.SchemaState)
	assert.Contains(t, *got.DdlStatement, "WITH SCHEMA FROZEN")

	require.NoError(t, e.alter(t, "born", "ALTER TABLE born UNFREEZE SCHEMA"))
	e.settle()
	assert.Equal(t, nosql.TableSchemaStateMutable, e.getTable(t, "born").SchemaState)

	require.NoError(t, e.alter(t, "born", "ALTER TABLE born (ADD extra STRING)"))
}

func TestReplicaRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkFrozen(t)

	send := func() (string, error) {
		r, err := e.client.CreateReplica(t.Context(), nosql.CreateReplicaRequest{
			TableNameOrId: new("mr"), OpcRetryToken: new("tok"),
			Region: new("us-phoenix-1"), CompartmentId: new(testCompartment),
		})
		if err != nil {
			return "", err
		}

		return *r.OpcWorkRequestId, nil
	}

	first, err := send()
	require.NoError(t, err)

	second, err := send()
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Len(t, e.getTable(t, "mr").Replicas, 1)
}

func (e *env) getConfig(t *testing.T) (nosql.HostedConfiguration, string) {
	t.Helper()

	resp, err := e.client.GetConfiguration(t.Context(), nosql.GetConfigurationRequest{
		CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)

	cfg, ok := resp.Configuration.(nosql.HostedConfiguration)
	require.True(t, ok, "%T", resp.Configuration)

	return cfg, *resp.Etag
}

func TestConfigurationKms(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	cfg, etag := e.getConfig(t)
	assert.Nil(t, cfg.KmsKey.Id)
	assert.Equal(t, nosql.KmsKeyKmsKeyStateActive, cfg.KmsKey.KmsKeyState)

	upd, err := e.client.UpdateConfiguration(t.Context(), nosql.UpdateConfigurationRequest{
		CompartmentId: new(testCompartment), IfMatch: new(etag),
		UpdateConfigurationDetails: nosql.UpdateHostedConfigurationDetails{
			KmsKey: &nosql.KmsKey{Id: new(testKey), KmsVaultId: new(testVault)},
		},
	})
	require.NoError(t, err)

	wr := e.workRequest(t, upd.OpcWorkRequestId)
	assert.Equal(t, nosql.WorkRequestOperationTypeUpdateConfiguration, wr.OperationType)
	assert.Equal(t, nosql.WorkRequestStatusInProgress, wr.Status)

	cfg, newEtag := e.getConfig(t)
	assert.Equal(t, testKey, *cfg.KmsKey.Id)
	assert.Equal(t, nosql.KmsKeyKmsKeyStateUpdating, cfg.KmsKey.KmsKeyState)
	assert.NotEqual(t, etag, newEtag)

	e.settle()

	cfg, _ = e.getConfig(t)
	assert.Equal(t, nosql.KmsKeyKmsKeyStateActive, cfg.KmsKey.KmsKeyState)
	assert.Equal(t, testVault, *cfg.KmsKey.KmsVaultId)
	assert.NotNil(t, cfg.KmsKey.TimeCreated)

	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	restored, _ := fresh.getConfig(t)
	assert.Equal(t, testKey, *restored.KmsKey.Id)

	un, err := e.client.UnassignKmsKey(t.Context(), nosql.UnassignKmsKeyRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.NotEmpty(t, *un.OpcWorkRequestId)

	cfg, _ = e.getConfig(t)
	assert.Nil(t, cfg.KmsKey.Id)
	assert.Equal(t, nosql.KmsKeyKmsKeyStateUpdating, cfg.KmsKey.KmsKeyState)
	e.settle()

	cfg, _ = e.getConfig(t)
	assert.Equal(t, nosql.KmsKeyKmsKeyStateActive, cfg.KmsKey.KmsKeyState)
}

func TestConfigurationErrors(t *testing.T) {
	t.Parallel()

	good := nosql.UpdateHostedConfigurationDetails{KmsKey: &nosql.KmsKey{Id: new(testKey), KmsVaultId: new(testVault)}}

	tests := []struct {
		details nosql.UpdateConfigurationDetails
		name    string
		etag    string
		multi   bool
		status  int
	}{
		{name: "badkey", status: http.StatusBadRequest, details: nosql.UpdateHostedConfigurationDetails{
			KmsKey: &nosql.KmsKey{Id: new("x"), KmsVaultId: new(testVault)},
		}},
		{name: "novault", status: http.StatusBadRequest, details: nosql.UpdateHostedConfigurationDetails{
			KmsKey: &nosql.KmsKey{Id: new(testKey)},
		}},
		{name: "nokms", status: http.StatusBadRequest, details: nosql.UpdateHostedConfigurationDetails{}},
		{name: "wrongenv", status: http.StatusBadRequest, details: nosql.UpdateMultiTenancyConfigurationDetails{}},
		{
			name:    "multitenancy",
			status:  http.StatusBadRequest,
			multi:   true,
			details: nosql.UpdateMultiTenancyConfigurationDetails{},
		},
		{name: "etag", status: http.StatusPreconditionFailed, etag: "stale", details: good},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetMultiTenancy(tt.multi)

			req := nosql.UpdateConfigurationRequest{
				CompartmentId: new(testCompartment), UpdateConfigurationDetails: tt.details,
			}
			if tt.etag != "" {
				req.IfMatch = new(tt.etag)
			}

			_, err := e.client.UpdateConfiguration(t.Context(), req)
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestConfigurationMultiTenancyAndDryRun(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	_, err := e.client.UpdateConfiguration(t.Context(), nosql.UpdateConfigurationRequest{
		CompartmentId: new(testCompartment), IsOpcDryRun: new(true),
		UpdateConfigurationDetails: nosql.UpdateHostedConfigurationDetails{
			KmsKey: &nosql.KmsKey{Id: new(testKey), KmsVaultId: new(testVault)},
		},
	})
	require.NoError(t, err)

	cfg, _ := e.getConfig(t)
	assert.Nil(t, cfg.KmsKey.Id, "dry run changes nothing")

	e.backend.SetMultiTenancy(true)

	resp, err := e.client.GetConfiguration(
		t.Context(),
		nosql.GetConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.IsType(t, nosql.MultiTenancyConfiguration{}, resp.Configuration)

	_, err = e.client.UnassignKmsKey(t.Context(), nosql.UnassignKmsKeyRequest{CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetConfiguration(t.Context(), nosql.GetConfigurationRequest{CompartmentId: new("")})
	assert.Error(t, err)
}

func TestDeleteWorkRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp := e.createTable(t, "wr", usersDDL("wr"), nil)

	del, err := e.client.DeleteWorkRequest(
		t.Context(),
		nosql.DeleteWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, *resp.OpcWorkRequestId, *del.OpcWorkRequestId)
	assert.Equal(t, nosql.WorkRequestStatusCanceled, e.workRequest(t, resp.OpcWorkRequestId).Status)

	_, err = e.client.DeleteWorkRequest(
		t.Context(),
		nosql.DeleteWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	done := e.createTable(t, "wr2", usersDDL("wr2"), nil)
	e.settle()

	_, err = e.client.DeleteWorkRequest(
		t.Context(),
		nosql.DeleteWorkRequestRequest{WorkRequestId: done.OpcWorkRequestId},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteWorkRequest(
		t.Context(),
		nosql.DeleteWorkRequestRequest{WorkRequestId: new("ocid1.nosqlworkrequest.oc1..x")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
