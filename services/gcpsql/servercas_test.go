package gcpsql_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestServerCaRotation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	inst := e.createInstance(t, "ca1", "POSTGRES_16", nil)
	first := inst.ServerCaCert.Sha1Fingerprint

	_, err := e.svc.Instances.RotateServerCa(project, "ca1", &sqladmin.InstancesRotateServerCaRequest{}).
		Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)

	mustWaitOp(t, e, mustOp(e.svc.Instances.AddServerCa(project, "ca1").Context(ctx).Do()))

	list, err := e.svc.Instances.ListServerCas(project, "ca1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Certs, 2)
	assert.Equal(t, first, list.ActiveVersion)

	next := list.Certs[1].Sha1Fingerprint
	require.NotEqual(t, first, next)

	_, err = e.svc.Instances.RotateServerCa(project, "ca1", &sqladmin.InstancesRotateServerCaRequest{
		RotateServerCaContext: &sqladmin.RotateServerCaContext{NextVersion: "00"},
	}).Context(ctx).Do()
	code, _ = apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)

	mustWaitOp(t, e, mustOp(e.svc.Instances.RotateServerCa(project, "ca1", &sqladmin.InstancesRotateServerCaRequest{
		RotateServerCaContext: &sqladmin.RotateServerCaContext{NextVersion: next},
	}).Context(ctx).Do()))

	list, err = e.svc.Instances.ListServerCas(project, "ca1").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, next, list.ActiveVersion)
	require.Len(t, list.Certs, 2)
	assert.Equal(t, first, list.Certs[1].Sha1Fingerprint)

	res, err := e.svc.SslCerts.Insert(project, "ca1", &sqladmin.SslCertsInsertRequest{CommonName: "after"}).
		Context(ctx).Do()
	require.NoError(t, err)
	require.NoError(t, parseCert(t, res.ClientCert.CertInfo.Cert).CheckSignatureFrom(parseCert(t, list.Certs[0].Cert)))

	mustWaitOp(t, e, mustOp(e.svc.Instances.ResetSslConfig(project, "ca1").Context(ctx).Do()))

	certs, err := e.svc.SslCerts.List(project, "ca1").Context(ctx).Do()
	require.NoError(t, err)
	assert.Empty(t, certs.Items)

	list, err = e.svc.Instances.ListServerCas(project, "ca1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Certs, 1)
	assert.NotEqual(t, next, list.ActiveVersion)
}

type opResult struct {
	op  *sqladmin.Operation
	err error
}

func mustOp(op *sqladmin.Operation, err error) opResult { return opResult{op: op, err: err} }

func mustWaitOp(t *testing.T, e *env, r opResult) {
	t.Helper()

	e.mustWait(t, r.op, r.err)
}

func TestFailover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		avail    string
		version  int64
		wantCode int
	}{
		{"regional", "REGIONAL", 0, 0},
		{"zonal", "ZONAL", 0, http.StatusBadRequest},
		{"stale version", "REGIONAL", 99, http.StatusPreconditionFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ctx := t.Context()
			inst := e.createInstance(t, "fo", "POSTGRES_16", &sqladmin.Settings{AvailabilityType: tt.avail})

			req := &sqladmin.InstancesFailoverRequest{FailoverContext: &sqladmin.FailoverContext{
				SettingsVersion: tt.version,
			}}
			if tt.version == 0 {
				req.FailoverContext.SettingsVersion = inst.Settings.SettingsVersion
			}

			op, err := e.svc.Instances.Failover(project, "fo", req).Context(ctx).Do()
			if tt.wantCode != 0 {
				code, _ := apiCode(t, err)
				assert.Equal(t, tt.wantCode, code)

				return
			}

			op = e.mustWait(t, op, err)
			assert.Equal(t, "FAILOVER", op.OperationType)

			after, gerr := e.svc.Instances.Get(project, "fo").Context(ctx).Do()
			require.NoError(t, gerr)
			assert.Equal(t, inst.GceZone, after.SecondaryGceZone)
			assert.Equal(t, inst.SecondaryGceZone, after.GceZone)
			assert.NotEqual(t, after.GceZone, after.SecondaryGceZone)
			assert.NotEmpty(t, after.GceZone)
		})
	}
}

func TestTruncateLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		version  string
		logType  string
		wantCode int
	}{
		{"general", "MYSQL_8_0", "MYSQL_GENERAL_TABLE", 0},
		{"slow", "MYSQL_8_4", "MYSQL_SLOW_TABLE", 0},
		{"bad type", "MYSQL_8_0", "NOPE", http.StatusBadRequest},
		{"postgres", "POSTGRES_16", "MYSQL_SLOW_TABLE", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "tl", tt.version, nil)

			op, err := e.svc.Instances.TruncateLog(project, "tl", &sqladmin.InstancesTruncateLogRequest{
				TruncateLogContext: &sqladmin.TruncateLogContext{LogType: tt.logType},
			}).Context(t.Context()).Do()
			if tt.wantCode != 0 {
				code, _ := apiCode(t, err)
				assert.Equal(t, tt.wantCode, code)

				return
			}

			op = e.mustWait(t, op, err)
			assert.Equal(t, "TRUNCATE_LOG", op.OperationType)
		})
	}
}
