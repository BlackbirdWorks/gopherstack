package gcpartifactregistry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/cloud/audit"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func TestCleanupWritesAuditEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		dryRun bool
	}{
		{name: "delete", dryRun: false},
		{name: "dry run", dryRun: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(epochTime())
			e := newEnv(t, "")
			e.handler.Backend.SetClock(clock.now)

			sink := &auditSink{}
			e.handler.Backend.Audit.SetSink(sink)

			e.createRepo(t, "cleanup", policyRepo(tt.dryRun, del("d", older(15*day))))

			svc := restService(t, e)
			uploadAt(t, svc, clock, epochTime(), "app", "1")
			uploadAt(t, svc, clock, epochTime().Add(25*day), "app", "2")
			clock.set(epochTime().Add(30 * day))

			before := len(sink.logged())
			e.handler.Backend.RunCleanup(t.Context())

			logged := sink.logged()[before:]
			require.Len(t, logged, 1)

			var al audit.AuditLog
			require.NoError(t, logged[0].GetProtoPayload().UnmarshalTo(&al))

			assert.Equal(t, gcpaudit.LogName(project, gcpaudit.DataWrite), logged[0].GetLogName())
			assert.Equal(t, "artifactregistry.googleapis.com", al.GetServiceName())
			assert.Equal(
				t,
				"google.devtools.artifactregistry.v1.ArtifactRegistry.BatchDeleteVersions",
				al.GetMethodName(),
			)
			assert.Equal(t, repoName("cleanup")+"/packages/-", al.GetResourceName())
			assert.Contains(
				t,
				al.GetAuthenticationInfo().GetPrincipalEmail(),
				"@gcp-sa-artifactregistry.iam.gserviceaccount.com",
			)
			assert.Equal(t, "artifactregistry.versions.delete", al.GetAuthorizationInfo()[0].GetPermission())

			req := al.GetRequest().AsMap()
			assert.Equal(t, tt.dryRun, req["validateOnly"])
			assert.Equal(t, repoName("cleanup")+"/packages/-", req["parent"])
			assert.Equal(t, []any{pkgName("cleanup", "app") + "/versions/1"}, req["names"])
		})
	}
}
