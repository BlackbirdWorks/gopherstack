package rds_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestRDSErrorCodes_FaultSuffix is a regression test for a wire-code bug
// found by field-diffing gopherstack's error-code table against
// aws-sdk-go-v2/service/rds@v1.116.2's types/errors.go ErrorCode() methods
// (the ground truth for what a real RDS server puts on the wire). AWS is
// inconsistent about the "Fault" suffix across error codes (e.g.
// DBInstanceNotFound has none, but DBClusterNotFoundFault does), so each
// case here is individually confirmed against the real SDK rather than
// assumed from a uniform convention. Every RDS error uses HTTP 400 (the
// Query/EC2 protocol convention — unlike REST/JSON protocols, HTTP status
// does not vary by fault type; only the <Code> element does).
func TestRDSErrorCodes_FaultSuffix(t *testing.T) {
	t.Parallel()

	type errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *rds.Handler)
		query    string
		wantCode string
	}{
		{
			name:     "DeleteDBCluster not found",
			query:    "Action=DeleteDBCluster&Version=2014-10-31&DBClusterIdentifier=missing&SkipFinalSnapshot=true",
			wantCode: "DBClusterNotFoundFault",
		},
		{
			name: "CreateDBCluster already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateDBCluster&Version=2014-10-31"+
					"&DBClusterIdentifier=dup-cluster&Engine=aurora-postgresql"+
					"&MasterUsername=admin&MasterUserPassword=password123")
			},
			query: "Action=CreateDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=dup-cluster&Engine=aurora-postgresql" +
				"&MasterUsername=admin&MasterUserPassword=password123",
			wantCode: "DBClusterAlreadyExistsFault",
		},
		{
			name:     "DeleteDBClusterSnapshot not found",
			query:    "Action=DeleteDBClusterSnapshot&Version=2014-10-31&DBClusterSnapshotIdentifier=missing",
			wantCode: "DBClusterSnapshotNotFoundFault",
		},
		{
			name: "CreateDBClusterSnapshot already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateDBCluster&Version=2014-10-31"+
					"&DBClusterIdentifier=csnap-src&Engine=aurora-postgresql"+
					"&MasterUsername=admin&MasterUserPassword=password123")
				postRDSForm(t, h, "Action=CreateDBClusterSnapshot&Version=2014-10-31"+
					"&DBClusterSnapshotIdentifier=dup-csnap&DBClusterIdentifier=csnap-src")
			},
			query: "Action=CreateDBClusterSnapshot&Version=2014-10-31" +
				"&DBClusterSnapshotIdentifier=dup-csnap&DBClusterIdentifier=csnap-src",
			wantCode: "DBClusterSnapshotAlreadyExistsFault",
		},
		{
			name:     "DeleteDBClusterEndpoint not found",
			query:    "Action=DeleteDBClusterEndpoint&Version=2014-10-31&DBClusterEndpointIdentifier=missing",
			wantCode: "DBClusterEndpointNotFoundFault",
		},
		{
			name: "CreateDBClusterEndpoint already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateDBCluster&Version=2014-10-31"+
					"&DBClusterIdentifier=cep-src&Engine=aurora-postgresql"+
					"&MasterUsername=admin&MasterUserPassword=password123")
				postRDSForm(t, h, "Action=CreateDBClusterEndpoint&Version=2014-10-31"+
					"&DBClusterEndpointIdentifier=dup-cep&DBClusterIdentifier=cep-src")
			},
			query: "Action=CreateDBClusterEndpoint&Version=2014-10-31" +
				"&DBClusterEndpointIdentifier=dup-cep&DBClusterIdentifier=cep-src",
			wantCode: "DBClusterEndpointAlreadyExistsFault",
		},
		{
			name:     "DeleteGlobalCluster not found",
			query:    "Action=DeleteGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=missing",
			wantCode: "GlobalClusterNotFoundFault",
		},
		{
			name: "CreateGlobalCluster already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateGlobalCluster&Version=2014-10-31"+
					"&GlobalClusterIdentifier=dup-gc&Engine=aurora-postgresql")
			},
			query:    "Action=CreateGlobalCluster&Version=2014-10-31&GlobalClusterIdentifier=dup-gc&Engine=aurora-postgresql",
			wantCode: "GlobalClusterAlreadyExistsFault",
		},
		{
			name:     "DeleteBlueGreenDeployment not found",
			query:    "Action=DeleteBlueGreenDeployment&Version=2014-10-31&BlueGreenDeploymentIdentifier=missing",
			wantCode: "BlueGreenDeploymentNotFoundFault",
		},
		{
			name: "CreateBlueGreenDeployment already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=bgd-src&Engine=mysql")
				postRDSForm(t, h, "Action=CreateBlueGreenDeployment&Version=2014-10-31"+
					"&BlueGreenDeploymentName=dup-bgd"+
					"&Source=arn:aws:rds:us-east-1:000000000000:db:bgd-src")
			},
			query: "Action=CreateBlueGreenDeployment&Version=2014-10-31" +
				"&BlueGreenDeploymentName=dup-bgd" +
				"&Source=arn:aws:rds:us-east-1:000000000000:db:bgd-src",
			wantCode: "BlueGreenDeploymentAlreadyExistsFault",
		},
		{
			name:     "DeleteIntegration not found",
			query:    "Action=DeleteIntegration&Version=2014-10-31&IntegrationIdentifier=missing",
			wantCode: "IntegrationNotFoundFault",
		},
		{
			name: "CreateIntegration already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateIntegration&Version=2014-10-31"+
					"&IntegrationName=dup-integration"+
					"&SourceArn=arn:aws:rds:us-east-1:000000000000:cluster:src"+
					"&TargetArn=arn:aws:redshift-serverless:us-east-1:000000000000:namespace/tgt")
			},
			query: "Action=CreateIntegration&Version=2014-10-31" +
				"&IntegrationName=dup-integration" +
				"&SourceArn=arn:aws:rds:us-east-1:000000000000:cluster:src" +
				"&TargetArn=arn:aws:redshift-serverless:us-east-1:000000000000:namespace/tgt",
			wantCode: "IntegrationAlreadyExistsFault",
		},
		{
			name:     "DeleteOptionGroup not found",
			query:    "Action=DeleteOptionGroup&Version=2014-10-31&OptionGroupName=missing",
			wantCode: "OptionGroupNotFoundFault",
		},
		{
			name: "CreateOptionGroup already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateOptionGroup&Version=2014-10-31"+
					"&OptionGroupName=dup-og&EngineName=mysql&MajorEngineVersion=8.0"+
					"&OptionGroupDescription=d")
			},
			query: "Action=CreateOptionGroup&Version=2014-10-31" +
				"&OptionGroupName=dup-og&EngineName=mysql&MajorEngineVersion=8.0" +
				"&OptionGroupDescription=d",
			wantCode: "OptionGroupAlreadyExistsFault",
		},
		{
			// Regression test: before this fix, DBProxy-already-exists had
			// no entry in the error-code mapping table at all, so it fell
			// through to an unmapped "" code and a 500 InternalFailure
			// instead of a client-facing 400 DBProxyAlreadyExistsFault.
			name: "CreateDBProxy already exists",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h, "Action=CreateDBProxy&Version=2014-10-31"+
					"&DBProxyName=dup-proxy&EngineFamily=MYSQL"+
					"&RoleArn=arn:aws:iam::000000000000:role/proxy-role"+
					"&Auth.member.1.AuthScheme=SECRETS")
			},
			query: "Action=CreateDBProxy&Version=2014-10-31" +
				"&DBProxyName=dup-proxy&EngineFamily=MYSQL" +
				"&RoleArn=arn:aws:iam::000000000000:role/proxy-role" +
				"&Auth.member.1.AuthScheme=SECRETS",
			wantCode: "DBProxyAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRDSForm(t, h, tt.query)
			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())

			var resp errResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantCode, resp.Error.Code)
		})
	}
}
